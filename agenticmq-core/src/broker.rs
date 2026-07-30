use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;
use chrono::Utc;
use tokio::sync::mpsc::{self, Sender, Receiver};
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::RwLock;

use crate::envelope::{MessageEnvelope, TaskStatus};
use crate::storage::SledStorage;
use crate::limiter::TokenRateLimiter;
use crate::error::BrokerError;

pub struct ModelQueue {
    tx: Sender<Uuid>,
    rx: TokioMutex<Receiver<Uuid>>,
    deferred: TokioMutex<Vec<Uuid>>,
}

impl ModelQueue {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(1000);
        Self {
            tx,
            rx: TokioMutex::new(rx),
            deferred: TokioMutex::new(Vec::new()),
        }
    }
}

pub struct Broker {
    storage: SledStorage,
    pub limiter: TokenRateLimiter,
    tasks: RwLock<HashMap<Uuid, MessageEnvelope>>,
    queues: RwLock<HashMap<String, Arc<ModelQueue>>>,
}

impl Broker {
    pub async fn new(state_path: &str) -> Result<Self, anyhow::Error> {
        let storage = SledStorage::new(state_path)?;
        let limiter = TokenRateLimiter::new();
        
        let mut tasks = HashMap::new();
        let mut queues = HashMap::new();

        // Recover state from disk on startup
        let persisted_tasks = storage.load_all_tasks().await?;
        tracing::info!("Recovered {} tasks from disk storage", persisted_tasks.len());

        for mut task in persisted_tasks {
            // If a task was left in Processing state, it means the broker or worker crashed.
            // Reset it back to Pending and re-queue it.
            if task.status == TaskStatus::Processing {
                task.status = TaskStatus::Pending;
                task.updated_at = Utc::now();
                let _ = storage.save_task(&task).await;
            }

            let task_id = task.task_id;
            let model = task.current_model.clone();
            let status = task.status;
            
            tasks.insert(task_id, task);

            // Re-queue pending tasks
            if status == TaskStatus::Pending {
                let model_queue = queues.entry(model).or_insert_with(|| Arc::new(ModelQueue::new()));
                let _ = model_queue.tx.send(task_id).await;
            }
        }

        Ok(Self {
            storage,
            limiter,
            tasks: RwLock::new(tasks),
            queues: RwLock::new(queues),
        })
    }

    async fn get_or_create_queue(&self, model: &str) -> Arc<ModelQueue> {
        {
            let queues = self.queues.read().await;
            if let Some(q) = queues.get(model) {
                return q.clone();
            }
        }
        let mut queues = self.queues.write().await;
        queues.entry(model.to_string()).or_insert_with(|| Arc::new(ModelQueue::new())).clone()
    }

    pub async fn submit_task(
        &self,
        prompt_data: String,
        system_context: Option<String>,
        token_budget: u32,
        max_cost_usd: f64,
        model: String,
        fallback_models: Vec<String>,
    ) -> Result<MessageEnvelope, BrokerError> {
        let task = MessageEnvelope::new(
            prompt_data,
            system_context,
            token_budget,
            max_cost_usd,
            model.clone(),
            fallback_models,
        );

        // Save to disk
        self.storage.save_task(&task).await
            .map_err(|e| BrokerError::Storage(e.to_string()))?;

        let task_id = task.task_id;

        // Update in-memory state
        {
            let mut tasks = self.tasks.write().await;
            tasks.insert(task_id, task.clone());
        }

        // Send to model queue channel
        let model_queue = self.get_or_create_queue(&model).await;
        if let Err(e) = model_queue.tx.send(task_id).await {
            return Err(BrokerError::Internal(format!("Failed to enqueue task: {}", e)));
        }

        tracing::info!("Task {} submitted for model {}", task_id, task.current_model);
        Ok(task)
    }

    pub async fn poll_task(&self, model: &str) -> Option<MessageEnvelope> {
        let queue = {
            let queues = self.queues.read().await;
            queues.get(model).cloned()
        };

        let queue = match queue {
            Some(q) => q,
            None => return None,
        };

        // First, check the deferred tasks
        let mut deferred = queue.deferred.lock().await;
        let mut selected_index = None;

        for (i, task_id) in deferred.iter().enumerate() {
            let tasks = self.tasks.read().await;
            if let Some(task) = tasks.get(task_id) {
                // Perform rate limit check passing task_id
                if self.limiter.check_and_record(model, *task_id, task.token_budget).await.is_ok() {
                    selected_index = Some(i);
                    break;
                }
            }
        }

        if let Some(idx) = selected_index {
            let task_id = deferred.remove(idx);
            return self.set_task_processing(task_id, model).await;
        }

        // Second, try to pull from the channel
        let mut rx = queue.rx.lock().await;
        while let Ok(task_id) = rx.try_recv() {
            let tasks = self.tasks.read().await;
            if let Some(task) = tasks.get(&task_id) {
                // Perform rate limit check passing task_id
                if self.limiter.check_and_record(model, task_id, task.token_budget).await.is_ok() {
                    // Drop read lock before calling async set_task_processing
                    drop(tasks);
                    return self.set_task_processing(task_id, model).await;
                } else {
                    // Defer this task for later
                    deferred.push(task_id);
                }
            }
        }

        None
    }

    async fn set_task_processing(&self, task_id: Uuid, model: &str) -> Option<MessageEnvelope> {
        let mut tasks = self.tasks.write().await;
        if let Some(task) = tasks.get_mut(&task_id) {
            if task.status != TaskStatus::Pending {
                tracing::warn!(
                    "Attempted to poll task {} but status is {:?} (expected Pending)",
                    task_id, task.status
                );
                return None;
            }
            task.status = TaskStatus::Processing;
            task.updated_at = Utc::now();
            
            // Persist changes to sled
            if let Err(e) = self.storage.save_task(task).await {
                tracing::error!("Failed to persist task {} status update: {}", task_id, e);
            }
            
            tracing::info!("Task {} polled and marked Processing for model {}", task_id, model);
            Some(task.clone())
        } else {
            None
        }
    }

    pub async fn checkpoint_task(
        &self,
        task_id: Uuid,
        tokens_used: u32,
        cost_usd: f64,
        output: Option<String>,
        error_message: Option<String>,
        pause_request: bool,
    ) -> Result<MessageEnvelope, BrokerError> {
        let mut tasks = self.tasks.write().await;
        let task = tasks.get_mut(&task_id).ok_or_else(|| BrokerError::TaskNotFound(task_id))?;

        if task.status != TaskStatus::Processing {
            return Err(BrokerError::InvalidStatus {
                task_id,
                expected: "processing".to_string(),
                actual: task.status,
            });
        }

        // Adjust rate limiter dynamic tracking based on incremental usage
        self.limiter.report_actual_usage(&task.current_model, task_id, tokens_used, pause_request).await;

        task.tokens_used = tokens_used;
        task.cost_usd = cost_usd;
        task.updated_at = Utc::now();

        if let Some(ref out) = output {
            task.output = Some(out.clone());
        }
        if let Some(ref err) = error_message {
            task.error_message = Some(err.clone());
        }

        if pause_request {
            task.status = TaskStatus::Paused;
            // Generate a secure pause/resume key
            let key = format!("x-resume-{}", Uuid::new_v4().simple());
            task.resume_key = Some(key);
            tracing::info!("Task {} paused. Resume key generated.", task_id);
        }

        self.storage.save_task(task).await
            .map_err(|e| BrokerError::Storage(e.to_string()))?;

        Ok(task.clone())
    }

    pub async fn complete_task(
        &self,
        task_id: Uuid,
        output: String,
        tokens_used: u32,
        cost_usd: f64,
    ) -> Result<MessageEnvelope, BrokerError> {
        let mut tasks = self.tasks.write().await;
        let task = tasks.get_mut(&task_id).ok_or_else(|| BrokerError::TaskNotFound(task_id))?;

        // Adjust rate limiter dynamic tracking
        self.limiter.report_actual_usage(&task.current_model, task_id, tokens_used, true).await;

        task.status = TaskStatus::Completed;
        task.output = Some(output);
        task.tokens_used = tokens_used;
        task.cost_usd = cost_usd;
        task.error_message = None;
        task.updated_at = Utc::now();

        self.storage.save_task(task).await
            .map_err(|e| BrokerError::Storage(e.to_string()))?;

        tracing::info!("Task {} successfully completed", task_id);
        Ok(task.clone())
    }

    pub async fn fail_task(
        &self,
        task_id: Uuid,
        error: String,
        tokens_used: u32,
        cost_usd: f64,
    ) -> Result<MessageEnvelope, BrokerError> {
        let (task_to_requeue, result_task) = {
            let mut tasks = self.tasks.write().await;
            let task = tasks.get_mut(&task_id).ok_or_else(|| BrokerError::TaskNotFound(task_id))?;

            // Adjust rate limiter dynamic tracking
            self.limiter.report_actual_usage(&task.current_model, task_id, tokens_used, true).await;

            task.tokens_used = tokens_used;
            task.cost_usd = cost_usd;
            task.error_message = Some(error.clone());
            task.updated_at = Utc::now();

            let task_to_requeue = if !task.fallback_models.is_empty() {
                // Perform smart fallback routing
                let next_model = task.fallback_models.remove(0);
                let prev_model = std::mem::replace(&mut task.current_model, next_model.clone());
                task.status = TaskStatus::Pending;
                task.trace_depth += 1;
                
                tracing::warn!(
                    "Task {} failed on model '{}'. Routing to fallback model '{}' (trace depth: {})",
                    task_id, prev_model, next_model, task.trace_depth
                );

                Some((task_id, next_model))
            } else {
                task.status = TaskStatus::Failed;
                tracing::error!("Task {} failed, no fallback models left. Error: {}", task_id, error);
                None
            };

            self.storage.save_task(task).await
                .map_err(|e| BrokerError::Storage(e.to_string()))?;
            
            (task_to_requeue, task.clone())
        };

        // Re-queue the task if it fell back to another model
        if let Some((tid, model)) = task_to_requeue {
            let model_queue = self.get_or_create_queue(&model).await;
            if let Err(e) = model_queue.tx.send(tid).await {
                return Err(BrokerError::Internal(format!("Failed to re-enqueue task: {}", e)));
            }
        }

        Ok(result_task)
    }

    pub async fn resume_task(&self, task_id: Uuid, resume_key: &str) -> Result<MessageEnvelope, BrokerError> {
        let model_to_requeue = {
            let mut tasks = self.tasks.write().await;
            let task = tasks.get_mut(&task_id).ok_or_else(|| BrokerError::TaskNotFound(task_id))?;

            if task.status != TaskStatus::Paused {
                return Err(BrokerError::InvalidStatus {
                    task_id,
                    expected: "paused".to_string(),
                    actual: task.status,
                });
            }

            match &task.resume_key {
                Some(key) if key == resume_key => {
                    task.status = TaskStatus::Pending;
                    task.resume_key = None;
                    task.updated_at = Utc::now();
                    let model_to_requeue = Some((task_id, task.current_model.clone()));
                    
                    self.storage.save_task(task).await
                        .map_err(|e| BrokerError::Storage(e.to_string()))?;
                    
                    tracing::info!("Task {} successfully resumed", task_id);
                    model_to_requeue
                }
                _ => return Err(BrokerError::InvalidResumeKey(task_id)),
            }
        };

        if let Some((tid, model)) = model_to_requeue {
            let model_queue = self.get_or_create_queue(&model).await;
            if let Err(e) = model_queue.tx.send(tid).await {
                return Err(BrokerError::Internal(format!("Failed to re-enqueue resumed task: {}", e)));
            }
        }

        let tasks = self.tasks.read().await;
        Ok(tasks.get(&task_id).cloned().unwrap())
    }

    pub async fn get_task(&self, task_id: Uuid) -> Option<MessageEnvelope> {
        let tasks = self.tasks.read().await;
        tasks.get(&task_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_broker_path(test_name: &str) -> String {
        let path = format!("/Users/sebastianpiotrowski/.gemini/antigravity-ide/scratch/.test_state_{}", test_name);
        let _ = fs::remove_dir_all(&path);
        path
    }

    #[tokio::test]
    async fn test_submit_and_poll() {
        let path = temp_broker_path("submit_and_poll");
        let broker = Broker::new(&path).await.unwrap();

        let task = broker.submit_task(
            "test prompt".to_string(),
            None,
            100,
            0.01,
            "gpt-4o".to_string(),
            vec![],
        ).await.unwrap();

        assert_eq!(task.current_model, "gpt-4o");
        assert_eq!(task.status, TaskStatus::Pending);

        // Poll it
        let polled = broker.poll_task("gpt-4o").await.unwrap();
        assert_eq!(polled.task_id, task.task_id);
        assert_eq!(polled.status, TaskStatus::Processing);

        // Try to poll again (should be empty)
        assert!(broker.poll_task("gpt-4o").await.is_none());

        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn test_fallback_routing() {
        let path = temp_broker_path("fallback");
        let broker = Broker::new(&path).await.unwrap();

        let task = broker.submit_task(
            "test prompt".to_string(),
            None,
            100,
            0.01,
            "gpt-4o".to_string(),
            vec!["gpt-4o-mini".to_string()],
        ).await.unwrap();

        // Poll and fail
        let polled = broker.poll_task("gpt-4o").await.unwrap();
        let failed_task = broker.fail_task(polled.task_id, "Rate limit hit".to_string(), 50, 0.001).await.unwrap();

        // Check it has been routed to fallback
        assert_eq!(failed_task.current_model, "gpt-4o-mini");
        assert_eq!(failed_task.status, TaskStatus::Pending);
        assert_eq!(failed_task.trace_depth, 1);

        // Should be pollable on gpt-4o-mini now
        let polled_fallback = broker.poll_task("gpt-4o-mini").await.unwrap();
        assert_eq!(polled_fallback.task_id, task.task_id);
        assert_eq!(polled_fallback.status, TaskStatus::Processing);

        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn test_pause_and_resume() {
        let path = temp_broker_path("pause_resume");
        let broker = Broker::new(&path).await.unwrap();

        let task = broker.submit_task(
            "dangerous prompt".to_string(),
            None,
            100,
            0.01,
            "gpt-4o".to_string(),
            vec![],
        ).await.unwrap();

        let polled = broker.poll_task("gpt-4o").await.unwrap();

        // Checkpoint with pause request
        let paused_task = broker.checkpoint_task(
            polled.task_id,
            50,
            0.001,
            Some("unsafe content".to_string()),
            None,
            true, // pause
        ).await.unwrap();

        assert_eq!(paused_task.status, TaskStatus::Paused);
        let resume_key = paused_task.resume_key.unwrap();
        assert!(resume_key.starts_with("x-resume-"));

        // Try to resume with invalid key
        assert!(broker.resume_task(task.task_id, "wrong_key").await.is_err());

        // Resume with correct key
        let resumed = broker.resume_task(task.task_id, &resume_key).await.unwrap();
        assert_eq!(resumed.status, TaskStatus::Pending);
        assert!(resumed.resume_key.is_none());

        // Should be back in queue
        let repolled = broker.poll_task("gpt-4o").await.unwrap();
        assert_eq!(repolled.task_id, task.task_id);

        let _ = fs::remove_dir_all(path);
    }
}
