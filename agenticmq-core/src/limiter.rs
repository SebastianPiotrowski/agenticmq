use std::collections::HashMap;
use tokio::sync::{RwLock, Mutex};
use chrono::{DateTime, Utc, Duration};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ModelLimit {
    pub tpm: u32, // Tokens Per Minute
    pub rpm: u32, // Requests Per Minute
}

#[derive(Debug, Clone)]
struct RateEvent {
    timestamp: DateTime<Utc>,
    task_id: Uuid,
    tokens: u32,
    resolved: bool,
}

#[derive(Debug)]
pub struct TokenRateLimiter {
    limits: RwLock<HashMap<String, ModelLimit>>,
    history: Mutex<HashMap<String, Vec<RateEvent>>>,
}

const SLIDING_WINDOW_SECS: i64 = 60;
const UNRESOLVED_SAFETY_WINDOW_MINS: i64 = 10;

impl TokenRateLimiter {
    pub fn new() -> Self {
        let mut limits = HashMap::new();
        
        let gpt4o_tpm = std::env::var("AGENTICMQ_DEFAULT_GPT4O_TPM")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30000);
        let gpt4o_rpm = std::env::var("AGENTICMQ_DEFAULT_GPT4O_RPM")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(20);
        limits.insert("gpt-4o".to_string(), ModelLimit { tpm: gpt4o_tpm, rpm: gpt4o_rpm });

        let mini_tpm = std::env::var("AGENTICMQ_DEFAULT_GPT4O_MINI_TPM")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(150000);
        let mini_rpm = std::env::var("AGENTICMQ_DEFAULT_GPT4O_MINI_RPM")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        limits.insert("gpt-4o-mini".to_string(), ModelLimit { tpm: mini_tpm, rpm: mini_rpm });

        let sonnet_tpm = std::env::var("AGENTICMQ_DEFAULT_CLAUDE_SONNET_TPM")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(40000);
        let sonnet_rpm = std::env::var("AGENTICMQ_DEFAULT_CLAUDE_SONNET_RPM")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(25);
        limits.insert("claude-3-5-sonnet".to_string(), ModelLimit { tpm: sonnet_tpm, rpm: sonnet_rpm });

        Self {
            limits: RwLock::new(limits),
            history: Mutex::new(HashMap::new()),
        }
    }

    pub async fn set_limit(&self, model: String, tpm: u32, rpm: u32) {
        let mut limits = self.limits.write().await;
        limits.insert(model, ModelLimit { tpm, rpm });
    }

    pub async fn check_and_record(&self, model: &str, task_id: Uuid, token_budget: u32) -> Result<(), String> {
        let limits = self.limits.read().await;
        let limit = match limits.get(model) {
            Some(l) => l,
            None => return Ok(()),
        };

        let now = Utc::now();
        let standard_cutoff = now - Duration::seconds(SLIDING_WINDOW_SECS);
        let safety_cutoff = now - Duration::minutes(UNRESOLVED_SAFETY_WINDOW_MINS);

        let mut history_map = self.history.lock().await;
        let events = history_map.entry(model.to_string()).or_default();

        events.retain(|event| {
            if event.timestamp > standard_cutoff {
                return true;
            }
            !event.resolved && event.timestamp > safety_cutoff
        });

        let current_requests = events.len() as u32;
        let current_tokens: u32 = events.iter().map(|e| e.tokens).sum();

        // Check RPM
        if current_requests + 1 > limit.rpm {
            return Err(format!(
                "RPM limit exceeded for model '{}'. Current: {}/min, Limit: {}/min",
                model, current_requests, limit.rpm
            ));
        }

        // Check TPM
        if current_tokens + token_budget > limit.tpm {
            return Err(format!(
                "TPM limit exceeded for model '{}'. Active window tokens: {} + budget: {} exceeds limit: {}/min",
                model, current_tokens, token_budget, limit.tpm
            ));
        }

        // Within limits: Record the event
        events.push(RateEvent {
            timestamp: now,
            task_id,
            tokens: token_budget,
            resolved: false,
        });

        Ok(())
    }

    /// Reports actual token usage after task completion/checkpoint, adjusting the registered window.
    pub async fn report_actual_usage(&self, model: &str, task_id: Uuid, actual_tokens: u32, resolved: bool) {
        let mut history_map = self.history.lock().await;
        if let Some(events) = history_map.get_mut(model) {
            // Find the event by task_id and adjust its token budget & resolution status
            if let Some(event) = events.iter_mut().find(|e| e.task_id == task_id) {
                event.tokens = actual_tokens;
                event.resolved = resolved;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rate_limiting_rpm() {
        let limiter = TokenRateLimiter::new();
        // Set tight limits: 1000 tokens, 2 requests per minute
        limiter.set_limit("test-model".to_string(), 1000, 2).await;

        let t1 = Uuid::new_v4();
        let t2 = Uuid::new_v4();
        let t3 = Uuid::new_v4();

        // First request: Should pass
        assert!(limiter.check_and_record("test-model", t1, 100).await.is_ok());
        // Second request: Should pass
        assert!(limiter.check_and_record("test-model", t2, 100).await.is_ok());
        // Third request: Should fail (RPM exceeded)
        let res = limiter.check_and_record("test-model", t3, 100).await;
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("RPM limit exceeded"));
    }

    #[tokio::test]
    async fn test_rate_limiting_tpm() {
        let limiter = TokenRateLimiter::new();
        // Set limits: 500 tokens, 10 requests per minute
        limiter.set_limit("test-model".to_string(), 500, 10).await;

        let t1 = Uuid::new_v4();
        let t2 = Uuid::new_v4();

        // First request: Should pass
        assert!(limiter.check_and_record("test-model", t1, 300).await.is_ok());
        // Second request: Should fail (TPM exceeded: 300 + 300 = 600 > 500)
        let res = limiter.check_and_record("test-model", t2, 300).await;
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("TPM limit exceeded"));
    }

    #[tokio::test]
    async fn test_report_actual_usage() {
        let limiter = TokenRateLimiter::new();
        limiter.set_limit("test-model".to_string(), 500, 10).await;

        let t1 = Uuid::new_v4();
        let t2 = Uuid::new_v4();

        // Reserve 400 tokens (out of 500)
        assert!(limiter.check_and_record("test-model", t1, 400).await.is_ok());

        // Second request for 200 tokens fails because 400 + 200 > 500
        assert!(limiter.check_and_record("test-model", t2, 200).await.is_err());

        // Report that the first request only used 100 tokens, mark as resolved
        limiter.report_actual_usage("test-model", t1, 100, true).await;

        // Second request should now pass, since active tokens is 100 + 200 = 300 <= 500
        assert!(limiter.check_and_record("test-model", t2, 200).await.is_ok());
    }

    #[tokio::test]
    async fn test_retention_policy() {
        let limiter = TokenRateLimiter::new();
        limiter.set_limit("test-model".to_string(), 1000, 10).await;

        let now = Utc::now();
        let t1 = Uuid::new_v4();
        let t2 = Uuid::new_v4();
        let t3 = Uuid::new_v4();

        {
            let mut history = limiter.history.lock().await;
            let events = history.entry("test-model".to_string()).or_default();
            
            // Event 1: unresolved, 2 minutes old -> should be kept (age < 10m)
            events.push(RateEvent {
                timestamp: now - Duration::seconds(120),
                task_id: t1,
                tokens: 100,
                resolved: false,
            });

            // Event 2: resolved, 2 minutes old -> should be pruned (age > 60s)
            events.push(RateEvent {
                timestamp: now - Duration::seconds(120),
                task_id: t2,
                tokens: 100,
                resolved: true,
            });

            // Event 3: unresolved, 11 minutes old -> should be pruned (age > 10m)
            events.push(RateEvent {
                timestamp: now - Duration::seconds(660),
                task_id: t3,
                tokens: 100,
                resolved: false,
            });
        }

        // Trigger prune by checking a new record
        let t4 = Uuid::new_v4();
        assert!(limiter.check_and_record("test-model", t4, 50).await.is_ok());

        // Check history contents
        let history = limiter.history.lock().await;
        let events = history.get("test-model").unwrap();
        
        // Should only contain:
        // 1. Event 1 (t1 - unresolved, 2 min old)
        // 2. Event 4 (t4 - newly created)
        assert_eq!(events.len(), 2);
        assert!(events.iter().any(|e| e.task_id == t1));
        assert!(events.iter().any(|e| e.task_id == t4));
    }
}
