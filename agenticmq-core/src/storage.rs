use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;
use crate::envelope::MessageEnvelope;

#[derive(Clone)]
pub struct SledStorage {
    db: Arc<sled::Db>,
}

impl SledStorage {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, anyhow::Error> {
        let db = sled::open(path)?;
        Ok(Self { db: Arc::new(db) })
    }

    pub async fn save_task(&self, task: &MessageEnvelope) -> Result<(), anyhow::Error> {
        let db = self.db.clone();
        let task = task.clone();
        tokio::task::spawn_blocking(move || {
            let key = task.task_id.as_bytes();
            let value = serde_json::to_vec(&task)?;
            db.insert(key, value)?;
            db.flush()?;
            Ok::<(), anyhow::Error>(())
        })
        .await?
    }

    pub async fn load_task(&self, task_id: Uuid) -> Result<Option<MessageEnvelope>, anyhow::Error> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let key = task_id.as_bytes();
            if let Some(ivec) = db.get(key)? {
                let task: MessageEnvelope = serde_json::from_slice(&ivec)?;
                Ok(Some(task))
            } else {
                Ok(None)
            }
        })
        .await?
    }

    pub async fn load_all_tasks(&self) -> Result<Vec<MessageEnvelope>, anyhow::Error> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let mut tasks = Vec::new();
            for item in db.iter() {
                let (_, ivec) = item?;
                let task: MessageEnvelope = serde_json::from_slice(&ivec)?;
                tasks.push(task);
            }
            Ok(tasks)
        })
        .await?
    }

    pub async fn delete_task(&self, task_id: Uuid) -> Result<(), anyhow::Error> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let key = task_id.as_bytes();
            db.remove(key)?;
            db.flush()?;
            Ok::<(), anyhow::Error>(())
        })
        .await?
    }
}
