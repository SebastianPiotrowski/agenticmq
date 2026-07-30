use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum BrokerError {
    #[error("Task {0} not found")]
    TaskNotFound(Uuid),

    #[error("Task {task_id} is not in {expected} state (current: {actual:?})")]
    InvalidStatus {
        task_id: Uuid,
        expected: String,
        actual: crate::envelope::TaskStatus,
    },

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Invalid or missing resume key for task {0}")]
    InvalidResumeKey(Uuid),

    #[error("Internal error: {0}")]
    Internal(String),
}
