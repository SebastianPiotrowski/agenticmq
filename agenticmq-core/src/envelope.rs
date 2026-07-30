use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::{DateTime, Utc};

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Processing,
    Paused,
    Completed,
    Failed,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MessageEnvelope {
    pub task_id: Uuid,
    pub prompt_data: String,
    pub system_context: Option<String>,
    pub token_budget: u32,
    pub max_cost_usd: f64,
    pub trace_depth: u32,
    
    pub status: TaskStatus,
    pub resume_key: Option<String>,
    
    // Model routing and fallbacks
    pub current_model: String,
    pub fallback_models: Vec<String>,
    
    // Telemetry and accumulation
    pub tokens_used: u32,
    pub cost_usd: f64,
    
    // Output / errors
    pub output: Option<String>,
    pub error_message: Option<String>,
    
    // Timestamps
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MessageEnvelope {
    pub fn new(
        prompt_data: String,
        system_context: Option<String>,
        token_budget: u32,
        max_cost_usd: f64,
        model: String,
        fallback_models: Vec<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            task_id: Uuid::new_v4(),
            prompt_data,
            system_context,
            token_budget,
            max_cost_usd,
            trace_depth: 0,
            status: TaskStatus::Pending,
            resume_key: None,
            current_model: model,
            fallback_models,
            tokens_used: 0,
            cost_usd: 0.0,
            output: None,
            error_message: None,
            created_at: now,
            updated_at: now,
        }
    }
}
