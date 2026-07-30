use std::collections::HashMap;
use tokio::sync::{RwLock, Mutex};
use chrono::{DateTime, Utc, Duration};

#[derive(Debug, Clone)]
pub struct ModelLimit {
    pub tpm: u32, // Tokens Per Minute
    pub rpm: u32, // Requests Per Minute
}

#[derive(Debug, Clone)]
struct RateEvent {
    timestamp: DateTime<Utc>,
    tokens: u32,
}

#[derive(Debug)]
pub struct TokenRateLimiter {
    limits: RwLock<HashMap<String, ModelLimit>>,
    history: Mutex<HashMap<String, Vec<RateEvent>>>,
}

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

    pub async fn get_limit(&self, model: &str) -> Option<ModelLimit> {
        let limits = self.limits.read().await;
        limits.get(model).cloned()
    }

    /// Checks if a task with the given token budget can be executed right now.
    /// If it can, it records the request and returns Ok(()).
    /// Otherwise, it returns Err(String) describing which limit was exceeded.
    pub async fn check_and_record(&self, model: &str, token_budget: u32) -> Result<(), String> {
        let limits = self.limits.read().await;
        let limit = match limits.get(model) {
            Some(l) => l,
            None => return Ok(()), // No limits configured for this model, proceed immediately
        };

        let now = Utc::now();
        let cutoff = now - Duration::seconds(60);

        let mut history_map = self.history.lock().await;
        let events = history_map.entry(model.to_string()).or_default();

        // Prune old events outside our sliding window (older than 60 seconds)
        events.retain(|event| event.timestamp > cutoff);

        // Calculate current usage
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
            tokens: token_budget,
        });

        Ok(())
    }

    /// Reports actual token usage after task completion/checkpoint, adjusting the registered window.
    pub async fn report_actual_usage(&self, model: &str, reserved_tokens: u32, actual_tokens: u32) {
        if reserved_tokens == actual_tokens {
            return;
        }

        let mut history_map = self.history.lock().await;
        if let Some(events) = history_map.get_mut(model) {
            // Find the most recent event with the reserved tokens and adjust it,
            // or simply adjust the total weight.
            if let Some(event) = events.iter_mut().rev().find(|e| e.tokens == reserved_tokens) {
                event.tokens = actual_tokens;
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

        // First request: Should pass
        assert!(limiter.check_and_record("test-model", 100).await.is_ok());
        // Second request: Should pass
        assert!(limiter.check_and_record("test-model", 100).await.is_ok());
        // Third request: Should fail (RPM exceeded)
        let res = limiter.check_and_record("test-model", 100).await;
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("RPM limit exceeded"));
    }

    #[tokio::test]
    async fn test_rate_limiting_tpm() {
        let limiter = TokenRateLimiter::new();
        // Set limits: 500 tokens, 10 requests per minute
        limiter.set_limit("test-model".to_string(), 500, 10).await;

        // First request: Should pass
        assert!(limiter.check_and_record("test-model", 300).await.is_ok());
        // Second request: Should fail (TPM exceeded: 300 + 300 = 600 > 500)
        let res = limiter.check_and_record("test-model", 300).await;
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("TPM limit exceeded"));
    }

    #[tokio::test]
    async fn test_report_actual_usage() {
        let limiter = TokenRateLimiter::new();
        limiter.set_limit("test-model".to_string(), 500, 10).await;

        // Reserve 400 tokens (out of 500)
        assert!(limiter.check_and_record("test-model", 400).await.is_ok());

        // Second request for 200 tokens fails because 400 + 200 > 500
        assert!(limiter.check_and_record("test-model", 200).await.is_err());

        // Report that the first request only used 100 tokens
        limiter.report_actual_usage("test-model", 400, 100).await;

        // Second request should now pass, since active tokens is 100 + 200 = 300 <= 500
        assert!(limiter.check_and_record("test-model", 200).await.is_ok());
    }
}
