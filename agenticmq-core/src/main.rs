mod envelope;
mod limiter;
mod storage;
mod broker;
mod error;

use std::sync::Arc;
use axum::{
    extract::{Path, Query, State},
    http::{StatusCode, HeaderMap},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use uuid::Uuid;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::broker::Broker;
use crate::envelope::MessageEnvelope;
use crate::error::BrokerError;

struct AppError(BrokerError);

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self.0 {
            BrokerError::TaskNotFound(_) => StatusCode::NOT_FOUND,
            BrokerError::InvalidStatus { .. } => StatusCode::BAD_REQUEST,
            BrokerError::InvalidResumeKey(_) => StatusCode::FORBIDDEN,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (
            status,
            Json(serde_json::json!({ "error": self.0.to_string() })),
        )
            .into_response()
    }
}

impl From<BrokerError> for AppError {
    fn from(err: BrokerError) -> Self {
        AppError(err)
    }
}

type ResultResponse<T> = Result<T, AppError>;

#[derive(Deserialize)]
struct SubmitTaskPayload {
    prompt_data: String,
    system_context: Option<String>,
    token_budget: u32,
    max_cost_usd: f64,
    model: String,
    fallback_models: Option<Vec<String>>,
    priority: Option<i8>,
    ttl_seconds: Option<u64>,
    max_retries: Option<u32>,
}

#[derive(Deserialize)]
struct PollParams {
    model: String,
    timeout_seconds: Option<u64>,
}

#[derive(Deserialize)]
struct CheckpointPayload {
    tokens_used: u32,
    cost_usd: f64,
    output: Option<String>,
    error_message: Option<String>,
    pause_request: bool,
}

#[derive(Deserialize)]
struct CompletePayload {
    output: String,
    tokens_used: u32,
    cost_usd: f64,
}

#[derive(Deserialize)]
struct FailPayload {
    error: String,
    tokens_used: u32,
    cost_usd: f64,
}

#[derive(Deserialize)]
struct LimitPayload {
    model: String,
    tpm: u32,
    rpm: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Directory path to store state files
    let state_dir = std::env::var("AGENTICMQ_STATE_DIR").unwrap_or_else(|_| ".agenticmq_state".to_string());
    let broker = Arc::new(Broker::new(&state_dir).await?);

    // Start background garbage collector (sweeping once per hour)
    broker.start_garbage_collector(tokio::time::Duration::from_secs(3600));

    // Setup CORS middleware
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Build routes
    let app = Router::new()
        .route("/tasks", post(submit_task))
        .route("/tasks/poll", get(poll_task))
        .route("/tasks/:task_id", get(get_task))
        .route("/tasks/:task_id/checkpoint", post(checkpoint_task))
        .route("/tasks/:task_id/complete", post(complete_task))
        .route("/tasks/:task_id/fail", post(fail_task))
        .route("/tasks/:task_id/resume", post(resume_task))
        .route("/limits", post(set_limit))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(broker);

    let host = std::env::var("AGENTICMQ_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("AGENTICMQ_PORT").unwrap_or_else(|_| "8080".to_string());
    let addr = format!("{}:{}", host, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("AgenticMQ Broker listening on http://{}", addr);
    axum::serve(listener, app).await?;

    Ok(())
}

async fn submit_task(
    State(broker): State<Arc<Broker>>,
    Json(payload): Json<SubmitTaskPayload>,
) -> ResultResponse<(StatusCode, Json<MessageEnvelope>)> {
    let task = broker
        .submit_task(
            payload.prompt_data,
            payload.system_context,
            payload.token_budget,
            payload.max_cost_usd,
            payload.model,
            payload.fallback_models.unwrap_or_default(),
            payload.priority.unwrap_or(0),
            payload.ttl_seconds,
            payload.max_retries.unwrap_or(3),
        )
        .await?;

    Ok((StatusCode::CREATED, Json(task)))
}

async fn poll_task(
    State(broker): State<Arc<Broker>>,
    Query(params): Query<PollParams>,
) -> ResultResponse<Json<Option<MessageEnvelope>>> {
    let timeout_dur = std::time::Duration::from_secs(params.timeout_seconds.unwrap_or(30));
    let task = broker.poll_task_long(&params.model, timeout_dur).await;
    Ok(Json(task))
}

async fn get_task(
    State(broker): State<Arc<Broker>>,
    Path(task_id): Path<Uuid>,
) -> ResultResponse<Json<MessageEnvelope>> {
    let task = broker
        .get_task(task_id)
        .await
        .ok_or_else(|| AppError(BrokerError::TaskNotFound(task_id)))?;
    Ok(Json(task))
}

async fn checkpoint_task(
    State(broker): State<Arc<Broker>>,
    Path(task_id): Path<Uuid>,
    Json(payload): Json<CheckpointPayload>,
) -> ResultResponse<Json<MessageEnvelope>> {
    let task = broker
        .checkpoint_task(
            task_id,
            payload.tokens_used,
            payload.cost_usd,
            payload.output,
            payload.error_message,
            payload.pause_request,
        )
        .await?;

    Ok(Json(task))
}

async fn complete_task(
    State(broker): State<Arc<Broker>>,
    Path(task_id): Path<Uuid>,
    Json(payload): Json<CompletePayload>,
) -> ResultResponse<Json<MessageEnvelope>> {
    let task = broker
        .complete_task(task_id, payload.output, payload.tokens_used, payload.cost_usd)
        .await?;

    Ok(Json(task))
}

async fn fail_task(
    State(broker): State<Arc<Broker>>,
    Path(task_id): Path<Uuid>,
    Json(payload): Json<FailPayload>,
) -> ResultResponse<Json<MessageEnvelope>> {
    let task = broker
        .fail_task(task_id, payload.error, payload.tokens_used, payload.cost_usd)
        .await?;

    Ok(Json(task))
}

async fn resume_task(
    State(broker): State<Arc<Broker>>,
    Path(task_id): Path<Uuid>,
    headers: HeaderMap,
) -> ResultResponse<Json<MessageEnvelope>> {
    // Look for x-resume-key header, supporting case-insensitive matching
    let resume_key = headers
        .get("x-resume-key")
        .or_else(|| headers.get("X-Resume-Key"))
        .and_then(|val| val.to_str().ok())
        .ok_or_else(|| AppError(BrokerError::Internal("Missing x-resume-key header".to_string())))?;

    let task = broker.resume_task(task_id, resume_key).await?;
    Ok(Json(task))
}

async fn set_limit(
    State(broker): State<Arc<Broker>>,
    Json(payload): Json<LimitPayload>,
) -> impl IntoResponse {
    broker.limiter.set_limit(payload.model, payload.tpm, payload.rpm).await;
    StatusCode::OK
}
