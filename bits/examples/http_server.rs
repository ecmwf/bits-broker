//! Minimal HTTP server built on top of the bits library.
//!
//! Demonstrates the submit/poll pattern:
//!   POST /job          — submit a job; long-polls up to POLL_TIMEOUT, then redirects
//!   GET  /job/{id}     — reconnect after a poll redirect
//!
//! Run with:
//!   cargo run --example http_server

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Json, Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use bits::{Bits, Job, JobResult, PollOutcome};
use serde_json::Value;
use tokio::net::TcpListener;

const BIND: &str = "0.0.0.0:8080";
const POLL_TIMEOUT: Duration = Duration::from_secs(25);

// ================================
//   State
// ================================

#[derive(Clone)]
struct AppState {
    bits: Arc<Bits>,
}

// ================================
//   Handlers
// ================================

async fn submit_job(State(state): State<AppState>, Json(body): Json<Value>) -> Response {
    let handle = state.bits.submit(Job::new(body));
    poll_by_id(&handle.id, &state).await
}

async fn poll_job(Path(id): Path<String>, State(state): State<AppState>) -> Response {
    poll_by_id(&id, &state).await
}

// ================================
//   Poll logic
// ================================

/// Wait for a job result, long-polling up to POLL_TIMEOUT.
/// On timeout, redirects the client back to GET /job/{id} to reconnect.
async fn poll_by_id(id: &str, state: &AppState) -> Response {
    match state.bits.poll(id, POLL_TIMEOUT).await {
        PollOutcome::Ready(result) => result_to_response(result),
        PollOutcome::Pending { id } => (
            StatusCode::SEE_OTHER,
            [
                (header::LOCATION, format!("/job/{id}")),
                (header::RETRY_AFTER, "0".to_string()),
            ],
        )
            .into_response(),
        PollOutcome::NotFound => StatusCode::NOT_FOUND.into_response(),
    }
}

fn result_to_response(result: JobResult) -> Response {
    match result {
        JobResult::Success { content_type, stream, .. } => {
            ([(header::CONTENT_TYPE, content_type)], Body::from_stream(stream)).into_response()
        }
        JobResult::Redirect { location, .. } => {
            (StatusCode::SEE_OTHER, [(header::LOCATION, location)]).into_response()
        }
        JobResult::Error { message } => (StatusCode::BAD_REQUEST, message).into_response(),
        JobResult::Failed { reason } => (StatusCode::INTERNAL_SERVER_ERROR, reason).into_response(),
    }
}

// ================================
//   Main
// ================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().init();

    let config = r#"
routes:
  default:
    - target::http:
        url: http://localhost:8081
"#;

    let bits = Arc::new(Bits::from_config(config)?);
    let state = AppState { bits };

    let app = Router::new()
        .route("/job", post(submit_job))
        .route("/job/{id}", get(poll_job))
        .with_state(state);

    let listener = TcpListener::bind(BIND).await?;
    tracing::info!(address = %listener.local_addr()?, "listening");
    axum::serve(listener, app).await?;
    Ok(())
}
