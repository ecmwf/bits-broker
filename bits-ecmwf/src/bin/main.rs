// bits-ecmwf: the BITS HTTP service with ECMWF actions pre-loaded.
//
// ECMWF actions are registered automatically at startup via inventory.
// Configure via a YAML file passed as the first argument.
//
// Usage: bits-ecmwf <config.yaml>

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

const DEFAULT_BIND: &str = "0.0.0.0:8080";
const POLL_TIMEOUT: Duration = Duration::from_secs(25);

#[derive(Clone)]
struct AppState {
    bits: Arc<Bits>,
}

async fn submit_job(State(state): State<AppState>, Json(body): Json<Value>) -> Response {
    let handle = state.bits.submit(Job::new(body));
    poll_by_id(&handle.id, &state).await
}

async fn poll_job(Path(id): Path<String>, State(state): State<AppState>) -> Response {
    poll_by_id(&id, &state).await
}

async fn poll_by_id(id: &str, state: &AppState) -> Response {
    match state.bits.poll(id, Some(POLL_TIMEOUT)).await {
        PollOutcome::Ready(result) => match result {
            JobResult::Success { content_type, stream, .. } => {
                ([(header::CONTENT_TYPE, content_type)], Body::from_stream(stream)).into_response()
            }
            JobResult::Redirect { location, .. } => {
                (StatusCode::SEE_OTHER, [(header::LOCATION, location)]).into_response()
            }
            JobResult::Error { message } => (StatusCode::BAD_REQUEST, message).into_response(),
            JobResult::Failed { reason } => {
                (StatusCode::INTERNAL_SERVER_ERROR, reason).into_response()
            }
            JobResult::Cancelled => StatusCode::GONE.into_response(),
            JobResult::ClientGone => StatusCode::GONE.into_response(),
        },
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config_path = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: bits-ecmwf <config.yaml>");
        std::process::exit(1);
    });
    let config_str = std::fs::read_to_string(&config_path)?;
    let bits = Arc::new(Bits::from_config(&config_str)?);

    let state = AppState { bits };
    let app = Router::new()
        .route("/job", post(submit_job))
        .route("/job/{id}", get(poll_job))
        .with_state(state);

    let listener = TcpListener::bind(DEFAULT_BIND).await?;
    tracing::info!(address = %listener.local_addr()?, "bits-ecmwf listening");
    axum::serve(listener, app).await?;
    Ok(())
}
