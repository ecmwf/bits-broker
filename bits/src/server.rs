//! Generic HTTP server for the bits broker.
//!
//! Exposes a jobs endpoint:
//!   POST /job          — submit a job; long-polls up to the configured timeout, then redirects
//!   GET  /job/{id}     — reconnect after a poll redirect
//!
//! Usage from a binary crate:
//! ```ignore
//! let (bits, server_config) = bits::parse_bootstrap(&config_str)?.into_parts()?;
//! let bits = Arc::new(bits);
//! bits::server::serve(bits, server_config).await?;
//! ```

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Json, Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::Deserialize;
use serde_json::Value;
use tokio::net::TcpListener;

use crate::{Bits, Job, JobResult, PollOutcome};

pub async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    result = ctrl_c => {
                        if let Err(e) = result {
                            tracing::error!(error = %e, "Ctrl-C listener failed, waiting on SIGTERM");
                            sigterm.recv().await;
                        }
                    }
                    _ = sigterm.recv() => {}
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "SIGTERM registration failed, falling back to Ctrl-C");
                if let Err(e) = ctrl_c.await {
                    tracing::error!(error = %e, "Ctrl-C listener also failed, server will run until killed");
                    std::future::pending::<()>().await;
                }
            }
        }
    }
    #[cfg(not(unix))]
    {
        if let Err(e) = ctrl_c.await {
            tracing::error!(error = %e, "Ctrl-C listener failed, server will run until killed");
            std::future::pending::<()>().await;
        }
    }
    tracing::info!("shutdown signal received, draining");
}

const DEFAULT_POLL_TIMEOUT_MS: u64 = 25_000;

/// Configuration for the built-in HTTP server.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Host interface to bind to (e.g. `"0.0.0.0"`).
    #[serde(default = "default_server_host")]
    pub host: String,

    /// TCP port to bind to (e.g. `8080`).
    #[serde(default = "default_server_port")]
    pub port: u16,

    /// Long-poll timeout in milliseconds for the initial submit response
    /// and reconnect polls.
    #[serde(default = "default_poll_timeout_ms")]
    pub poll_timeout_ms: u64,
}

fn default_server_host() -> String {
    "0.0.0.0".into()
}

fn default_server_port() -> u16 {
    8080
}

fn default_poll_timeout_ms() -> u64 {
    DEFAULT_POLL_TIMEOUT_MS
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_server_host(),
            port: default_server_port(),
            poll_timeout_ms: default_poll_timeout_ms(),
        }
    }
}

impl ServerConfig {
    /// Returns the poll timeout as a [`Duration`].
    pub fn poll_timeout(&self) -> Duration {
        Duration::from_millis(self.poll_timeout_ms)
    }
}

// ── Structured error response ───────────────────────────────────────────────

pub const CODE_JOB_NOT_FOUND: &str = "JOB_NOT_FOUND";
pub const CODE_JOB_LOST: &str = "JOB_LOST";
pub const CODE_JOB_ERROR: &str = "JOB_ERROR";
pub const CODE_JOB_FAILED: &str = "JOB_FAILED";
pub const CODE_ACTION_CANCELLED: &str = "ACTION_CANCELLED";
pub const CODE_ACTION_CLIENT_GONE: &str = "ACTION_CLIENT_GONE";

#[derive(serde::Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
    retryable: bool,
}

fn json_error(
    status: StatusCode,
    code: &'static str,
    message: impl Into<String>,
    retryable: bool,
) -> Response {
    (
        status,
        axum::Json(ErrorBody {
            code,
            message: message.into(),
            retryable,
        }),
    )
        .into_response()
}

// ── Axum state ──────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    bits: Arc<Bits>,
    poll_timeout: Duration,
}

// ── Public entry point ──────────────────────────────────────────────────────

/// Build the axum [`Router`] for the bits HTTP API.
///
/// Returns a fully wired router that can be passed directly to [`axum::serve`],
/// or composed into a larger application.
pub fn router(bits: Arc<Bits>, poll_timeout: Duration) -> Router {
    let state = AppState { bits, poll_timeout };
    Router::new()
        .route("/job", post(submit_job))
        .route("/job/{id}", get(poll_job))
        .with_state(state)
}

/// Start the HTTP server. Runs until the process is terminated.
pub async fn serve(
    bits: Arc<Bits>,
    config: ServerConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    serve_with_shutdown(bits, config, std::future::pending()).await
}

/// Start the HTTP server with a graceful shutdown future.
///
/// When `shutdown` completes, the server stops accepting new connections
/// and drains in-flight requests before returning.
pub async fn serve_with_shutdown(
    bits: Arc<Bits>,
    config: ServerConfig,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error>> {
    let broker_id = bits.broker_id().to_string();
    let routes = bits.route_names().join(", ");

    let app = router(bits, config.poll_timeout());

    let bind_addr = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(&bind_addr).await?;
    let local_addr = listener.local_addr()?;

    eprintln!();
    eprintln!("  \x1b[1m\x1b[96mbits\x1b[0m");
    eprintln!("  \x1b[2mbroker\x1b[0m  {broker_id}");
    eprintln!("  \x1b[2mserver\x1b[0m  {local_addr}");
    eprintln!("  \x1b[2mroutes\x1b[0m  {routes}");
    eprintln!();

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

// ── Handlers ────────────────────────────────────────────────────────────────

async fn submit_job(State(state): State<AppState>, Json(body): Json<Value>) -> Response {
    let handle = state.bits.submit(Job::new(body));
    poll_by_id(&handle.id, &state).await
}

async fn poll_job(Path(id): Path<String>, State(state): State<AppState>) -> Response {
    poll_by_id(&id, &state).await
}

async fn poll_by_id(id: &str, state: &AppState) -> Response {
    match state.bits.poll(id, Some(state.poll_timeout)).await {
        PollOutcome::Ready(result) => result_to_response(result),
        PollOutcome::Pending { id } => (
            StatusCode::SEE_OTHER,
            [
                (header::LOCATION, format!("/job/{id}")),
                (header::RETRY_AFTER, "0".to_string()),
            ],
        )
            .into_response(),
        PollOutcome::NotFound => json_error(
            StatusCode::NOT_FOUND,
            CODE_JOB_NOT_FOUND,
            "job does not exist or was already consumed",
            false,
        ),
        PollOutcome::JobLost => json_error(
            StatusCode::GONE,
            CODE_JOB_LOST,
            "job existed but is no longer recoverable",
            false,
        ),
    }
}

fn result_to_response(result: JobResult) -> Response {
    match result {
        JobResult::Success {
            content_type,
            stream,
            ..
        } => (
            [(header::CONTENT_TYPE, content_type)],
            Body::from_stream(stream),
        )
            .into_response(),
        JobResult::Redirect { location, .. } => {
            (StatusCode::SEE_OTHER, [(header::LOCATION, location)]).into_response()
        }
        JobResult::Error { message } => {
            json_error(StatusCode::BAD_REQUEST, CODE_JOB_ERROR, message, false)
        }
        JobResult::Failed { .. } => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            CODE_JOB_FAILED,
            "internal server error",
            false,
        ),
        JobResult::Cancelled => json_error(
            StatusCode::GONE,
            CODE_ACTION_CANCELLED,
            "job was cancelled",
            false,
        ),
        JobResult::ClientGone => json_error(
            StatusCode::GONE,
            CODE_ACTION_CLIENT_GONE,
            "client disconnected",
            false,
        ),
    }
}
