// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Generic HTTP server for the bits broker, exposing two endpoints (plus an
//! optional metrics endpoint when the `metrics-prometheus` feature is enabled):
//!
//! - `POST /job`    — submit a job; the server long-polls up to the configured
//!                    timeout and returns a result or redirects the client.
//! - `GET  /job/{id}` — reconnect after a poll redirect.
//! - `GET  /metrics`  — Prometheus text exposition (requires `metrics-prometheus` feature).
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

use crate::bits::PENDING_STATUS_HEADER;
use crate::{Bits, Job, JobResult, PollOutcome, SubmitOutcome};

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

const DEFAULT_POLL_TIMEOUT_SECS_RAW: u64 = 25;
const DEFAULT_POLL_TIMEOUT_SECS: f64 = DEFAULT_POLL_TIMEOUT_SECS_RAW as f64;
const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(DEFAULT_POLL_TIMEOUT_SECS_RAW);

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

    /// Long-poll timeout in seconds for the initial submit response
    /// and reconnect polls.
    #[serde(default = "default_poll_timeout_secs")]
    pub poll_timeout_secs: f64,

    /// Value for the Retry-After header on 529 overload and 429 rate-limit responses.
    #[serde(default = "default_retry_after_secs")]
    pub retry_after_secs: u64,
}

fn default_retry_after_secs() -> u64 {
    DEFAULT_RETRY_AFTER_SECS
}

fn default_server_host() -> String {
    "0.0.0.0".into()
}

fn default_server_port() -> u16 {
    8080
}

fn default_poll_timeout_secs() -> f64 {
    DEFAULT_POLL_TIMEOUT_SECS
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_server_host(),
            port: default_server_port(),
            poll_timeout_secs: default_poll_timeout_secs(),
            retry_after_secs: default_retry_after_secs(),
        }
    }
}

impl ServerConfig {
    /// Returns the poll timeout as a [`Duration`].
    ///
    /// Falls back to the default (25s) if `poll_timeout_secs` is negative,
    /// NaN, infinite, or overflows [`Duration`]. Configs produced by
    /// [`parse_bootstrap`](crate::parse_bootstrap) are always validated,
    /// so the fallback only applies to manually constructed instances.
    pub fn poll_timeout(&self) -> Duration {
        Duration::try_from_secs_f64(self.poll_timeout_secs).unwrap_or(DEFAULT_POLL_TIMEOUT)
    }
}

// ── Structured error response ───────────────────────────────────────────────

pub const CODE_JOB_NOT_FOUND: &str = "JOB_NOT_FOUND";
pub const CODE_JOB_LOST: &str = "JOB_LOST";
pub const CODE_JOB_ERROR: &str = "JOB_ERROR";
pub const CODE_JOB_FAILED: &str = "JOB_FAILED";
pub const CODE_ACTION_CANCELLED: &str = "ACTION_CANCELLED";
pub const CODE_ACTION_CLIENT_GONE: &str = "ACTION_CLIENT_GONE";
pub const CODE_QUEUE_FULL: &str = "QUEUE_FULL";
pub const CODE_USER_LIMIT_EXCEEDED: &str = "USER_LIMIT_EXCEEDED";

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

pub const DEFAULT_RETRY_AFTER_SECS: u64 = 5;

/// Returns an HTTP 529 (Site Overloaded) response with a Retry-After header.
/// 529 is non-standard but widely recognized (Cloudflare, Anthropic API) for
/// server-side overload distinct from 429 (per-client rate limiting).
fn overloaded_response(reason: &str, retry_after_secs: u64) -> Response {
    let status = StatusCode::from_u16(529).expect("529 is a valid HTTP status code");
    (
        status,
        [(header::RETRY_AFTER, retry_after_secs.to_string())],
        axum::Json(ErrorBody {
            code: CODE_QUEUE_FULL,
            message: reason.to_string(),
            retryable: true,
        }),
    )
        .into_response()
}

/// Returns an HTTP 429 (Too Many Requests) response with a Retry-After header.
/// Used for per-caller admission limits (e.g. per-user/per-realm/per-role
/// route caps), as distinct from [`overloaded_response`]'s system-wide 529.
fn rate_limited_response(reason: &str, retry_after_secs: u64) -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        [(header::RETRY_AFTER, retry_after_secs.to_string())],
        axum::Json(ErrorBody {
            code: CODE_USER_LIMIT_EXCEEDED,
            message: reason.to_string(),
            retryable: true,
        }),
    )
        .into_response()
}

// ── Axum state ──────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    bits: Arc<Bits>,
    poll_timeout: Duration,
    retry_after_secs: u64,
}

// ── Public entry point ──────────────────────────────────────────────────────

/// Build the axum [`Router`] for the bits HTTP API.
///
/// Returns a fully wired router that can be passed directly to [`axum::serve`],
/// or composed into a larger application.
pub fn router(bits: Arc<Bits>, poll_timeout: Duration, retry_after_secs: u64) -> Router {
    let state = AppState {
        bits,
        poll_timeout,
        retry_after_secs,
    };
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

/// Feature-gated router exposing `GET /metrics` in the Prometheus text format.
/// Kept separate so the core [`router`] and [`AppState`] stay untouched.
#[cfg(feature = "metrics-prometheus")]
fn metrics_router(handle: crate::metrics::PrometheusHandle) -> Router {
    async fn render(State(handle): State<crate::metrics::PrometheusHandle>) -> Response {
        let body = handle.render();
        ([(header::CONTENT_TYPE, handle.content_type())], body).into_response()
    }
    Router::new()
        .route("/metrics", get(render))
        .with_state(handle)
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

    let app = router(bits, config.poll_timeout(), config.retry_after_secs);
    // Metrics are process-global; pick up the handle installed by
    // `metrics::init_prometheus` (if any) rather than threading it through the
    // public signature, which would make the feature non-additive.
    #[cfg(feature = "metrics-prometheus")]
    let app = match crate::metrics::installed_handle() {
        Some(handle) => app.merge(metrics_router(handle)),
        None => app,
    };

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
    match state.bits.submit(Job::new(body)) {
        SubmitOutcome::Accepted(handle) => poll_by_id(&handle.id, &state).await,
        SubmitOutcome::Overloaded => {
            overloaded_response("broker at capacity", state.retry_after_secs)
        }
    }
}

async fn poll_job(Path(id): Path<String>, State(state): State<AppState>) -> Response {
    poll_by_id(&id, &state).await
}

async fn poll_by_id(id: &str, state: &AppState) -> Response {
    match state.bits.poll(id, Some(state.poll_timeout)).await {
        PollOutcome::Ready(result) => result_to_response(result, state.retry_after_secs),
        PollOutcome::Pending { id, status } => Response::builder()
            .status(StatusCode::SEE_OTHER)
            .header(header::LOCATION, format!("/job/{id}"))
            .header(header::RETRY_AFTER, "0")
            .header(PENDING_STATUS_HEADER, status.as_str())
            .body(Body::empty())
            .unwrap(),
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

fn result_to_response(result: JobResult, retry_after_secs: u64) -> Response {
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
        JobResult::Redirect {
            location,
            content_type,
            content_length,
            ..
        } => {
            // Carry the object's content metadata alongside the redirect so a
            // proxying broker can reconstruct the v1 redirect body without an
            // extra round-trip (see runtime::recovery::try_proxy_with_lease).
            let mut builder = Response::builder()
                .status(StatusCode::SEE_OTHER)
                .header(header::LOCATION, location);
            if let Some(ct) = content_type {
                builder = builder.header("x-polytope-content-type", ct);
            }
            if let Some(cl) = content_length {
                builder = builder.header("x-polytope-content-length", cl.to_string());
            }
            builder.body(Body::empty()).unwrap()
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
        JobResult::Overloaded { reason } => overloaded_response(&reason, retry_after_secs),
        JobResult::RateLimited { reason } => rate_limited_response(&reason, retry_after_secs),
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

#[cfg(all(test, feature = "metrics-prometheus"))]
mod metrics_router_tests {
    use super::*;

    #[tokio::test]
    async fn metrics_endpoint_returns_exposition() {
        let handle = crate::metrics::init_prometheus(crate::metrics::HistogramBuckets::default());
        let app = metrics_router(handle);

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve metrics");
        });

        let resp = reqwest::get(format!("http://{addr}/metrics"))
            .await
            .expect("request");
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert!(
            content_type.contains("text/plain"),
            "unexpected content-type: {content_type}"
        );
        let body = resp.text().await.expect("body");
        assert!(!body.is_empty(), "metrics body should not be empty");
    }
}
