use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    routing::{get, post},
};
use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::actions::{ActionError, TargetResult};
use crate::dispatcher::queue::Queue;
use crate::dispatcher::{DispatchGuard, Executor, PendingMap};
use crate::result::JobResult;
use crate::worker_server::WorkerServer;

fn default_heartbeat_timeout_secs() -> f64 {
    60.0
}

/// Configuration for the remote-pool executor.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RemotePoolConfig {
    /// Seconds without a heartbeat before an in-progress job is evicted.
    /// Fractional values are supported (e.g. 0.1 for 100 ms).
    pub heartbeat_timeout_secs: f64,
}

impl<'de> serde::Deserialize<'de> for RemotePoolConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};
        use std::fmt;

        struct RemotePoolConfigVisitor;

        impl<'de> Visitor<'de> for RemotePoolConfigVisitor {
            type Value = RemotePoolConfig;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a RemotePoolConfig object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut heartbeat_timeout_secs = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "heartbeat_timeout_secs" => {
                            if heartbeat_timeout_secs.is_some() {
                                return Err(de::Error::duplicate_field("heartbeat_timeout_secs"));
                            }
                            heartbeat_timeout_secs = Some(map.next_value()?);
                        }
                        "bind" => {
                            return Err(de::Error::custom(
                                "remote_pool.bind has been removed; configure host and port at bits.worker_server instead",
                            ));
                        }
                        _ => {
                            return Err(de::Error::unknown_field(
                                &key,
                                &["heartbeat_timeout_secs"],
                            ));
                        }
                    }
                }

                Ok(RemotePoolConfig {
                    heartbeat_timeout_secs: heartbeat_timeout_secs
                        .unwrap_or_else(default_heartbeat_timeout_secs),
                })
            }
        }

        deserializer.deserialize_map(RemotePoolConfigVisitor)
    }
}

// ─── worker outcome ───────────────────────────────────────────────────────────

enum WorkerOutcome {
    Complete {
        content_type: String,
        size: i64,
        stream: Box<dyn futures::Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>,
    },
    Redirect {
        location: String,
        message: String,
    },
    Reject {
        reason: String,
    },
    Error {
        message: String,
    },
}

// ─── shared state ─────────────────────────────────────────────────────────────

struct InProgressJob {
    result_tx: oneshot::Sender<WorkerOutcome>,
    last_heartbeat: Instant,
}

/// State shared between the HTTP handlers and the executor.
///
/// Holds references to the dispatcher queue and pending map. The `/work`
/// handler calls `queue.dequeue()` directly when a worker polls.
struct RemotePoolState {
    queue: Arc<dyn Queue>,
    /// Pending map holding work futures and reply channels, keyed by job ID.
    /// The `/work` handler removes the entry after dequeue to drop the local
    /// work future (remote workers do all work externally). The reply channel
    /// is moved to `in_progress` so completion handlers can send back results.
    pending: Arc<PendingMap<TargetResult>>,
    in_progress: DashMap<String, InProgressJob>,
    heartbeat_timeout: Duration,
}

// ─── HTTP request / response types ────────────────────────────────────────────

#[derive(Deserialize)]
struct PollParams {
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
}

fn default_timeout_ms() -> u64 {
    30_000
}

/// Returned to the worker on a successful `GET /work` poll.
#[derive(Serialize)]
struct WorkResponse {
    job_id: String,
    request: serde_json::Value,
    user: serde_json::Value,
    metadata: serde_json::Value,
}

#[derive(Deserialize)]
struct RedirectRequest {
    location: String,
    #[serde(default)]
    message: String,
}

#[derive(Deserialize)]
struct RejectRequest {
    reason: String,
}

#[derive(Deserialize)]
struct ErrorRequest {
    message: String,
}

// ─── axum handlers ────────────────────────────────────────────────────────────

/// Long-poll endpoint. Blocks until a job is dequeued or `timeout_ms` elapses.
/// Returns 200 + job JSON, or 204 No Content on timeout.
///
/// This handler directly calls `queue.dequeue()` — the queue determines ordering.
/// After dequeue, it resolves the pending entry (dropping the local work future,
/// since remote workers do all work externally) and moves the reply channel into
/// `in_progress`.
async fn handle_get_work(
    State(state): State<Arc<RemotePoolState>>,
    Query(params): Query<PollParams>,
) -> Result<Json<WorkResponse>, StatusCode> {
    let timeout = Duration::from_millis(params.timeout_ms);

    let job = tokio::select! {
        biased;
        result = state.queue.dequeue() => {
            match result {
                Some(job) => job,
                None => return Err(StatusCode::NO_CONTENT),
            }
        }
        _ = tokio::time::sleep(timeout) => {
            return Err(StatusCode::NO_CONTENT);
        }
    };

    // Resolve the pending entry for this job.
    let item = state.pending.lock().unwrap().remove(&job.id);
    let Some((guard, _work, reply_tx)) = item else {
        // Caller cancelled before the work handler picked it up — skip.
        // Return 204 to tell the worker to poll again.
        return Err(StatusCode::NO_CONTENT);
    };

    if reply_tx.is_closed() {
        // Caller cancelled — drop.
        return Err(StatusCode::NO_CONTENT);
    }

    match guard {
        DispatchGuard::None => {}
        DispatchGuard::Cancelled => {
            if job.is_cancelled() {
                let _ = reply_tx.send(Err(ActionError::Cancelled));
                return Err(StatusCode::NO_CONTENT);
            }
        }
        DispatchGuard::CancelledOrClientGone => {
            if job.is_cancelled() {
                let _ = reply_tx.send(Err(ActionError::Cancelled));
                return Err(StatusCode::NO_CONTENT);
            }
            if !job.client_present() {
                let _ = reply_tx.send(Err(ActionError::ClientGone));
                return Err(StatusCode::NO_CONTENT);
            }
        }
    }

    // Build the one-shot channel for the worker outcome.
    let (outcome_tx, outcome_rx) = oneshot::channel::<WorkerOutcome>();

    state.in_progress.insert(
        job.id.clone(),
        InProgressJob {
            result_tx: outcome_tx,
            last_heartbeat: Instant::now(),
        },
    );

    // Spawn a task that waits for the worker outcome and translates it into
    // the TargetResult sent back to the original dispatcher caller.
    tokio::spawn(async move {
        let result = match outcome_rx.await {
            Ok(outcome) => match outcome {
                WorkerOutcome::Complete {
                    content_type,
                    size,
                    stream,
                } => Ok(TargetResult::Complete(JobResult::Success {
                    content_type,
                    size,
                    stream,
                })),
                WorkerOutcome::Redirect { location, message } => {
                    Ok(TargetResult::Complete(JobResult::Redirect {
                        location,
                        message,
                    }))
                }
                WorkerOutcome::Reject { reason } => Ok(TargetResult::Reject { reason }),
                WorkerOutcome::Error { message } => Err(ActionError::ResourceError(message)),
            },
            Err(_) => Err(ActionError::ResourceError(
                "worker heartbeat timeout or disconnect".into(),
            )),
        };
        let _ = reply_tx.send(result);
    });

    let resp = WorkResponse {
        job_id: job.id.clone(),
        request: job.request.clone(),
        user: job.user.clone(),
        metadata: job.metadata.clone(),
    };

    Ok(Json(resp))
}

/// Heartbeat endpoint. Workers call this periodically to prevent timeout eviction.
async fn handle_heartbeat(
    State(state): State<Arc<RemotePoolState>>,
    Path(job_id): Path<String>,
) -> StatusCode {
    match state.in_progress.get_mut(&job_id) {
        Some(mut entry) => {
            entry.last_heartbeat = Instant::now();
            StatusCode::OK
        }
        None => StatusCode::NOT_FOUND,
    }
}

/// Completion data endpoint. Worker posts the successful response body as a
/// streaming HTTP request body; the waiting caller receives a
/// streaming `JobResult::Success`.
async fn handle_complete_data(
    State(state): State<Arc<RemotePoolState>>,
    Path(job_id): Path<String>,
    request: axum::extract::Request,
) -> StatusCode {
    match state.in_progress.remove(&job_id) {
        Some((_, entry)) => {
            let content_type = request
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("application/octet-stream")
                .to_string();
            let size = request
                .headers()
                .get(header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(-1);

            let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(16);
            let mut body = request.into_body().into_data_stream();
            tokio::spawn(async move {
                while let Some(frame) = body.next().await {
                    match frame {
                        Ok(bytes) => {
                            if tx.send(Ok(bytes)).await.is_err() {
                                break;
                            }
                        }
                        Err(err) => {
                            let _ = tx.send(Err(std::io::Error::other(err.to_string()))).await;
                            break;
                        }
                    }
                }
            });

            let outcome = WorkerOutcome::Complete {
                content_type,
                size,
                stream: Box::new(tokio_stream::wrappers::ReceiverStream::new(rx)),
            };
            let _ = entry.result_tx.send(outcome);
            StatusCode::OK
        }
        None => StatusCode::NOT_FOUND,
    }
}

async fn handle_complete_redirect(
    State(state): State<Arc<RemotePoolState>>,
    Path(job_id): Path<String>,
    Json(req): Json<RedirectRequest>,
) -> StatusCode {
    match state.in_progress.remove(&job_id) {
        Some((_, entry)) => {
            let _ = entry.result_tx.send(WorkerOutcome::Redirect {
                location: req.location,
                message: req.message,
            });
            StatusCode::OK
        }
        None => StatusCode::NOT_FOUND,
    }
}

async fn handle_complete_reject(
    State(state): State<Arc<RemotePoolState>>,
    Path(job_id): Path<String>,
    Json(req): Json<RejectRequest>,
) -> StatusCode {
    match state.in_progress.remove(&job_id) {
        Some((_, entry)) => {
            let _ = entry
                .result_tx
                .send(WorkerOutcome::Reject { reason: req.reason });
            StatusCode::OK
        }
        None => StatusCode::NOT_FOUND,
    }
}

async fn handle_complete_error(
    State(state): State<Arc<RemotePoolState>>,
    Path(job_id): Path<String>,
    Json(req): Json<ErrorRequest>,
) -> StatusCode {
    match state.in_progress.remove(&job_id) {
        Some((_, entry)) => {
            let _ = entry.result_tx.send(WorkerOutcome::Error {
                message: req.message,
            });
            StatusCode::OK
        }
        None => StatusCode::NOT_FOUND,
    }
}

// ─── executor ─────────────────────────────────────────────────────────────────

/// Dispatches jobs to external workers via HTTP long-poll.
///
/// On `start_scheduler`, this spawns an axum HTTP server with endpoints:
///
/// - `GET  /work?timeout_ms=N`   — long-poll; directly dequeues from the
///   dispatcher queue, drops the local work future, and returns job JSON.
///   Returns 204 on timeout.
/// - `POST /heartbeat/{job_id}`  — worker keepalive; resets the heartbeat timer.
/// - `POST /complete/data/{job_id}`     — worker streams the successful body.
/// - `POST /complete/redirect/{job_id}` — worker posts redirect JSON.
/// - `POST /complete/reject/{job_id}`   — worker posts reject JSON.
/// - `POST /complete/error/{job_id}`    — worker posts error JSON.
///
/// A background reaper task evicts jobs whose heartbeat has expired, which
/// causes the suspended caller to receive a `ResourceError`.
///
/// This executor must always be paired with a `remote` target action.
pub struct RemotePoolExecutor {
    pool_name: String,
    heartbeat_timeout: Duration,
    worker_server: Arc<WorkerServer>,
}

impl RemotePoolExecutor {
    pub fn new(pool_name: &str, heartbeat_timeout: Duration, worker_server: Arc<WorkerServer>) -> Self {
        Self {
            pool_name: pool_name.to_string(),
            heartbeat_timeout,
            worker_server,
        }
    }
}

impl Executor<TargetResult> for RemotePoolExecutor {
    fn start_scheduler(&self, queue: Arc<dyn Queue>, pending: Arc<PendingMap<TargetResult>>) {
        let state = Arc::new(RemotePoolState {
            queue,
            pending,
            in_progress: DashMap::new(),
            heartbeat_timeout: self.heartbeat_timeout,
        });

        // HTTP server task.
        let app = Router::new()
            .route("/work", get(handle_get_work))
            .route("/heartbeat/{job_id}", post(handle_heartbeat))
            .route("/complete/data/{job_id}", post(handle_complete_data))
            .route(
                "/complete/redirect/{job_id}",
                post(handle_complete_redirect),
            )
            .route("/complete/reject/{job_id}", post(handle_complete_reject))
            .route("/complete/error/{job_id}", post(handle_complete_error))
            .with_state(Arc::clone(&state));

        self.worker_server
            .register_pool(&self.pool_name, app)
            .unwrap_or_else(|e| {
                panic!(
                    "remote_pool: failed to register pool '{}': {e}",
                    self.pool_name
                )
            });

        // Heartbeat reaper task.
        let heartbeat_timeout = self.heartbeat_timeout;
        let reaper_state = Arc::clone(&state);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat_timeout / 2);
            loop {
                interval.tick().await;
                let now = Instant::now();
                reaper_state.in_progress.retain(|job_id, entry| {
                    let alive =
                        now.duration_since(entry.last_heartbeat) < reaper_state.heartbeat_timeout;
                    if !alive {
                        tracing::warn!(
                            job_id,
                            "remote_pool: evicting job due to heartbeat timeout"
                        );
                    }
                    alive
                });
            }
        });
    }
}
