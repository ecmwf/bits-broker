use std::any::TypeId;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
};
use bytes::Bytes;
use dashmap::DashMap;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, oneshot};

use crate::actions::{ActionError, TargetResult, target_remote::RemoteTarget};
use crate::dispatcher::Executor;
use crate::job::Job;
use crate::result::JobResult;

// ─── worker outcome ───────────────────────────────────────────────────────────

enum WorkerOutcome {
    Complete { content_type: String, body: Bytes },
    Redirect { location: String, message: String },
    Reject { reason: String },
    Error { message: String },
}

// ─── shared state ─────────────────────────────────────────────────────────────

struct AvailableJob {
    job: Job,
    result_tx: oneshot::Sender<WorkerOutcome>,
}

struct InProgressJob {
    result_tx: oneshot::Sender<WorkerOutcome>,
    last_heartbeat: Instant,
}

struct RemotePoolState {
    available: Mutex<VecDeque<AvailableJob>>,
    available_notify: Notify,
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

/// Body the worker POSTs to `POST /complete/{job_id}`.
///
/// `body` is the raw response payload as a UTF-8 string. For binary data the
/// worker should base64-encode it and set `content_type` accordingly; decoding
/// is left to the downstream consumer.
#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum CompleteRequest {
    Complete {
        content_type: String,
        body: String,
    },
    Redirect {
        location: String,
        #[serde(default)]
        message: String,
    },
    Reject {
        reason: String,
    },
    Error {
        message: String,
    },
}

// ─── axum handlers ────────────────────────────────────────────────────────────

/// Long-poll endpoint. Blocks until a job is available or `timeout_ms` elapses.
/// Returns 200 + job JSON, or 204 No Content on timeout.
async fn handle_get_work(
    State(state): State<Arc<RemotePoolState>>,
    Query(params): Query<PollParams>,
) -> Result<Json<WorkResponse>, StatusCode> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(params.timeout_ms);

    loop {
        {
            let mut queue = state.available.lock().unwrap();
            if let Some(entry) = queue.pop_front() {
                let resp = WorkResponse {
                    job_id: entry.job.id.clone(),
                    request: entry.job.request.clone(),
                    user: entry.job.user.clone(),
                    metadata: entry.job.metadata.clone(),
                };
                state.in_progress.insert(
                    entry.job.id.clone(),
                    InProgressJob {
                        result_tx: entry.result_tx,
                        last_heartbeat: Instant::now(),
                    },
                );
                return Ok(Json(resp));
            }
        }

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(StatusCode::NO_CONTENT);
        }

        tokio::select! {
            _ = state.available_notify.notified() => {},
            _ = tokio::time::sleep(remaining) => {},
        }
    }
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

/// Completion endpoint. Worker posts the outcome; the waiting `execute()` future
/// is unblocked and the job is removed from the in-progress map.
async fn handle_complete(
    State(state): State<Arc<RemotePoolState>>,
    Path(job_id): Path<String>,
    Json(req): Json<CompleteRequest>,
) -> StatusCode {
    match state.in_progress.remove(&job_id) {
        Some((_, entry)) => {
            let outcome = match req {
                CompleteRequest::Complete { content_type, body } => WorkerOutcome::Complete {
                    content_type,
                    body: Bytes::from(body.into_bytes()),
                },
                CompleteRequest::Redirect { location, message } => {
                    WorkerOutcome::Redirect { location, message }
                }
                CompleteRequest::Reject { reason } => WorkerOutcome::Reject { reason },
                CompleteRequest::Error { message } => WorkerOutcome::Error { message },
            };
            let _ = entry.result_tx.send(outcome);
            StatusCode::OK
        }
        None => StatusCode::NOT_FOUND,
    }
}

// ─── executor ─────────────────────────────────────────────────────────────────

/// Dispatches jobs to external workers via HTTP long-poll.
///
/// On construction this spawns an axum HTTP server with three endpoints:
///
/// - `GET  /work?timeout_ms=N`   — long-poll; blocks until a job is available,
///   returns job JSON, or 204 on timeout.
/// - `POST /heartbeat/{job_id}`  — worker keepalive; resets the heartbeat timer.
/// - `POST /complete/{job_id}`   — worker posts the result:
///   ```json
///   {"status": "complete", "content_type": "...", "body": "..."}
///   {"status": "redirect", "location": "...", "message": "..."}
///   {"status": "reject",   "reason": "..."}
///   {"status": "error",    "message": "..."}
///   ```
///
/// A background reaper task evicts jobs whose heartbeat has expired, which
/// causes the suspended `execute()` future to return a `ResourceError`.
///
/// This executor must always be paired with a `remote` target action. The
/// `work` future passed to `execute()` is ignored — all work is done remotely.
pub struct RemotePoolExecutor {
    state: Arc<RemotePoolState>,
}

impl RemotePoolExecutor {
    pub fn new(bind: &str, heartbeat_timeout: Duration) -> Self {
        let state = Arc::new(RemotePoolState {
            available: Mutex::new(VecDeque::new()),
            available_notify: Notify::new(),
            in_progress: DashMap::new(),
            heartbeat_timeout,
        });

        // HTTP server task.
        let app = Router::new()
            .route("/work", get(handle_get_work))
            .route("/heartbeat/{job_id}", post(handle_heartbeat))
            .route("/complete/{job_id}", post(handle_complete))
            .with_state(Arc::clone(&state));

        let bind_addr = bind.to_string();
        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(&bind_addr)
                .await
                .unwrap_or_else(|e| panic!("remote_pool: failed to bind to {bind_addr}: {e}"));
            tracing::info!(
                addr = %listener.local_addr().unwrap(),
                "remote_pool HTTP server listening"
            );
            axum::serve(listener, app)
                .await
                .unwrap_or_else(|e| panic!("remote_pool: server error: {e}"));
        });

        // Heartbeat reaper task.
        //
        // When an `InProgressJob` is removed here its `result_tx` is dropped,
        // which causes the waiting `result_rx.await` in `execute()` to return
        // `Err(RecvError)` → `ActionError::ResourceError("worker heartbeat timeout")`.
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

        Self { state }
    }
}

impl<T: Send + 'static> Executor<T> for RemotePoolExecutor {
    fn execute(
        &self,
        job: &Job,
        action_type_id: TypeId,
        work: BoxFuture<'static, Result<T, ActionError>>,
    ) -> BoxFuture<'_, Result<T, ActionError>> {
        use std::any::Any;

        // The action must be RemoteTarget and the result type must be TargetResult.
        // Both are enforced by config validation, but we guard here for safety.
        if action_type_id != TypeId::of::<RemoteTarget>()
            || TypeId::of::<T>() != TypeId::of::<TargetResult>()
        {
            drop(work);
            return Box::pin(async {
                Err(ActionError::ConfigError(
                    "remote_pool executor requires a 'remote' target action".into(),
                ))
            });
        }

        let (result_tx, result_rx) = oneshot::channel::<WorkerOutcome>();

        self.state
            .available
            .lock()
            .unwrap()
            .push_back(AvailableJob {
                job: job.clone(),
                result_tx,
            });
        self.state.available_notify.notify_one();

        Box::pin(async move {
            let outcome = result_rx.await.map_err(|_| {
                ActionError::ResourceError("worker heartbeat timeout or disconnect".into())
            })?;

            let target_result: TargetResult = match outcome {
                WorkerOutcome::Complete { content_type, body } => {
                    let size = body.len() as i64;
                    let stream = Box::new(futures::stream::once(futures::future::ready(Ok::<
                        _,
                        std::io::Error,
                    >(
                        body
                    ))));
                    TargetResult::Complete(JobResult::Success {
                        content_type,
                        size,
                        stream,
                    })
                }
                WorkerOutcome::Redirect { location, message } => {
                    TargetResult::Complete(JobResult::Redirect { location, message })
                }
                WorkerOutcome::Reject { reason } => TargetResult::Reject { reason },
                WorkerOutcome::Error { message } => {
                    return Err(ActionError::ResourceError(message));
                }
            };

            // Safe: TypeId guard above confirmed T == TargetResult.
            let boxed: Box<dyn Any + Send> = Box::new(target_result);
            boxed
                .downcast::<T>()
                .map(|b| *b)
                .map_err(|_| ActionError::ResourceError("remote_pool: type mismatch".into()))
        })
    }
}
