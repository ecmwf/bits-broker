pub mod executor;
pub mod queue;

pub use executor::{AsyncPoolExecutor, RemotePoolConfig, RemotePoolExecutor, ThreadPoolExecutor};
pub use queue::{AgePriorityQueue, CostWeightedQueue, FifoQueue, Queue, QueueKind};

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

use crate::actions::{ActionError, TargetResult};
use crate::job::Job;
use crate::metrics;
use crate::worker_server::WorkerServer;

pub const DEFAULT_QUEUE_CAPACITY: usize = 500_000;

/// Selects the executor implementation to construct from config.
///
/// Concurrency is specified per-executor because it only applies to local
/// pool executors (`async_pool`, `thread_pool`).  `remote_pool` delegates to
/// external workers and has no local concurrency knob.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutorKind {
    AsyncPool {
        #[serde(default)]
        concurrency: Option<usize>,
    },
    ThreadPool {
        #[serde(default)]
        concurrency: Option<usize>,
    },
    RemotePool {
        #[serde(flatten)]
        config: RemotePoolConfig,
    },
}

/// A pending item is a work future paired with a reply channel.
///
/// The queue only stores `Job` metadata for ordering. The actual async work
/// and the channel to send its result back live here, keyed by `job.id`.
///
/// The optional semaphore permit is held while the job waits in the queue.
/// When the executor removes this entry, the permit drops and frees the slot.
pub type PendingItem<T> = (
    DispatchGuard,
    BoxFuture<'static, Result<T, ActionError>>,
    oneshot::Sender<Result<T, ActionError>>,
    Option<OwnedSemaphorePermit>,
    Instant, // enqueued_at — for queue wait-time metric
);

/// Maps `job.id` to the pending work and reply channel.
pub type PendingMap<T> = Mutex<HashMap<String, PendingItem<T>>>;

/// An executor controls how queued work is scheduled and run.
///
/// Each executor owns its scheduling loop. The dispatcher calls
/// `start_scheduler` once at construction, passing the queue and pending map.
/// The executor is then responsible for pulling jobs from the queue, resolving
/// the corresponding work future from the pending map, and running it.
pub trait Executor<T: Send + 'static>: Send + Sync {
    fn start_scheduler(
        &self,
        queue: Arc<dyn Queue>,
        pending: Arc<PendingMap<T>>,
    ) -> Result<(), String>;
}

// ================================
//   Per-user admission limit
// ================================

/// Config for a hard per-user cap on jobs admitted to a dispatcher.
///
/// Counts jobs that are queued OR in-flight for this dispatcher (from
/// `dispatch` until the work future resolves or is dropped). When a user is
/// already at `max`, further dispatches for that user are rejected with
/// [`ActionError::UserLimitExceeded`] rather than queued. (More sophisticated
/// per-user fair scheduling / de-weighting is a separate future change to the
/// queue itself; this is a simple hard admission cap.)
///
/// The user identity is derived by reading each JSON pointer in `key` from
/// `job.user` and concatenating the results. A job whose user is missing any
/// key component is treated as unidentifiable and is NOT limited (fail-open),
/// so an anonymous/unkeyed request is never wrongly rejected.
#[derive(Debug, Clone)]
pub struct UserLimitConfig {
    pub max: usize,
    pub key: Vec<String>,
}

/// How long a lazy-mode broker trusts a cached per-dispatcher count before refreshing.
const LAZY_RECONCILE_INTERVAL: Duration = Duration::from_secs(2);
/// Limits at or below this use strict cross-broker enforcement; above it, lazy.
const STRICT_MAX_THRESHOLD: usize = 10;

/// Tracks per-user queued-or-in-flight counts for a single dispatcher.
///
/// The cap is always PER-DISPATCHER (per route target) and per-user — never a
/// global tally across dispatchers. With no store it is broker-local only. With
/// a store it synchronises that per-dispatcher count across the dispatcher's
/// broker replicas: **strict** (`max <= 10`) consults the store on every admit
/// and enforces the cap by cross-replica FIFO rank; **lazy** (`max > 10`) gates
/// on the local count and reconciles a cached per-dispatcher count periodically,
/// tolerating brief over-use.
pub(crate) struct UserLimiter {
    max: usize,
    key: Vec<String>,
    scope: String,
    broker_id: String,
    counts: DashMap<String, usize>,
    store: Option<Arc<dyn crate::db::PersistenceStore>>,
    /// Lazy mode: user -> (last synced per-dispatcher count, when refreshed).
    cached_count: DashMap<String, (usize, Instant)>,
}

impl UserLimiter {
    fn new(
        cfg: UserLimitConfig,
        scope: String,
        broker_id: String,
        store: Option<Arc<dyn crate::db::PersistenceStore>>,
    ) -> Self {
        Self {
            max: cfg.max,
            key: cfg.key,
            scope,
            broker_id,
            counts: DashMap::new(),
            store,
            cached_count: DashMap::new(),
        }
    }

    fn strict(&self) -> bool {
        self.max <= STRICT_MAX_THRESHOLD
    }

    /// Resolve the per-user key from `job.user`, or `None` if any component is
    /// absent/null (unidentifiable user -> not limited).
    fn user_key(&self, user: &serde_json::Value) -> Option<String> {
        let mut parts = Vec::with_capacity(self.key.len());
        for ptr in &self.key {
            match user.pointer(ptr)? {
                serde_json::Value::String(s) => parts.push(s.clone()),
                serde_json::Value::Null => return None,
                other => parts.push(other.to_string()),
            }
        }
        Some(parts.join("\u{1f}"))
    }

    /// Atomically admit one job locally if under the cap. Ok(new count) or
    /// Err(current at-cap count).
    fn try_admit_local(&self, key: &str) -> Result<usize, usize> {
        let mut slot = self.counts.entry(key.to_string()).or_insert(0);
        if *slot >= self.max {
            Err(*slot)
        } else {
            *slot += 1;
            Ok(*slot)
        }
    }

    /// Increment the local count without checking the cap (used when a store is
    /// the authority for admission but we still track a local estimate).
    fn incr_local(&self, key: &str) {
        *self.counts.entry(key.to_string()).or_insert(0) += 1;
    }

    /// Release one local slot for `key`, dropping the entry at zero.
    fn release_local(&self, key: &str) {
        if let Some(mut slot) = self.counts.get_mut(key) {
            *slot = slot.saturating_sub(1);
        }
        self.counts.remove_if(key, |_, v| *v == 0);
    }

    fn guard(self: &Arc<Self>, key: &str, job_id: &str) -> UserLimitGuard {
        UserLimitGuard {
            limiter: Arc::clone(self),
            key: key.to_string(),
            job_id: job_id.to_string(),
        }
    }

    /// Admit one job for `user_key`, returning a guard on success or `None` when
    /// over the cap. Store errors fail OPEN (admit) — availability over strictness.
    async fn admit(self: &Arc<Self>, user_key: &str, job_id: &str) -> Option<UserLimitGuard> {
        let Some(store) = self.store.clone() else {
            // Broker-local only.
            return self
                .try_admit_local(user_key)
                .ok()
                .map(|_| self.guard(user_key, job_id));
        };

        if self.strict() {
            // Strict: the store is authoritative via a cross-replica FIFO rank.
            let reserved = match store
                .reserve_user_slot(&self.scope, user_key, job_id, &self.broker_id)
                .await
            {
                Ok(seq) => Some(seq),
                Err(err) => {
                    tracing::warn!(error = %err, scope = %self.scope, "user-limit reserve failed; failing open");
                    None
                }
            };
            // From here the guard owns both the local count and the store entry,
            // so every exit path (rejection or a dropped future) releases them.
            self.incr_local(user_key);
            let guard = self.guard(user_key, job_id);
            let Some(seq) = reserved else {
                return Some(guard); // fail open: reserve failed
            };
            let entries = match store.list_user_slots(&self.scope, user_key).await {
                Ok(entries) => entries,
                Err(err) => {
                    tracing::warn!(error = %err, scope = %self.scope, "user-limit list failed; failing open");
                    return Some(guard);
                }
            };
            // Rank our entry among this dispatcher's in-flight entries for the
            // user by (seq, job_id): the first `max` survive, the rest back out.
            // Deterministic across the dispatcher's replicas, so exactly `max`
            // are admitted for this (dispatcher, user) — not a global tally.
            let mut ordered: Vec<(u64, &str)> =
                entries.iter().map(|e| (e.seq, e.job_id.as_str())).collect();
            ordered.sort_unstable();
            let rank = ordered
                .iter()
                .position(|(s, jid)| *s == seq && *jid == job_id)
                .unwrap_or(0);
            if rank < self.max {
                Some(guard)
            } else {
                drop(guard); // releases the local count and the store entry
                None
            }
        } else {
            // Lazy: local gate first (local <= synced count, so a full local
            // count means definitely full), then a periodically-refreshed
            // per-dispatcher view from the store.
            if self.try_admit_local(user_key).is_err() {
                return None;
            }
            // Guard owns the local increment + store release from here on.
            let guard = self.guard(user_key, job_id);
            if let Err(err) = store
                .reserve_user_slot(&self.scope, user_key, job_id, &self.broker_id)
                .await
            {
                tracing::warn!(error = %err, scope = %self.scope, "user-limit reserve failed; failing open");
                return Some(guard);
            }
            let fresh = self
                .cached_count
                .get(user_key)
                .map(|v| v.1.elapsed() < LAZY_RECONCILE_INTERVAL);
            let synced = if fresh == Some(true) {
                self.cached_count.get(user_key).map(|v| v.0).unwrap_or(0)
            } else {
                match store.list_user_slots(&self.scope, user_key).await {
                    Ok(entries) => {
                        self.cached_count
                            .insert(user_key.to_string(), (entries.len(), Instant::now()));
                        entries.len()
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, scope = %self.scope, "user-limit list failed; failing open");
                        return Some(guard);
                    }
                }
            };
            if synced > self.max {
                drop(guard); // releases the local count and the store entry
                None
            } else {
                Some(guard)
            }
        }
    }
}

/// RAII guard that releases a user's admission slot when the work future
/// resolves or is dropped (completion, error, cancellation, or dispatcher drain).
///
/// The local slot is released synchronously; the shared-store entry is released
/// on a spawned task (fire-and-forget). The reclaim sweeper is the backstop if
/// that release is lost (e.g. broker crash).
pub(crate) struct UserLimitGuard {
    limiter: Arc<UserLimiter>,
    key: String,
    job_id: String,
}

impl Drop for UserLimitGuard {
    fn drop(&mut self) {
        self.limiter.release_local(&self.key);
        if let Some(store) = &self.limiter.store {
            let store = Arc::clone(store);
            let scope = self.limiter.scope.clone();
            let key = self.key.clone();
            let job_id = self.job_id.clone();
            tokio::spawn(async move {
                if let Err(err) = store.release_user_slot(&scope, &key, &job_id).await {
                    tracing::warn!(error = %err, scope = %scope, "user-limit release failed (reclaim sweeper is the backstop)");
                }
            });
        }
    }
}

// ================================
//   Dispatcher
// ================================

/// Composes a [`Queue`] with an [`Executor`].
///
/// The queue controls *ordering* — which job runs next. The executor
/// controls *scheduling and execution* — it owns the loop that pulls jobs
/// from the queue and decides how to run the associated work.
///
/// When `dispatch` is called the job is enqueued for ordering and the caller
/// suspends. The executor's scheduler is the entity that dequeues jobs,
/// resolves pending work, and sends results back to suspended callers.
pub struct Dispatcher<T: Send + 'static> {
    queue: Arc<dyn Queue>,
    pending: Arc<PendingMap<T>>,
    admission: Arc<Semaphore>,
    closing: Arc<AtomicBool>,
    /// Optional hard per-user admission cap (queued + in-flight).
    user_limiter: Option<Arc<UserLimiter>>,
}

impl<T: Send + 'static> Clone for Dispatcher<T> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            pending: Arc::clone(&self.pending),
            admission: Arc::clone(&self.admission),
            closing: Arc::clone(&self.closing),
            user_limiter: self.user_limiter.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DispatchGuard {
    None,
    Cancelled,
    CancelledOrClientGone,
}

impl<T: Send + 'static> Dispatcher<T> {
    /// Shut this dispatcher down: close the queue (causing executor tasks to
    /// exit), close the admission semaphore (rejecting new dispatches), and
    /// fail any callers still waiting for a queued result.
    pub fn close(&self) {
        self.closing.store(true, Ordering::Release);
        self.admission.close();
        self.queue.close();
        let stranded: Vec<oneshot::Sender<Result<T, ActionError>>> = self
            .pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .drain()
            .map(|(_, (_guard, _work, reply_tx, _permit, _enqueued_at))| reply_tx)
            .collect();
        metrics::record_queue_drained(stranded.len());
        for reply_tx in stranded {
            let _ = reply_tx.send(Err(ActionError::ResourceError(
                "dispatcher closed".to_string(),
            )));
        }
    }

    /// Build a `Dispatcher` from config values, returning `Ok(None)` if neither
    /// queue nor executor is specified (no scheduling needed).
    ///
    /// Returns `Err` when the configuration is internally inconsistent (e.g.
    /// `remote_pool` executor without a `remote` target or missing worker server).
    pub fn from_config(
        queue: Option<&QueueKind>,
        executor: Option<&ExecutorKind>,
        pool_name: Option<&str>,
        worker_server: Option<Arc<WorkerServer>>,
        queue_capacity: usize,
    ) -> Result<Option<Self>, String> {
        if queue.is_none() && executor.is_none() {
            return Ok(None);
        }
        const DEFAULT_POOL_SIZE: usize = 256;
        let executor: Arc<dyn Executor<T>> = match executor {
            None => {
                tracing::debug!(
                    "no executor specified, defaulting to async_pool({})",
                    DEFAULT_POOL_SIZE
                );
                Arc::new(AsyncPoolExecutor::new(DEFAULT_POOL_SIZE))
            }
            Some(ExecutorKind::AsyncPool { concurrency: None }) => {
                Arc::new(AsyncPoolExecutor::new(DEFAULT_POOL_SIZE))
            }
            Some(ExecutorKind::AsyncPool {
                concurrency: Some(n),
            }) => Arc::new(AsyncPoolExecutor::new(*n)),
            Some(ExecutorKind::ThreadPool { concurrency }) => Arc::new(ThreadPoolExecutor::new(
                concurrency.unwrap_or(DEFAULT_POOL_SIZE),
            )),
            Some(ExecutorKind::RemotePool { config: cfg }) => {
                if TypeId::of::<T>() != TypeId::of::<TargetResult>() {
                    return Err(
                        "remote_pool executor can only be used with target actions".to_string()
                    );
                }
                let pool_name = pool_name.ok_or_else(|| {
                    "remote_pool executor requires a named target registry entry (pool name)"
                        .to_string()
                })?;
                let worker_server = worker_server.ok_or_else(|| {
                    "remote_pool executor requires bits.worker_server to be configured".to_string()
                })?;
                let heartbeat_timeout = Duration::try_from_secs_f64(cfg.heartbeat_timeout_secs)
                    .map_err(|e| format!("remote_pool: invalid heartbeat_timeout_secs: {e}"))?;
                if heartbeat_timeout.is_zero() {
                    return Err("remote_pool: heartbeat_timeout_secs must be positive".to_string());
                }
                let callback_url = worker_server.callback_url(pool_name);
                let concrete: Arc<dyn Executor<TargetResult>> = Arc::new(
                    RemotePoolExecutor::new(pool_name, heartbeat_timeout, worker_server)
                        .with_callback_url(callback_url),
                );
                let any: Box<dyn Any> = Box::new(concrete);
                *any.downcast::<Arc<dyn Executor<T>>>().map_err(|_| {
                    "remote_pool: internal type mismatch (T != TargetResult)".to_string()
                })?
            }
        };
        if queue.is_none() {
            tracing::debug!("no queue specified, defaulting to fifo");
        }
        let queue: Arc<dyn Queue> = match queue.unwrap_or(&QueueKind::Fifo) {
            QueueKind::Fifo => Arc::new(FifoQueue::new()),
            QueueKind::CostWeighted => Arc::new(CostWeightedQueue::new()),
            QueueKind::AgePriority => Arc::new(AgePriorityQueue::new()),
        };
        Ok(Some(Self::new(queue, executor, queue_capacity)?))
    }

    pub fn new(
        queue: Arc<dyn Queue>,
        executor: Arc<dyn Executor<T>>,
        queue_capacity: usize,
    ) -> Result<Self, String> {
        if queue_capacity == 0 {
            return Err("queue_capacity must be greater than zero".to_string());
        }
        let pending: Arc<PendingMap<T>> = Arc::new(Mutex::new(HashMap::new()));
        let admission = Arc::new(Semaphore::new(queue_capacity));

        executor.start_scheduler(Arc::clone(&queue), Arc::clone(&pending))?;

        Ok(Self {
            queue,
            pending,
            admission,
            closing: Arc::new(AtomicBool::new(false)),
            user_limiter: None,
        })
    }

    /// Attach (or clear) a per-user admission cap. `scope` is the shared key for
    /// cross-broker accounting (targets cached by name share a scope, hence a
    /// limit); `broker_id` tags this broker's entries; `store`, when present,
    /// enables cross-broker strict/lazy sync. See [`UserLimitConfig`].
    pub fn with_user_limit(
        mut self,
        cfg: Option<UserLimitConfig>,
        scope: &str,
        broker_id: &str,
        store: Option<Arc<dyn crate::db::PersistenceStore>>,
    ) -> Self {
        self.user_limiter = cfg.map(|c| {
            Arc::new(UserLimiter::new(
                c,
                scope.to_string(),
                broker_id.to_string(),
                store,
            ))
        });
        self
    }

    pub fn dispatch(
        &self,
        job: &Job,
        guard: DispatchGuard,
        work: BoxFuture<'static, Result<T, ActionError>>,
    ) -> BoxFuture<'static, Result<T, ActionError>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let pending = Arc::clone(&self.pending);
        let queue = Arc::clone(&self.queue);
        let admission = Arc::clone(&self.admission);
        let closing = Arc::clone(&self.closing);
        let user_limiter = self.user_limiter.clone();
        let user_key = user_limiter.as_ref().and_then(|l| l.user_key(&job.user));
        let job_to_enqueue = job.clone();
        let cancelled = job.cancelled.clone();
        let pollers = job.active_pollers.clone();
        let deadline_nanos = job.reconnect_deadline_nanos.clone();
        Box::pin(async move {
            // Hard per-user admission cap: reject when the user already has
            // `max` jobs queued or in-flight (broker-local, and cross-broker via
            // the store when configured). The guard is moved into the work
            // future below so the slot is held for the whole queued+in-flight
            // span and released on any exit path.
            let user_guard = match (&user_limiter, &user_key) {
                (Some(limiter), Some(key)) => match limiter.admit(key, &job_to_enqueue.id).await {
                    Some(guard) => Some(guard),
                    None => {
                        return Err(ActionError::UserLimitExceeded(format!(
                            "user is at the per-user limit ({}) for this route",
                            limiter.max
                        )));
                    }
                },
                _ => None,
            };

            let permit = match admission.try_acquire_owned() {
                Ok(p) => p,
                Err(tokio::sync::TryAcquireError::NoPermits) => {
                    return Err(ActionError::QueueFull(
                        "dispatcher queue is full".to_string(),
                    ));
                }
                Err(tokio::sync::TryAcquireError::Closed) => {
                    return Err(ActionError::ResourceError("dispatcher closed".to_string()));
                }
            };

            if closing.load(Ordering::Acquire) {
                return Err(ActionError::ResourceError("dispatcher closed".to_string()));
            }

            let guarded_work: BoxFuture<'static, Result<T, ActionError>> = Box::pin(async move {
                // Held for the queued + in-flight span; releases the per-user
                // slot when work completes, errors, is cancelled, or is dropped.
                let _user_guard = user_guard;
                match guard {
                    DispatchGuard::None => {}
                    DispatchGuard::Cancelled => {
                        if cancelled.load(Ordering::Acquire) {
                            return Err(ActionError::Cancelled);
                        }
                    }
                    DispatchGuard::CancelledOrClientGone => {
                        if cancelled.load(Ordering::Acquire) {
                            return Err(ActionError::Cancelled);
                        }
                        if !crate::job::is_client_present(&pollers, &deadline_nanos) {
                            return Err(ActionError::ClientGone);
                        }
                    }
                }

                work.await
            });

            let job_id = job_to_enqueue.id.clone();
            {
                let mut map = pending.lock().unwrap_or_else(|p| p.into_inner());
                if closing.load(Ordering::Acquire) {
                    return Err(ActionError::ResourceError("dispatcher closed".to_string()));
                }
                map.insert(
                    job_id,
                    (guard, guarded_work, reply_tx, Some(permit), Instant::now()),
                );
            }
            metrics::record_queue_enqueued();
            queue.enqueue(job_to_enqueue);

            reply_rx
                .await
                .map_err(|_| ActionError::ResourceError("dispatcher closed".into()))?
        })
    }
}
