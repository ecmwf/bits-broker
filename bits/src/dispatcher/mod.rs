pub mod executor;
pub mod queue;

pub use executor::{AsyncPoolExecutor, RemotePoolConfig, RemotePoolExecutor, ThreadPoolExecutor};
pub use queue::{AgePriorityQueue, CostWeightedQueue, FifoQueue, Queue, QueueKind};

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
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
    Instant, // ponytail: enqueued_at — for queue wait-time metric
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
}

impl<T: Send + 'static> Clone for Dispatcher<T> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            pending: Arc::clone(&self.pending),
            admission: Arc::clone(&self.admission),
            closing: Arc::clone(&self.closing),
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
                let concrete: Arc<dyn Executor<TargetResult>> = Arc::new(RemotePoolExecutor::new(
                    pool_name,
                    heartbeat_timeout,
                    worker_server,
                ));
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
        })
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
        let job_to_enqueue = job.clone();
        let cancelled = job.cancelled.clone();
        let pollers = job.active_pollers.clone();
        let deadline_nanos = job.reconnect_deadline_nanos.clone();
        Box::pin(async move {
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
                map.insert(job_id, (guard, guarded_work, reply_tx, Some(permit), Instant::now()));
            }
            metrics::record_queue_enqueued();
            queue.enqueue(job_to_enqueue);

            reply_rx
                .await
                .map_err(|_| ActionError::ResourceError("dispatcher closed".into()))?
        })
    }
}
