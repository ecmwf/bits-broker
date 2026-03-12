pub mod executor;
pub mod queue;

pub use executor::{
    AsyncPoolExecutor, RemotePoolConfig, RemotePoolExecutor, ThreadPoolExecutor,
};
pub use queue::{AgePriorityQueue, CostWeightedQueue, FifoQueue, Queue, QueueKind};

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::oneshot;

use crate::actions::{ActionError, TargetResult};
use crate::job::Job;

/// Selects the executor implementation to construct from config.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorKind {
    AsyncPool,
    ThreadPool,
    RemotePool(RemotePoolConfig),
}

/// A pending item is a work future paired with a reply channel.
///
/// The queue only stores `Job` metadata for ordering. The actual async work
/// and the channel to send its result back live here, keyed by `job.id`.
pub type PendingItem<T> = (
    DispatchGuard,
    BoxFuture<'static, Result<T, ActionError>>,
    oneshot::Sender<Result<T, ActionError>>,
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
    fn start_scheduler(&self, queue: Arc<dyn Queue>, pending: Arc<PendingMap<T>>);
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
}

impl<T: Send + 'static> Clone for Dispatcher<T> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            pending: Arc::clone(&self.pending),
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
    /// Build a `Dispatcher` from config values, returning `None` if neither
    /// queue nor concurrency is specified (no scheduling needed).
    pub fn from_config(
        queue: Option<&QueueKind>,
        executor: Option<&ExecutorKind>,
        concurrency: Option<usize>,
    ) -> Option<Self> {
        if queue.is_none() && concurrency.is_none() && executor.is_none() {
            return None;
        }
        let concurrency = concurrency.unwrap_or(tokio::sync::Semaphore::MAX_PERMITS);
        let executor: Arc<dyn Executor<T>> = match executor {
            None | Some(ExecutorKind::AsyncPool) => Arc::new(AsyncPoolExecutor::new(concurrency)),
            Some(ExecutorKind::ThreadPool) => Arc::new(ThreadPoolExecutor::new(concurrency)),
            Some(ExecutorKind::RemotePool(cfg)) => {
                // RemotePoolExecutor only implements Executor<TargetResult>.
                // Config validation ensures this branch is only reached for
                // target actions, so T == TargetResult. We construct the
                // concrete type and downcast via Any.
                assert_eq!(
                    TypeId::of::<T>(),
                    TypeId::of::<TargetResult>(),
                    "remote_pool executor requires T == TargetResult"
                );
                let concrete: Arc<dyn Executor<TargetResult>> =
                    Arc::new(RemotePoolExecutor::new(
                        &cfg.bind,
                        Duration::from_secs_f64(cfg.heartbeat_timeout_secs),
                    ));
                // Safety: T == TargetResult verified by the assert above.
                // Arc<dyn Executor<TargetResult>> and Arc<dyn Executor<T>>
                // have identical layout when T == TargetResult.
                let any: Box<dyn Any> = Box::new(concrete);
                *any.downcast::<Arc<dyn Executor<T>>>().unwrap_or_else(|_| {
                    panic!("remote_pool: type mismatch (T != TargetResult)")
                })
            }
        };
        let queue: Arc<dyn Queue> = match queue.unwrap_or(&QueueKind::Fifo) {
            QueueKind::Fifo => Arc::new(FifoQueue::new()),
            QueueKind::CostWeighted => Arc::new(CostWeightedQueue::new()),
            QueueKind::AgePriority => Arc::new(AgePriorityQueue::new()),
        };
        Some(Self::new(queue, executor))
    }

    pub fn new(queue: Arc<dyn Queue>, executor: Arc<dyn Executor<T>>) -> Self {
        let pending: Arc<PendingMap<T>> = Arc::new(Mutex::new(HashMap::new()));

        executor.start_scheduler(Arc::clone(&queue), Arc::clone(&pending));

        Self { queue, pending }
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
        let job_to_enqueue = job.clone();
        let job_for_guard = job_to_enqueue.clone();
        Box::pin(async move {
            let guarded_work: BoxFuture<'static, Result<T, ActionError>> = Box::pin(async move {
                match guard {
                    DispatchGuard::None => {}
                    DispatchGuard::Cancelled => {
                        if job_for_guard.is_cancelled() {
                            return Err(ActionError::Cancelled);
                        }
                    }
                    DispatchGuard::CancelledOrClientGone => {
                        if job_for_guard.is_cancelled() {
                            return Err(ActionError::Cancelled);
                        }
                        if !job_for_guard.client_present() {
                            return Err(ActionError::ClientGone);
                        }
                    }
                }

                work.await
            });

            // Insert before enqueue so the scheduler always finds the entry.
            pending
                .lock()
                .unwrap()
                .insert(job_to_enqueue.id.clone(), (guard, guarded_work, reply_tx));
            queue.enqueue(job_to_enqueue.clone());

            reply_rx
                .await
                .map_err(|_| ActionError::ResourceError("dispatcher closed".into()))?
        })
    }
}
