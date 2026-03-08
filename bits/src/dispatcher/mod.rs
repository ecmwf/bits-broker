pub mod external_pool;
pub mod semaphore;
pub mod thread_pool;

pub use external_pool::ExternalPoolExecutor;
pub use semaphore::SemaphoreExecutor;
pub use thread_pool::ThreadPoolExecutor;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use tokio::sync::oneshot;

use crate::actions::{ActionError, TargetResult};
use crate::job::Job;
use crate::queue::{CostWeightedQueue, FifoQueue, Queue, QueueKind};

/// Selects the executor implementation to construct from config.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorKind {
    Semaphore,
    ThreadPool,
}

/// An executor controls how a unit of work is run.
///
/// The executor receives a `work` future and decides how to run it —
/// inline with a semaphore, spawned as an independent task, handed to a
/// remote worker, etc. Ordering is handled separately by the [`Queue`].
///
/// [`Queue`]: crate::queue::Queue
pub trait Executor: Send + Sync {
    fn execute(
        &self,
        job: &Job,
        work: BoxFuture<'static, Result<TargetResult, ActionError>>,
    ) -> BoxFuture<'_, Result<TargetResult, ActionError>>;
}

// ================================
//   Dispatcher
// ================================

type PendingItem = (
    BoxFuture<'static, Result<TargetResult, ActionError>>,
    oneshot::Sender<Result<TargetResult, ActionError>>,
);

type PendingMap = Mutex<HashMap<String, PendingItem>>;

/// Composes a [`Queue`] with an [`Executor`].
///
/// The queue controls *ordering* — which job runs next. The executor
/// controls *execution* — concurrency limits, thread-pool offload, etc.
///
/// When `dispatch` is called the job is enqueued for ordering and the caller
/// suspends. A background worker is the only entity that calls `dequeue`;
/// once it picks a job it runs the associated work through the executor and
/// sends the result back to the suspended caller.
pub struct Dispatcher {
    queue: Arc<dyn Queue>,
    #[allow(dead_code)] // held to keep the Arc alive; worker uses executor_ref
    executor: Arc<dyn Executor>,
    pending: Arc<PendingMap>,
}

impl Dispatcher {
    /// Build a `Dispatcher` from config values, returning `None` if neither
    /// queue nor concurrency is specified (no scheduling needed).
    pub fn from_config(
        queue: Option<&QueueKind>,
        executor: Option<&ExecutorKind>,
        concurrency: Option<usize>,
    ) -> Option<Self> {
        if queue.is_none() && concurrency.is_none() {
            return None;
        }
        let concurrency = concurrency.unwrap_or(tokio::sync::Semaphore::MAX_PERMITS);
        let executor: Arc<dyn Executor> = match executor.unwrap_or(&ExecutorKind::Semaphore) {
            ExecutorKind::Semaphore => Arc::new(SemaphoreExecutor::new(concurrency)),
            ExecutorKind::ThreadPool => Arc::new(ThreadPoolExecutor::new(concurrency)),
        };
        let queue: Arc<dyn Queue> = match queue.unwrap_or(&QueueKind::Fifo) {
            QueueKind::Fifo => Arc::new(FifoQueue::new()),
            QueueKind::CostWeighted => Arc::new(CostWeightedQueue::new()),
        };
        Some(Self::new(queue, executor))
    }

    pub fn new(queue: Arc<dyn Queue>, executor: Arc<dyn Executor>) -> Self {
        let pending: Arc<PendingMap> = Arc::new(Mutex::new(HashMap::new()));

        let queue_ref = Arc::clone(&queue);
        let executor_ref = Arc::clone(&executor);
        let pending_ref = Arc::clone(&pending);

        tokio::spawn(async move {
            while let Some(job) = queue_ref.dequeue().await {
                let item = pending_ref.lock().unwrap().remove(&job.id);
                let Some((work, reply_tx)) = item else {
                    // Caller cancelled before we dequeued — skip.
                    continue;
                };

                if reply_tx.is_closed() {
                    // Caller cancelled after we dequeued — drop the work.
                    continue;
                }

                let executor = Arc::clone(&executor_ref);
                tokio::spawn(async move {
                    let result = executor.execute(&job, work).await;
                    let _ = reply_tx.send(result);
                });
            }
        });

        Self { queue, executor, pending }
    }

    pub fn dispatch(
        &self,
        job: &Job,
        work: BoxFuture<'static, Result<TargetResult, ActionError>>,
    ) -> BoxFuture<'_, Result<TargetResult, ActionError>> {
        let (reply_tx, reply_rx) = oneshot::channel();

        // Insert before enqueue so the worker always finds the entry.
        self.pending.lock().unwrap().insert(job.id.clone(), (work, reply_tx));
        self.queue.enqueue(job.clone());

        Box::pin(async move {
            reply_rx
                .await
                .map_err(|_| ActionError::ResourceError("dispatcher closed".into()))?
        })
    }
}
