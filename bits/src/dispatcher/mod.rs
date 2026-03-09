pub mod executor;
pub mod queue;

pub use executor::{RemotePoolExecutor, SemaphoreExecutor, ThreadPoolExecutor};
pub use queue::{CostWeightedQueue, FifoQueue, Queue, QueueKind};

use std::any::TypeId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use futures::future::BoxFuture;
use tokio::sync::oneshot;

use crate::actions::ActionError;
use crate::db::{PersistenceStore, PersistentJobRecord};
use crate::job::Job;

fn default_remote_bind() -> String {
    "0.0.0.0:9001".into()
}

fn default_heartbeat_timeout_secs() -> f64 {
    60.0
}

/// Configuration for the remote-pool executor.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RemotePoolConfig {
    /// Address the long-poll HTTP server binds to.
    #[serde(default = "default_remote_bind")]
    pub bind: String,
    /// Seconds without a heartbeat before an in-progress job is evicted.
    /// Fractional values are supported (e.g. 0.1 for 100 ms).
    #[serde(default = "default_heartbeat_timeout_secs")]
    pub heartbeat_timeout_secs: f64,
}

/// Selects the executor implementation to construct from config.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorKind {
    Semaphore,
    ThreadPool,
    RemotePool(RemotePoolConfig),
}

/// An executor controls how a unit of work is run.
///
/// The executor receives a `work` future and decides how to run it —
/// inline with a semaphore, spawned as an independent task, handed to a
/// remote worker, etc. Ordering is handled separately by the [`Queue`].
///
/// [`Queue`]: crate::queue::Queue
pub trait Executor<T>: Send + Sync {
    fn execute(
        &self,
        job: &Job,
        action_type_id: TypeId,
        work: BoxFuture<'static, Result<T, ActionError>>,
    ) -> BoxFuture<'_, Result<T, ActionError>>;
}

// ================================
//   Dispatcher
// ================================

type PendingItem<T> = (
    BoxFuture<'static, Result<T, ActionError>>,
    oneshot::Sender<Result<T, ActionError>>,
);

type PendingMap<T> = Mutex<HashMap<String, PendingItem<T>>>;

/// Composes a [`Queue`] with an [`Executor`].
///
/// The queue controls *ordering* — which job runs next. The executor
/// controls *execution* — concurrency limits, thread-pool offload, etc.
///
/// When `dispatch` is called the job is enqueued for ordering and the caller
/// suspends. A background worker is the only entity that calls `dequeue`;
/// once it picks a job it runs the associated work through the executor and
/// sends the result back to the suspended caller.
pub struct Dispatcher<T: Send + 'static> {
    queue: Arc<dyn Queue>,
    #[allow(dead_code)] // held to keep the Arc alive; worker uses executor_ref
    executor: Arc<dyn Executor<T>>,
    pending: Arc<PendingMap<T>>,
    #[allow(dead_code)] // captured by value into the worker task at construction
    action_type_id: TypeId,
    job_store: Option<Arc<dyn PersistenceStore>>,
    broker_id: String,
    lock_ttl: Duration,
    persistent: bool,
}

impl<T: Send + 'static> Dispatcher<T> {
    /// Build a `Dispatcher` from config values, returning `None` if neither
    /// queue nor concurrency is specified (no scheduling needed).
    pub fn from_config(
        queue: Option<&QueueKind>,
        executor: Option<&ExecutorKind>,
        concurrency: Option<usize>,
        action_type_id: TypeId,
    ) -> Option<Self> {
        Self::from_config_with_persistence(
            queue,
            executor,
            concurrency,
            action_type_id,
            None,
            "local".to_string(),
            Duration::from_secs(300),
            false,
        )
    }

    pub fn from_config_with_persistence(
        queue: Option<&QueueKind>,
        executor: Option<&ExecutorKind>,
        concurrency: Option<usize>,
        action_type_id: TypeId,
        job_store: Option<Arc<dyn PersistenceStore>>,
        broker_id: String,
        lock_ttl: Duration,
        persistent: bool,
    ) -> Option<Self> {
        if queue.is_none() && concurrency.is_none() && executor.is_none() && !persistent {
            return None;
        }
        let concurrency = concurrency.unwrap_or(tokio::sync::Semaphore::MAX_PERMITS);
        let executor: Arc<dyn Executor<T>> = match executor {
            None | Some(ExecutorKind::Semaphore) => Arc::new(SemaphoreExecutor::new(concurrency)),
            Some(ExecutorKind::ThreadPool) => Arc::new(ThreadPoolExecutor::new(concurrency)),
            Some(ExecutorKind::RemotePool(cfg)) => Arc::new(RemotePoolExecutor::new(
                &cfg.bind,
                Duration::from_secs_f64(cfg.heartbeat_timeout_secs),
            )),
        };
        let queue: Arc<dyn Queue> = match queue.unwrap_or(&QueueKind::Fifo) {
            QueueKind::Fifo => Arc::new(FifoQueue::new()),
            QueueKind::CostWeighted => Arc::new(CostWeightedQueue::new()),
        };
        Some(Self::new(
            queue,
            executor,
            action_type_id,
            job_store,
            broker_id,
            lock_ttl,
            persistent,
        ))
    }

    pub fn new(
        queue: Arc<dyn Queue>,
        executor: Arc<dyn Executor<T>>,
        action_type_id: TypeId,
        job_store: Option<Arc<dyn PersistenceStore>>,
        broker_id: String,
        lock_ttl: Duration,
        persistent: bool,
    ) -> Self {
        let pending: Arc<PendingMap<T>> = Arc::new(Mutex::new(HashMap::new()));

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
                    let result = executor.execute(&job, action_type_id, work).await;
                    let _ = reply_tx.send(result);
                });
            }
        });

        Self {
            queue,
            executor,
            pending,
            action_type_id,
            job_store,
            broker_id,
            lock_ttl,
            persistent,
        }
    }

    pub fn dispatch(
        &self,
        job: &Job,
        work: BoxFuture<'static, Result<T, ActionError>>,
    ) -> BoxFuture<'static, Result<T, ActionError>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let pending = Arc::clone(&self.pending);
        let queue = Arc::clone(&self.queue);
        let job_to_enqueue = job.clone();
        let job_id = job.id.clone();
        let persistent = self.persistent;
        let lock_ttl = self.lock_ttl;
        let broker_id = self.broker_id.clone();
        let job_store = self.job_store.clone();
        Box::pin(async move {
            if persistent {
                let Some(store) = &job_store else {
                    return Err(ActionError::ConfigError(
                        "persistent dispatcher requires configured job store".into(),
                    ));
                };
                let locked_until = Utc::now()
                    + chrono::Duration::from_std(lock_ttl)
                        .unwrap_or_else(|_| chrono::Duration::seconds(60));
                let record = PersistentJobRecord {
                    job_id: job_to_enqueue.id.clone(),
                    broker_id: broker_id.clone(),
                    locked_until,
                    original_request: job_to_enqueue.original_request.clone(),
                    user: job_to_enqueue.user.clone(),
                    metadata: job_to_enqueue.metadata.clone(),
                    created_at: job_to_enqueue.created_at,
                };
                store.upsert_job(record).await.map_err(to_action_error)?;
            }

            // Insert before enqueue so the worker always finds the entry.
            pending.lock().unwrap().insert(job_id.clone(), (work, reply_tx));
            queue.enqueue(job_to_enqueue.clone());

            let _heartbeat = if persistent {
                if let Some(store) = job_store.clone() {
                    Some(AbortOnDrop::spawn(async move {
                        let tick = lock_ttl.div_f64(2.0).max(Duration::from_millis(100));
                        loop {
                            tokio::time::sleep(tick).await;
                            let _ = store.renew_job_lock(&job_id, &broker_id, lock_ttl).await;
                        }
                    }))
                } else {
                    None
                }
            } else {
                None
            };

            let result = reply_rx
                .await
                .map_err(|_| ActionError::ResourceError("dispatcher closed".into()))?;

            if persistent {
                if let Some(store) = &job_store {
                    let _ = store.delete_job(&job_to_enqueue.id).await;
                }
            }

            result
        })
    }
}

fn to_action_error(err: crate::db::DbError) -> ActionError {
    ActionError::ResourceError(format!("persistence error: {err}"))
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl AbortOnDrop {
    fn spawn<F>(future: F) -> Self
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        Self(tokio::spawn(future))
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
