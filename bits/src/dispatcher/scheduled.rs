use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use tokio::sync::oneshot;

use crate::actions::{ActionError, TargetResult};
use crate::dispatcher::Dispatcher;
use crate::job::Job;
use crate::queue::Queue;

type PendingItem = (
    BoxFuture<'static, Result<TargetResult, ActionError>>,
    oneshot::Sender<Result<TargetResult, ActionError>>,
);

type PendingMap = Mutex<HashMap<String, PendingItem>>;

/// Composes a [`Queue`] with an inner [`Dispatcher`].
///
/// The queue controls *ordering* — which job runs next. The inner dispatcher
/// controls *execution* — concurrency limits, thread-pool offload, etc.
///
/// When `dispatch` is called the job is enqueued for ordering and the caller
/// suspends. A background worker is the only entity that calls `dequeue`;
/// once it picks a job it spawns the associated work through the inner
/// dispatcher and sends the result back to the suspended caller.
pub struct ScheduledDispatcher {
    queue: Arc<dyn Queue>,
    #[allow(dead_code)] // held to keep the Arc alive; worker uses inner_ref
    inner: Arc<dyn Dispatcher>,
    pending: Arc<PendingMap>,
}

impl ScheduledDispatcher {
    pub fn new(queue: Arc<dyn Queue>, inner: Arc<dyn Dispatcher>) -> Self {
        let pending: Arc<PendingMap> = Arc::new(Mutex::new(HashMap::new()));

        let queue_ref = Arc::clone(&queue);
        let inner_ref = Arc::clone(&inner);
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

                let inner = Arc::clone(&inner_ref);
                tokio::spawn(async move {
                    let result = inner.dispatch(&job, work).await;
                    let _ = reply_tx.send(result);
                });
            }
        });

        Self { queue, inner, pending }
    }
}

impl Dispatcher for ScheduledDispatcher {
    fn dispatch(
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
                .map_err(|_| ActionError::ResourceError("scheduler closed".into()))?
        })
    }
}
