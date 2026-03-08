use std::sync::Arc;

use futures::future::BoxFuture;
use tokio::sync::Semaphore;

use crate::actions::{ActionError, TargetResult};
use crate::dispatcher::Dispatcher;
use crate::job::Job;

/// Runs each work future as an independent tokio task, bounded by a semaphore.
///
/// Unlike `SemaphoreDispatcher`, which runs `work` inline inside the calling
/// task, `ThreadPoolDispatcher` spawns each `work` future with `tokio::spawn`.
/// Tokio's work-stealing scheduler then assigns it to whichever thread is
/// available, keeping CPU-heavy or long-running work off the caller's thread.
///
/// At most `concurrency` tasks are active simultaneously; additional callers
/// wait for a slot before spawning.
pub struct ThreadPoolDispatcher {
    semaphore: Arc<Semaphore>,
}

impl ThreadPoolDispatcher {
    pub fn new(concurrency: usize) -> Self {
        Self { semaphore: Arc::new(Semaphore::new(concurrency)) }
    }
}

impl Dispatcher for ThreadPoolDispatcher {
    fn dispatch(
        &self,
        _job: &Job,
        work: BoxFuture<'static, Result<TargetResult, ActionError>>,
    ) -> BoxFuture<'_, Result<TargetResult, ActionError>> {
        let semaphore = Arc::clone(&self.semaphore);
        Box::pin(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .map_err(|_| ActionError::ResourceError("semaphore closed".into()))?;
            tokio::spawn(work)
                .await
                .map_err(|_| ActionError::ResourceError("dispatched task panicked".into()))?
        })
    }
}
