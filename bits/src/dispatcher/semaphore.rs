use std::sync::Arc;

use futures::future::BoxFuture;
use tokio::sync::Semaphore;

use crate::actions::{ActionError, TargetResult};
use crate::dispatcher::Executor;
use crate::job::Job;

/// Limits how many work futures execute concurrently.
///
/// Each call to `execute` acquires a permit before polling `work`. Permits
/// are released automatically when `work` completes, so at most `concurrency`
/// futures run at the same time. Callers beyond that limit are suspended until
/// a slot opens.
pub struct SemaphoreExecutor {
    semaphore: Arc<Semaphore>,
}

impl SemaphoreExecutor {
    pub fn new(concurrency: usize) -> Self {
        Self { semaphore: Arc::new(Semaphore::new(concurrency)) }
    }
}

impl Executor for SemaphoreExecutor {
    fn execute(
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
            work.await
        })
    }
}
