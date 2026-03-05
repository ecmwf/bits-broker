use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Semaphore;

use crate::actions::{Action, ActionError, CheckAction, CheckResult, TargetAction, TargetResult, TransformAction, TransformResult};
use crate::job::Job;

/// Limits concurrent execution using semaphores.
///
/// - `capacity`: maximum total jobs in the system (waiting + executing).
///   Arrivals beyond this are rejected immediately with `QueueFull`.
/// - `concurrency`: maximum jobs executing at once.
///   Jobs beyond this block on the caller's task until a slot is free.
///
/// No internal threads are spawned — jobs execute inline on the caller's task.
pub struct SemaphoreQueue {
    capacity: Semaphore,
    concurrency: Semaphore,
    action: Arc<Action>,
}

impl SemaphoreQueue {
    pub fn new(capacity: usize, concurrency: usize, action: Action) -> Self {
        Self {
            capacity: Semaphore::new(capacity),
            concurrency: Semaphore::new(concurrency),
            action: Arc::new(action),
        }
    }

    async fn acquire(&self) -> Result<(tokio::sync::SemaphorePermit<'_>, tokio::sync::SemaphorePermit<'_>), ActionError> {

        // Reject immediately if at capacity
        let cap = self.capacity.try_acquire()
            .map_err(|_| ActionError::QueueFull("semaphore queue at capacity".into()))?;

        // Wait for concurrency slot
        let conc = self.concurrency.acquire().await
            .map_err(|_| ActionError::QueueFull("semaphore closed".into()))?;
        
        Ok((cap, conc))
    }
}

impl std::fmt::Debug for SemaphoreQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SemaphoreQueue")
            .field("capacity_available", &self.capacity.available_permits())
            .field("concurrency_available", &self.concurrency.available_permits())
            .finish()
    }
}

#[async_trait]
impl CheckAction for SemaphoreQueue {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        let _permits = self.acquire().await?;
        match self.action.as_ref() {
            Action::Check(check) => check.evaluate(job).await,
            _ => Err(ActionError::ConfigError("queue type mismatch: expected check".into())),
        }
    }
}

#[async_trait]
impl TransformAction for SemaphoreQueue {
    async fn execute(&self, job: &mut Job) -> Result<TransformResult, ActionError> {
        let _permits = self.acquire().await?;
        match self.action.as_ref() {
            Action::Transform(transform) => transform.execute(job).await,
            _ => Err(ActionError::ConfigError("queue type mismatch: expected transform".into())),
        }
    }
}

#[async_trait]
impl TargetAction for SemaphoreQueue {
    async fn dispatch(&self, job: &Job) -> Result<TargetResult, ActionError> {
        let _permits = self.acquire().await?;
        match self.action.as_ref() {
            Action::Target(target) => target.dispatch(job).await,
            _ => Err(ActionError::ConfigError("queue type mismatch: expected target".into())),
        }
    }
}

impl crate::queue::Queue for SemaphoreQueue {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::time::Duration;
    use serde_json::json;
    use crate::queue::test_helpers::*;

    #[tokio::test]
    async fn test_check_passes_through() {
        let queue = SemaphoreQueue::new(10, 4, Action::Check(Box::new(PassCheck)));
        let result = queue.evaluate(&test_job()).await.unwrap();
        assert!(matches!(result, CheckResult::Pass));
    }

    #[tokio::test]
    async fn test_transform_writes_back() {
        let queue = SemaphoreQueue::new(
            10, 1,
            Action::Transform(Box::new(SetMetadata(json!({"transformed": true})))),
        );
        let mut job = test_job();
        queue.execute(&mut job).await.unwrap();
        assert_eq!(job.metadata, json!({"transformed": true}));
    }

    #[tokio::test]
    async fn test_target_result_returned() {
        let queue = SemaphoreQueue::new(10, 1, Action::Target(Box::new(RejectTarget)));
        let result = queue.dispatch(&test_job()).await.unwrap();
        assert!(matches!(result, TargetResult::Reject { .. }));
    }

    #[tokio::test]
    async fn test_capacity_rejection() {
        // capacity=1: only one job allowed in the system at a time
        let (slow, _) = SlowCheck::new(Duration::from_millis(100));
        let queue = Arc::new(SemaphoreQueue::new(1, 1, Action::Check(Box::new(slow))));

        let q = Arc::clone(&queue);
        let handle = tokio::spawn(async move { q.evaluate(&test_job()).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let result = queue.evaluate(&test_job()).await;
        assert!(matches!(result, Err(ActionError::QueueFull(_))));

        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_concurrency_limit() {
        let (slow, max_concurrent) = SlowCheck::new(Duration::from_millis(50));
        let queue = Arc::new(SemaphoreQueue::new(10, 2, Action::Check(Box::new(slow))));

        let handles: Vec<_> = (0..6)
            .map(|_| {
                let q = Arc::clone(&queue);
                tokio::spawn(async move { q.evaluate(&test_job()).await })
            })
            .collect();

        for h in handles {
            h.await.unwrap().unwrap();
        }

        assert!(max_concurrent.load(std::sync::atomic::Ordering::SeqCst) <= 2);
    }
}
