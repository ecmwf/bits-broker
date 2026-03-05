use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

use crate::actions::{Action, ActionError, CheckAction, CheckResult, TargetAction, TargetResult, TransformAction, TransformResult};
use crate::job::Job;

struct QueuedJob {
    job: Job,
    response: oneshot::Sender<WorkerResponse>,
}

enum WorkerResponse {
    Check(Result<CheckResult, ActionError>),
    Transform(Job, Result<TransformResult, ActionError>),
    Target(Result<TargetResult, ActionError>),
}

/// Dispatches jobs to a fixed pool of internal worker tasks.
///
/// - `capacity`: size of the bounded channel. Callers block if full (backpressure).
///   Returns `QueueFull` if the channel is full and the caller cannot wait.
/// - `workers`: number of worker tasks spawned. Workers run concurrently and
///   each processes one job at a time.
///
/// Unlike `SemaphoreQueue`, jobs execute on worker tasks rather than the caller's task.
/// Workers are spawned lazily on the first call.
pub struct WorkerQueue {
    capacity: usize,
    workers: usize,
    action: Arc<Action>,
    channel: OnceLock<mpsc::Sender<QueuedJob>>,
}

impl WorkerQueue {
    pub fn new(capacity: usize, workers: usize, action: Action) -> Self {
        Self {
            capacity,
            workers,
            action: Arc::new(action),
            channel: OnceLock::new(),
        }
    }

    async fn send(&self, job: Job) -> Result<WorkerResponse, ActionError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender()
            .send(QueuedJob { job, response: response_tx })
            .await
            .map_err(|_| ActionError::QueueFull("worker queue full".into()))?;
        response_rx.await
            .map_err(|_| ActionError::NetworkError("worker died".into()))
    }

    fn sender(&self) -> &mpsc::Sender<QueuedJob> {
        self.channel.get_or_init(|| {
            let (tx, rx) = mpsc::channel(self.capacity);
            let rx = Arc::new(tokio::sync::Mutex::new(rx));
            for _ in 0..self.workers {
                let action = Arc::clone(&self.action);
                let rx = Arc::clone(&rx);
                tokio::spawn(async move {
                    loop {
                        let queued = { rx.lock().await.recv().await };
                        match queued {
                            Some(QueuedJob { job, response }) => {
                                let _ = response.send(run(&action, job).await);
                            }
                            None => break, // sender dropped, shut down
                        }
                    }
                });
            }
            tx
        })
    }
}

impl std::fmt::Debug for WorkerQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerQueue")
            .field("capacity", &self.capacity)
            .field("workers", &self.workers)
            .finish()
    }
}

async fn run(action: &Action, job: Job) -> WorkerResponse {
    match action {
        Action::Check(check) => WorkerResponse::Check(check.evaluate(&job).await),
        Action::Transform(transform) => {
            let mut job = job;
            let result = transform.execute(&mut job).await;
            WorkerResponse::Transform(job, result)
        }
        Action::Target(target) => WorkerResponse::Target(target.dispatch(&job).await),
        _ => WorkerResponse::Target(Err(ActionError::ConfigError(
            "worker queue can only wrap check, transform, or target actions".into(),
        ))),
    }
}

#[async_trait]
impl CheckAction for WorkerQueue {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        match self.send(job.clone()).await? {
            WorkerResponse::Check(result) => result,
            _ => Err(ActionError::ConfigError("queue type mismatch: expected check".into())),
        }
    }
}

#[async_trait]
impl TransformAction for WorkerQueue {
    async fn execute(&self, job: &mut Job) -> Result<TransformResult, ActionError> {
        match self.send(job.clone()).await? {
            WorkerResponse::Transform(updated, result) => {
                *job = updated;
                result
            }
            _ => Err(ActionError::ConfigError("queue type mismatch: expected transform".into())),
        }
    }
}

#[async_trait]
impl TargetAction for WorkerQueue {
    async fn dispatch(&self, job: &Job) -> Result<TargetResult, ActionError> {
        match self.send(job.clone()).await? {
            WorkerResponse::Target(result) => result,
            _ => Err(ActionError::ConfigError("queue type mismatch: expected target".into())),
        }
    }
}

impl crate::queue::Queue for WorkerQueue {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::time::Duration;
    use serde_json::json;
    use crate::queue::test_helpers::*;

    #[tokio::test]
    async fn test_check_passes_through() {
        let queue = WorkerQueue::new(10, 2, Action::Check(Box::new(PassCheck)));
        let result = queue.evaluate(&test_job()).await.unwrap();
        assert!(matches!(result, CheckResult::Pass));
    }

    #[tokio::test]
    async fn test_transform_writes_back() {
        let queue = WorkerQueue::new(
            10, 1,
            Action::Transform(Box::new(SetMetadata(json!({"transformed": true})))),
        );
        let mut job = test_job();
        queue.execute(&mut job).await.unwrap();
        assert_eq!(job.metadata, json!({"transformed": true}));
    }

    #[tokio::test]
    async fn test_target_result_returned() {
        let queue = WorkerQueue::new(10, 1, Action::Target(Box::new(RejectTarget)));
        let result = queue.dispatch(&test_job()).await.unwrap();
        assert!(matches!(result, TargetResult::Reject { .. }));
    }

    #[tokio::test]
    async fn test_worker_concurrency_limit() {
        // Workers limit how many jobs run at once; excess jobs queue in the channel
        let (slow, max_concurrent) = SlowCheck::new(Duration::from_millis(50));
        let queue = Arc::new(WorkerQueue::new(10, 2, Action::Check(Box::new(slow))));

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

    #[tokio::test]
    async fn test_queued_jobs_complete() {
        // With capacity=2 and a slow worker, callers block until space is available
        // but all jobs should eventually complete.
        let (slow, _) = SlowCheck::new(Duration::from_millis(30));
        let queue = Arc::new(WorkerQueue::new(2, 1, Action::Check(Box::new(slow))));

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let q = Arc::clone(&queue);
                tokio::spawn(async move { q.evaluate(&test_job()).await })
            })
            .collect();

        for h in handles {
            h.await.unwrap().unwrap();
        }
    }
}
