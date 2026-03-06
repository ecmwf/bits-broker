use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use bits::actions::{ActionError, TargetAction, TargetResult};
use bits::job::Job;
use bits::queue::{CostWeightedQueue, Queue};
use bits::result::JobResult;

/// A target that sleeps for a fixed duration, scheduled through a cost-weighted
/// queue. Cheap jobs (low `metadata["cost"]`) are admitted before expensive ones.
/// Useful for testing cost-weighted scheduling behaviour.
#[derive(Debug, Serialize, Deserialize)]
pub struct TargetDummyDelay {
    pub duration_ms: u64,
    pub concurrency: usize,
    #[serde(skip)]
    queue: OnceLock<Arc<CostWeightedQueue>>,
}

impl TargetDummyDelay {
    pub fn new(duration_ms: u64, concurrency: usize) -> Self {
        Self { duration_ms, concurrency, queue: OnceLock::new() }
    }

    fn queue(&self) -> &Arc<CostWeightedQueue> {
        self.queue.get_or_init(|| Arc::new(CostWeightedQueue::new(self.concurrency)))
    }
}

#[async_trait]
impl TargetAction for TargetDummyDelay {
    async fn dispatch(&self, job: &Job) -> Result<TargetResult, ActionError> {
        let _permit = self.queue().acquire(job).await?;
        tokio::time::sleep(Duration::from_millis(self.duration_ms)).await;
        Ok(TargetResult::Complete(JobResult::Redirect {
            location: String::new(),
            message: format!("dummy dispatch complete for job {}", job.id),
        }))
    }
}

bits::register_action!(target, "dummy_dispatch", TargetDummyDelay);
