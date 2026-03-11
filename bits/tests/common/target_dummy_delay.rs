use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use bits::actions::{ActionError, TargetAction, TargetResult};
use bits::dispatcher::queue::{CostWeightedQueue, Queue};
use bits::job::Job;
use bits::result::JobResult;

/// A target that enqueues the job into a cost-weighted queue, dequeues it
/// (yielding to cheaper waiting jobs first), then sleeps for a fixed duration.
/// Useful for testing priority-ordering behaviour.
#[derive(Debug, Serialize, Deserialize)]
pub struct TargetDummyDelay {
    pub duration_ms: u64,
    #[serde(skip)]
    queue: OnceLock<Arc<CostWeightedQueue>>,
}

impl TargetDummyDelay {
    pub fn new(duration_ms: u64) -> Self {
        Self {
            duration_ms,
            queue: OnceLock::new(),
        }
    }

    fn queue(&self) -> &Arc<CostWeightedQueue> {
        self.queue
            .get_or_init(|| Arc::new(CostWeightedQueue::new()))
    }
}

#[async_trait]
impl TargetAction for TargetDummyDelay {
    async fn dispatch(&self, job: &Job) -> Result<TargetResult, ActionError> {
        self.queue().enqueue(job.clone());
        let _ = self.queue().dequeue().await;
        tokio::time::sleep(Duration::from_millis(self.duration_ms)).await;
        Ok(TargetResult::Complete(JobResult::Redirect {
            location: String::new(),
            message: format!("dummy dispatch complete for job {}", job.id),
        }))
    }
}

bits::register_action!(target, "dummy_dispatch", TargetDummyDelay);
