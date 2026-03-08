use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use bits::actions::{ActionError, CheckAction, CheckResult};
use bits::job::Job;
use bits::queue::{FifoQueue, Queue};

/// A check that always passes after sleeping for a fixed duration.
/// Jobs are enqueued into a FIFO queue and dequeued before sleeping,
/// so they are processed in arrival order.
#[derive(Debug, Serialize, Deserialize)]
pub struct CheckDummyDelay {
    pub duration_ms: u64,
    #[serde(skip, default = "CheckDummyDelay::default_queue")]
    pub queue: Arc<FifoQueue>,
}

impl CheckDummyDelay {
    #[allow(dead_code)]
    pub fn new(duration_ms: u64) -> Self {
        Self { duration_ms, queue: Self::default_queue() }
    }

    fn default_queue() -> Arc<FifoQueue> {
        Arc::new(FifoQueue::new())
    }
}

#[async_trait]
impl CheckAction for CheckDummyDelay {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        self.queue.enqueue(job.clone());
        let _ = self.queue.dequeue().await;
        tokio::time::sleep(Duration::from_millis(self.duration_ms)).await;
        Ok(CheckResult::Pass)
    }
}

bits::register_action!(check, "dummy_delay", CheckDummyDelay);
