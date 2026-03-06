use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use bits::actions::{ActionError, CheckAction, CheckResult};
use bits::job::Job;
use bits::queue::{FifoQueue, Queue};

/// A check that always passes after sleeping for a fixed duration, serialised
/// one-at-a-time through a FIFO queue. Useful for testing queue behaviour.
#[derive(Debug, Serialize, Deserialize)]
pub struct CheckDummyDelay {
    pub duration_ms: u64,
    #[serde(skip, default = "CheckDummyDelay::default_queue")]
    pub queue: Arc<FifoQueue>,
}

impl CheckDummyDelay {
    pub fn new(duration_ms: u64) -> Self {
        Self { duration_ms, queue: Self::default_queue() }
    }

    fn default_queue() -> Arc<FifoQueue> {
        Arc::new(FifoQueue::new(1))
    }
}

#[async_trait]
impl CheckAction for CheckDummyDelay {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        let _permit = self.queue.acquire(job).await?;
        tokio::time::sleep(Duration::from_millis(self.duration_ms)).await;
        Ok(CheckResult::Pass)
    }
}

bits::register_action!(check, "dummy_delay", CheckDummyDelay);
