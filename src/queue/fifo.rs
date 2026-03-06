use async_trait::async_trait;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use std::sync::Arc;

use crate::actions::ActionError;
use crate::job::Job;
use crate::queue::Queue;

/// A simple FIFO queue backed by a semaphore.
///
/// Callers are admitted in arrival order up to `concurrency` at a time.
#[derive(Debug)]
pub struct FifoQueue {
    semaphore: Arc<Semaphore>,
}

impl FifoQueue {
    pub fn new(concurrency: usize) -> Self {
        Self { semaphore: Arc::new(Semaphore::new(concurrency)) }
    }
}

#[async_trait]
impl Queue for FifoQueue {
    type Permit = OwnedSemaphorePermit;

    async fn acquire(&self, _job: &Job) -> Result<Self::Permit, ActionError> {
        Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .map_err(|_| ActionError::ResourceError("queue closed".into()))
    }
}
