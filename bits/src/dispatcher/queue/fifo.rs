use async_trait::async_trait;
use tokio::sync::{Mutex, mpsc};

use super::Queue;
use crate::job::Job;

/// A simple FIFO queue. Items are dequeued in the order they were enqueued.
pub struct FifoQueue {
    tx: std::sync::Mutex<Option<mpsc::UnboundedSender<Job>>>,
    rx: Mutex<mpsc::UnboundedReceiver<Job>>,
}

impl std::fmt::Debug for FifoQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FifoQueue").finish_non_exhaustive()
    }
}

impl FifoQueue {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            tx: std::sync::Mutex::new(Some(tx)),
            rx: Mutex::new(rx),
        }
    }
}

impl Default for FifoQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Queue for FifoQueue {
    fn enqueue(&self, job: Job) {
        let guard = self.tx.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(tx) = guard.as_ref() {
            if tx.send(job).is_err() {
                tracing::debug!("fifo queue closed; job dropped");
            }
        } else {
            tracing::debug!("fifo queue closed; job dropped");
        }
    }

    async fn dequeue(&self) -> Option<Job> {
        self.rx.lock().await.recv().await
    }

    fn close(&self) {
        self.tx.lock().unwrap_or_else(|p| p.into_inner()).take();
    }
}
