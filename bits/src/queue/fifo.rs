use async_trait::async_trait;
use tokio::sync::{mpsc, Mutex};

use crate::job::Job;
use crate::queue::Queue;

/// A simple FIFO queue. Items are dequeued in the order they were enqueued.
#[derive(Debug)]
pub struct FifoQueue {
    tx: mpsc::UnboundedSender<Job>,
    rx: Mutex<mpsc::UnboundedReceiver<Job>>,
}

impl FifoQueue {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self { tx, rx: Mutex::new(rx) }
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
        let _ = self.tx.send(job);
    }

    async fn dequeue(&self) -> Option<Job> {
        self.rx.lock().await.recv().await
    }
}
