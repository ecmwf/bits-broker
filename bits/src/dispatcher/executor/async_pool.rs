use std::sync::Arc;

use crate::dispatcher::queue::Queue;
use crate::dispatcher::{Executor, PendingMap};

/// Runs work futures on a pool of Tokio tasks.
///
/// When `start_scheduler` is called, `concurrency` Tokio tasks are spawned.
/// Each task loops: dequeue a job, resolve the pending work future, run it
/// inline, send the result back, then dequeue the next. Concurrency is
/// controlled by the number of tasks.
pub struct AsyncPoolExecutor {
    concurrency: usize,
}

impl AsyncPoolExecutor {
    pub fn new(concurrency: usize) -> Self {
        Self { concurrency }
    }
}

impl<T: Send + 'static> Executor<T> for AsyncPoolExecutor {
    fn start_scheduler(
        &self,
        queue: Arc<dyn Queue>,
        pending: Arc<PendingMap<T>>,
    ) -> Result<(), String> {
        for _ in 0..self.concurrency {
            let queue = Arc::clone(&queue);
            let pending = Arc::clone(&pending);

            tokio::spawn(async move {
                while let Some(job) = queue.dequeue().await {
                    let item = pending
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .remove(&job.id);
                    let Some((_guard, work, reply_tx, permit)) = item else {
                        continue;
                    };
                    drop(permit);

                    if reply_tx.is_closed() {
                        continue;
                    }

                    let result = work.await;
                    if reply_tx.send(result).is_err() {
                        tracing::debug!("async_pool: caller dropped before result delivery");
                    }
                }
            });
        }
        Ok(())
    }
}
