use std::sync::Arc;

use crate::dispatcher::queue::Queue;
use crate::dispatcher::{Executor, PendingMap};

/// Runs each work future on a pool of dedicated OS threads.
///
/// `concurrency` threads are pre-spawned at construction. When
/// `start_scheduler` is called, each thread loops: it calls
/// `queue.dequeue()` (via `block_on`), resolves the pending work future,
/// runs it on the OS thread, and sends the result back.
///
/// Async futures run inside `Handle::block_on` on the OS thread, so all
/// tokio I/O still goes through the runtime — the thread just blocks until
/// the future completes rather than yielding back to the async scheduler.
/// This is appropriate for work that would otherwise starve the runtime or
/// needs guaranteed OS-thread isolation.
pub struct ThreadPoolExecutor {
    concurrency: usize,
}

impl ThreadPoolExecutor {
    pub fn new(concurrency: usize) -> Self {
        Self { concurrency }
    }
}

impl<T: Send + 'static> Executor<T> for ThreadPoolExecutor {
    fn start_scheduler(&self, queue: Arc<dyn Queue>, pending: Arc<PendingMap<T>>) {
        let handle = tokio::runtime::Handle::current();

        for _ in 0..self.concurrency {
            let queue = Arc::clone(&queue);
            let pending = Arc::clone(&pending);
            let handle = handle.clone();

            std::thread::spawn(move || {
                loop {
                    let job = match handle.block_on(queue.dequeue()) {
                        Some(job) => job,
                        None => break, // queue closed
                    };

                    let item = pending.lock().unwrap().remove(&job.id);
                    let Some((_guard, work, reply_tx)) = item else {
                        // Caller cancelled before we dequeued — skip.
                        continue;
                    };

                    if reply_tx.is_closed() {
                        // Caller cancelled after we dequeued — drop the work.
                        continue;
                    }

                    let result = handle.block_on(work);
                    let _ = reply_tx.send(result);
                }
            });
        }
    }
}
