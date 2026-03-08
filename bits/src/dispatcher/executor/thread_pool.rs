use std::any::TypeId;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use tokio::sync::mpsc;

use crate::actions::ActionError;
use crate::dispatcher::Executor;
use crate::job::Job;

type WorkFn = Box<dyn FnOnce() + Send + 'static>;

/// Runs each work future on a pool of dedicated OS threads.
///
/// `concurrency` threads are pre-spawned at construction. Each loops on a
/// shared channel, pulling work items as fast as they arrive. The calling
/// async task suspends until a thread picks up the work and sends back the
/// result via a oneshot.
///
/// Async futures run inside `Handle::block_on` on the OS thread, so all
/// tokio I/O still goes through the runtime — the thread just blocks until
/// the future completes rather than yielding back to the async scheduler.
/// This is appropriate for work that would otherwise starve the runtime or
/// needs guaranteed OS-thread isolation.
pub struct ThreadPoolExecutor {
    tx: mpsc::UnboundedSender<WorkFn>,
}

impl ThreadPoolExecutor {
    pub fn new(concurrency: usize) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<WorkFn>();
        let rx = Arc::new(Mutex::new(rx));

        for _ in 0..concurrency {
            let rx = Arc::clone(&rx);
            std::thread::spawn(move || loop {
                // Lock is held only for the duration of blocking_recv, then
                // released before work() runs so other threads can dequeue.
                let work = match rx.lock().unwrap().blocking_recv() {
                    Some(work) => work,
                    None => break,
                };
                work();
            });
        }

        Self { tx }
    }
}

impl<T: Send + 'static> Executor<T> for ThreadPoolExecutor {
    fn execute(
        &self,
        _job: &Job,
        _action_type_id: TypeId,
        work: BoxFuture<'static, Result<T, ActionError>>,
    ) -> BoxFuture<'static, Result<T, ActionError>> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::runtime::Handle::current();

        let work_fn: WorkFn = Box::new(move || {
            let result = handle.block_on(work);
            let _ = result_tx.send(result);
        });

        let tx = self.tx.clone();
        Box::pin(async move {
            tx.send(work_fn)
                .map_err(|_| ActionError::ResourceError("thread pool closed".into()))?;
            result_rx
                .await
                .map_err(|_| ActionError::ResourceError("worker thread died".into()))?
        })
    }
}
