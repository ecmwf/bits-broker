use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use dashmap::DashMap;

use crate::db::PersistenceStore;
use crate::job::Job;

/// Shared shutdown signal that can wake blocked threads immediately.
pub(crate) struct ShutdownSignal {
    stopped: Mutex<bool>,
    condvar: Condvar,
}

impl ShutdownSignal {
    pub(crate) fn new() -> Self {
        Self {
            stopped: Mutex::new(false),
            condvar: Condvar::new(),
        }
    }

    pub(crate) fn stop(&self) {
        let mut guard = self.stopped.lock().unwrap_or_else(|p| p.into_inner());
        *guard = true;
        self.condvar.notify_all();
    }

    pub(crate) fn is_stopped(&self) -> bool {
        *self.stopped.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub(crate) fn wait_timeout(&self, duration: Duration) {
        let guard = self.stopped.lock().unwrap_or_else(|p| p.into_inner());
        if *guard {
            return;
        }
        let _ = self.condvar.wait_timeout(guard, duration);
    }
}

pub(crate) struct ConnectedGuard(Arc<AtomicBool>);

impl ConnectedGuard {
    pub(crate) fn new(flag: Arc<AtomicBool>) -> Self {
        flag.store(true, Ordering::Release);
        Self(flag)
    }
}

impl Drop for ConnectedGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

pub(crate) fn start_sweeper(
    jobs: Arc<DashMap<String, Arc<Job>>>,
    sweep_interval: Duration,
    job_store: Option<Arc<dyn PersistenceStore>>,
    shutdown: Arc<ShutdownSignal>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let runtime = job_store.as_ref().map(|_| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("sweeper tokio runtime")
        });

        loop {
            shutdown.wait_timeout(sweep_interval);

            if shutdown.is_stopped() {
                return;
            }

            let mut expired = Vec::new();

            for entry in jobs.iter() {
                let job = entry.value();
                let has_result = job
                    .result
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .is_some();

                if has_result && !job.client_present() {
                    expired.push(entry.key().clone());
                }
            }

            for id in expired {
                let removed = jobs.remove_if(&id, |_, job| {
                    job.result
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .is_some()
                        && !job.client_present()
                });
                if let Some((_, job)) = removed
                    && job.persisted.load(Ordering::Acquire)
                    && let (Some(store), Some(rt)) = (&job_store, &runtime)
                    && let Err(err) = rt.block_on(store.delete_job(&id))
                {
                    tracing::warn!(job.id = %id, error = %err, "sweeper durable cleanup failed");
                }
            }
        }
    })
}
