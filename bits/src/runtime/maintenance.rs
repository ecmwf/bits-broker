use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

use crate::db::PersistenceStore;
use crate::job::Job;

pub(crate) struct ConnectedGuard(Arc<AtomicBool>);

impl ConnectedGuard {
    pub(crate) fn new(flag: Arc<AtomicBool>) -> Self {
        flag.store(true, Ordering::Relaxed);
        Self(flag)
    }
}

impl Drop for ConnectedGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

pub(crate) fn start_sweeper(
    jobs: Arc<DashMap<String, Arc<Job>>>,
    sweep_interval: Duration,
    job_store: Option<Arc<dyn PersistenceStore>>,
    stop_flag: Arc<AtomicBool>,
) {
    std::thread::spawn(move || {
        let runtime = job_store.as_ref().map(|_| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("sweeper tokio runtime")
        });

        loop {
            std::thread::sleep(sweep_interval);

            if stop_flag.load(Ordering::Relaxed) {
                return;
            }

            let mut expired = Vec::new();

            for entry in jobs.iter() {
                let job = entry.value();
                let has_result = job.result.lock().unwrap().is_some();

                if has_result && !job.client_present() {
                    expired.push(entry.key().clone());
                }
            }

            for id in expired {
                if let Some((_, job)) = jobs.remove(&id) {
                    if job.persisted.load(Ordering::Relaxed) {
                        if let (Some(store), Some(rt)) = (&job_store, &runtime) {
                            if let Err(err) = rt.block_on(store.delete_job(&id)) {
                                tracing::warn!(job.id = %id, error = %err, "sweeper durable cleanup failed");
                            }
                        }
                    }
                }
            }
        }
    });
}
