use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use dashmap::DashMap;

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

pub(crate) fn start_sweeper(jobs: Arc<DashMap<String, Arc<Job>>>, sweep_interval: Duration) {
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(sweep_interval);
            let mut expired = Vec::new();

            for entry in jobs.iter() {
                let job = entry.value();
                let has_result = job.result.lock().unwrap().is_some();

                if has_result && !job.client_present() {
                    expired.push(entry.key().clone());
                }
            }

            for id in expired {
                jobs.remove(&id);
            }
        }
    });
}
