use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::bits::{JobHandle, SubmitOutcome};
use crate::db::PersistenceStore;
use crate::job::Job;
use crate::routing::switch::Switch;
use crate::runtime::recovery::decode_job_id;
use crate::runtime::runner::spawn_job;

#[derive(Clone, Copy)]
pub(crate) enum SubmissionAdmission {
    EnforceLimit,
    BypassLimitAlreadyPersisted,
}

#[derive(Clone)]
pub(crate) struct SubmitContext {
    pub(crate) jobs: Arc<DashMap<String, Arc<Job>>>,
    pub(crate) job_count: Arc<AtomicUsize>,
    pub(crate) max_jobs: usize,
    pub(crate) broker_id: String,
    pub(crate) site: String,
    pub(crate) env: String,
    pub(crate) broker_slot: u16,
    pub(crate) job_store: Option<Arc<dyn PersistenceStore>>,
    pub(crate) persist_after: Option<Duration>,
    pub(crate) reconnect_buffer: Duration,
    pub(crate) in_flight: Arc<AtomicUsize>,
}

impl SubmitContext {
    pub(crate) fn submit(
        &self,
        router: Arc<Switch>,
        mut job: Job,
        admission: SubmissionAdmission,
    ) -> SubmitOutcome {
        if decode_job_id(&job.id).is_err() {
            job.id = self.new_job_id();
        }
        let job_id = job.id.clone();

        let already_persisted = match admission {
            SubmissionAdmission::EnforceLimit => {
                loop {
                    let current = self.job_count.load(Ordering::Relaxed);
                    if current >= self.max_jobs {
                        tracing::warn!(
                            max_jobs = self.max_jobs,
                            current = current,
                            "broker at capacity, rejecting job"
                        );
                        return SubmitOutcome::Overloaded;
                    }
                    if self
                        .job_count
                        .compare_exchange_weak(
                            current,
                            current + 1,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
                false
            }
            SubmissionAdmission::BypassLimitAlreadyPersisted => {
                self.job_count.fetch_add(1, Ordering::Relaxed);
                true
            }
        };

        job.set_reconnect_deadline(Instant::now() + self.reconnect_buffer);

        let job = Arc::new(job);
        self.jobs.insert(job_id.clone(), job.clone());

        spawn_job(
            router,
            job,
            self.job_store.clone(),
            self.persist_after,
            self.broker_id.clone(),
            already_persisted,
            self.in_flight.clone(),
        );

        SubmitOutcome::Accepted(JobHandle { id: job_id })
    }

    pub(crate) fn new_job_id(&self) -> String {
        crate::request_id::encode(&self.site, &self.env, self.broker_slot, chrono::Utc::now())
            .expect("runtime site/env/slot should encode as a request ID")
    }
}
