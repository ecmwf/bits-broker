use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::bits::JobHandle;
use crate::db::PersistenceStore;
use crate::job::Job;
use crate::routing::switch::Switch;
use crate::runtime::recovery::decode_job_id;
use crate::runtime::runner::spawn_job;

#[derive(Clone)]
pub struct RouteHandle {
    pub(crate) name: String,
    pub(crate) router: Arc<Switch>,
    pub(crate) jobs: Arc<DashMap<String, Arc<Job>>>,
    pub(crate) broker_id: String,
    pub(crate) site: String,
    pub(crate) env: String,
    pub(crate) broker_slot: u16,
    pub(crate) job_store: Option<Arc<dyn PersistenceStore>>,
    pub(crate) persist_after: Option<Duration>,
    pub(crate) reconnect_buffer: Duration,
    pub(crate) in_flight: Arc<AtomicUsize>,
}

impl RouteHandle {
    pub fn submit(&self, job: Job) -> JobHandle {
        let mut job = job;
        if decode_job_id(&job.id).is_err() {
            job.id = self.new_job_id();
        }
        let job_id = job.id.clone();
        job.set_reconnect_deadline(Instant::now() + self.reconnect_buffer);
        let job = Arc::new(job);
        self.jobs.insert(job_id.clone(), job.clone());

        spawn_job(
            self.router.clone(),
            job,
            self.job_store.clone(),
            self.persist_after,
            self.broker_id.clone(),
            false,
            self.in_flight.clone(),
        );

        JobHandle { id: job_id }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the validated site tag inherited from the broker runtime.
    pub fn site(&self) -> &str {
        &self.site
    }

    /// Returns the validated environment tag inherited from the broker runtime.
    pub fn env(&self) -> &str {
        &self.env
    }

    /// Returns the allocated broker slot inherited from the broker runtime.
    pub fn broker_slot(&self) -> u16 {
        self.broker_slot
    }

    fn new_job_id(&self) -> String {
        crate::polytope_id::encode(&self.site, &self.env, self.broker_slot, chrono::Utc::now())
            .expect("runtime site/env/slot should encode as a request ID")
    }
}
