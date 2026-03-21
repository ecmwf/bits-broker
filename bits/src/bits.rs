use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::config::{RuntimeConfig, parse_bootstrap};
use crate::db::{ClaimResult, DbError, PersistenceStore};
use crate::job::Job;
use crate::result::JobResult;
use crate::routing::switch::Switch;
use crate::runtime::maintenance::{ConnectedGuard, ShutdownSignal, start_sweeper};
use crate::runtime::recovery::{LeaseLookup, owner_from_job_id};
use crate::runtime::runner::spawn_job;

/// Buffer added on top of the poll timeout to allow for the reconnect round-trip.
const RECONNECT_BUFFER: Duration = Duration::from_secs(5);
/// Default sweep interval for removing expired completed jobs.
const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug)]
/// Result of polling a submitted job.
pub enum PollOutcome {
    /// The job reached a terminal state and produced a final result.
    Ready(JobResult),
    /// The job is still in progress; poll again using the returned id.
    Pending { id: String },
    /// No local, remote, or durable record exists for this job id.
    NotFound,
    /// The original owner disappeared and no durable state remained to recover.
    JobLost,
}

/// Handle returned from submission and used for later polling.
pub struct JobHandle {
    /// Stable identifier for the submitted job.
    pub id: String,
}

/// Main broker runtime used to submit, cancel, and poll jobs.
pub struct Bits {
    pub(crate) router: Arc<Switch>,
    pub(crate) jobs: Arc<DashMap<String, Arc<Job>>>,
    pub(crate) broker_id: String,
    pub(crate) internal_poll_base_url: String,
    pub(crate) internal_poll_timeout: Duration,
    pub(crate) persist_after: Option<Duration>,
    pub(crate) job_store: Option<Arc<dyn PersistenceStore>>,
    pub(crate) internal_client: reqwest::Client,
    pub(crate) shutdown: Arc<ShutdownSignal>,
    sweeper_handle: Option<std::thread::JoinHandle<()>>,
    heartbeat_handle: Option<std::thread::JoinHandle<()>>,
}

impl Bits {
    #[doc(hidden)]
    /// Constructs a broker directly from runtime pieces for integration tests.
    pub fn from_router_for_tests(
        router: Switch,
        broker_id: String,
        internal_poll_base_url: String,
        internal_poll_timeout: Duration,
        persist_after: Option<Duration>,
        job_store: Option<Arc<dyn PersistenceStore>>,
        broker_lease_ttl: Duration,
    ) -> Self {
        let shutdown = Arc::new(ShutdownSignal::new());
        let mut bits = Bits {
            router: Arc::new(router),
            jobs: Arc::new(DashMap::new()),
            broker_id,
            internal_poll_base_url,
            internal_poll_timeout,
            persist_after,
            job_store,
            internal_client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("failed to build reqwest client"),
            shutdown: shutdown.clone(),
            sweeper_handle: None,
            heartbeat_handle: None,
        };
        bits.sweeper_handle = Some(start_sweeper(
            bits.jobs.clone(),
            DEFAULT_SWEEP_INTERVAL,
            bits.job_store.clone(),
            shutdown,
        ));
        bits.heartbeat_handle = bits.start_broker_lease_heartbeat(broker_lease_ttl);
        bits
    }

    /// Builds a broker from the YAML configuration format used by the binaries.
    pub fn from_config(config: &str) -> Result<Self, Box<dyn std::error::Error>> {
        parse_bootstrap(config)?.into_bits()
    }

    pub(crate) fn from_runtime_config(
        parsed: RuntimeConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let sweep_interval = parsed.sweep_interval.unwrap_or(DEFAULT_SWEEP_INTERVAL);
        let instance_id = format!("{}-{}", parsed.broker_id, uuid::Uuid::new_v4());
        let shutdown = Arc::new(ShutdownSignal::new());
        let mut bits = Bits {
            router: Arc::new(parsed.router),
            jobs: Arc::new(DashMap::new()),
            broker_id: instance_id,
            internal_poll_base_url: parsed.internal_poll_base_url,
            internal_poll_timeout: parsed.internal_poll_timeout,
            persist_after: parsed.persist_after,
            job_store: parsed.job_store,
            internal_client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            shutdown: shutdown.clone(),
            sweeper_handle: None,
            heartbeat_handle: None,
        };

        bits.sweeper_handle = Some(start_sweeper(
            bits.jobs.clone(),
            sweep_interval,
            bits.job_store.clone(),
            shutdown,
        ));
        bits.heartbeat_handle = bits.start_broker_lease_heartbeat(parsed.broker_lease_ttl);

        Ok(bits)
    }

    /// Returns the broker instance identifier.
    pub fn broker_id(&self) -> &str {
        &self.broker_id
    }

    /// Returns the names of the top-level routes configured on this broker.
    pub fn route_names(&self) -> Vec<&str> {
        self.router.route_names()
    }

    /// Collects descriptors from every instantiated action in the routing tree.
    pub fn describe_actions(&self) -> Vec<serde_json::Value> {
        self.router.describe_actions()
    }

    /// Submits a job for routing and execution.
    pub fn submit(&self, job: Job) -> JobHandle {
        self.submit_with_state(job, false)
    }

    fn submit_with_state(&self, mut job: Job, already_persisted: bool) -> JobHandle {
        if owner_from_job_id(&job.id).is_none() {
            job.id = self.new_job_id();
        }
        let job_id = job.id.clone();

        job.set_reconnect_deadline(Instant::now() + RECONNECT_BUFFER);

        let job = Arc::new(job);
        self.jobs.insert(job_id.clone(), job.clone());

        spawn_job(
            self.router.clone(),
            job,
            self.job_store.clone(),
            self.persist_after,
            self.broker_id.clone(),
            already_persisted,
        );

        JobHandle { id: job_id }
    }

    /// Requests cancellation for a previously submitted job.
    ///
    /// Cancellation is best-effort and is observed at action boundaries.
    pub fn cancel(&self, id: &str) {
        if let Some(job) = self.jobs.get(id) {
            job.cancelled.store(true, Ordering::Release);
        }
    }

    /// Waits for a job result or reports that the caller should retry later.
    ///
    /// When `timeout` is `Some`, the call long-polls for up to that duration.
    pub async fn poll(&self, id: &str, timeout: Option<Duration>) -> PollOutcome {
        if let Some(outcome) = self.poll_local(id, timeout).await {
            return outcome;
        }

        let Some(owner) = owner_from_job_id(id) else {
            return PollOutcome::NotFound;
        };

        if owner == self.broker_id {
            return PollOutcome::NotFound;
        }

        match self.lookup_owner_lease(owner).await {
            LeaseLookup::Active(lease) => {
                if let Some(outcome) = self.try_proxy_with_lease(&lease, id, timeout).await {
                    return outcome;
                }
                return PollOutcome::Pending { id: id.to_string() };
            }
            LeaseLookup::Unknown => return PollOutcome::Pending { id: id.to_string() },
            LeaseLookup::MissingOrExpired => {}
        }

        let Some(store) = &self.job_store else {
            return PollOutcome::NotFound;
        };

        // We only reach claim once the observed owner lease is missing/expired.
        // The claim result then determines whether we recover locally, retry proxying,
        // or surface terminal/liveness outcomes to the caller.
        match self.claim_with_backoff(store, id, owner, timeout).await {
            Ok(ClaimResult::Claimed(record)) => {
                // This broker won ownership and can recover from durable state.
                // Re-submit restored work, then immediately continue as a local poll
                // so this request can long-poll instead of forcing an instant reconnect.
                self.submit_with_state(Job::restore(record), true);
                self.poll_local(id, timeout)
                    .await
                    .unwrap_or(PollOutcome::Pending { id: id.to_string() })
            }
            Ok(ClaimResult::Active { owner_broker_id }) => {
                match self.lookup_owner_lease(&owner_broker_id).await {
                    // Ownership moved concurrently to another live broker.
                    // Proxy to that owner when reachable, otherwise keep client in pending loop.
                    LeaseLookup::Active(lease) => self
                        .try_proxy_with_lease(&lease, id, timeout)
                        .await
                        .unwrap_or(PollOutcome::Pending { id: id.to_string() }),
                    LeaseLookup::MissingOrExpired | LeaseLookup::Unknown => {
                        PollOutcome::Pending { id: id.to_string() }
                    }
                }
            }
            // No durable record exists for this id anymore.
            Ok(ClaimResult::NotFound) => PollOutcome::JobLost,
            Err(DbError::Conflict(message)) => {
                // Rare optimistic-claim race. Keep response in pending loop so the
                // next poll can observe the winning owner.
                tracing::warn!(job.id = %id, error = %message, "claim conflict");
                PollOutcome::Pending { id: id.to_string() }
            }
            Err(DbError::Backend(message)) => {
                // Backend remained unavailable after in-poll backoff retries.
                // Return pending so client retries on the next poll interval.
                tracing::warn!(job.id = %id, error = %message, "claim backend unavailable after retries");
                PollOutcome::Pending { id: id.to_string() }
            }
        }
    }

    fn schedule_durable_cleanup(&self, id: &str, job: &crate::job::Job) {
        if job.persisted.load(Ordering::Acquire)
            && let Some(store) = &self.job_store
        {
            let store = store.clone();
            let job_id = id.to_string();
            tokio::spawn(async move {
                if let Err(err) = store.delete_job(&job_id).await {
                    tracing::warn!(job.id = %job_id, error = %err, "durable record cleanup failed");
                }
            });
        }
    }

    async fn poll_local(&self, id: &str, timeout: Option<Duration>) -> Option<PollOutcome> {
        let job = self.jobs.get(id).map(|r| r.clone())?;

        let _guard = ConnectedGuard::new(job.client_connected.clone());

        let notified = job.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        if let Some(result) = job.result.lock().unwrap_or_else(|p| p.into_inner()).take() {
            self.jobs.remove(id);
            self.schedule_durable_cleanup(id, &job);
            return Some(PollOutcome::Ready(result));
        }

        let outcome = match timeout {
            Some(t) => match tokio::time::timeout(t, notified).await {
                Ok(()) => match job.result.lock().unwrap_or_else(|p| p.into_inner()).take() {
                    Some(result) => {
                        self.jobs.remove(id);
                        self.schedule_durable_cleanup(id, &job);
                        PollOutcome::Ready(result)
                    }
                    None => PollOutcome::Pending { id: id.to_string() },
                },
                Err(_) => PollOutcome::Pending { id: id.to_string() },
            },
            None => {
                notified.await;
                match job.result.lock().unwrap_or_else(|p| p.into_inner()).take() {
                    Some(result) => {
                        self.jobs.remove(id);
                        self.schedule_durable_cleanup(id, &job);
                        PollOutcome::Ready(result)
                    }
                    None => PollOutcome::Pending { id: id.to_string() },
                }
            }
        };

        job.set_reconnect_deadline(Instant::now() + RECONNECT_BUFFER);

        Some(outcome)
    }

    fn new_job_id(&self) -> String {
        format!("{}~{}", self.broker_id, uuid::Uuid::new_v4())
    }
}

impl Drop for Bits {
    fn drop(&mut self) {
        self.shutdown.stop();
        if let Some(handle) = self.sweeper_handle.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.heartbeat_handle.take() {
            let _ = handle.join();
        }
    }
}
