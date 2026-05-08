use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::config::{RouteFactory, RuntimeConfig, parse_bootstrap};
use crate::db::{ClaimResult, DbError, PersistenceStore};
use crate::error::{BitsError, ConfigError};
use crate::job::Job;
use crate::result::JobResult;
use crate::route_handle::RouteHandle;
use crate::routing::switch::Switch;
use crate::runtime::maintenance::{ConnectedGuard, ShutdownSignal, start_sweeper};
use crate::runtime::recovery::{LeaseLookup, decode_job_id, slot_from_job_id};
use crate::runtime::runner::spawn_job;

/// Default reconnect buffer added on top of the poll timeout.
const DEFAULT_RECONNECT_BUFFER: Duration = Duration::from_secs(5);
/// Default sweep interval for removing expired completed jobs.
const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(180);

pub const DEFAULT_MAX_JOBS: usize = 500_000;

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

/// Outcome of a job submission attempt.
pub enum SubmitOutcome {
    /// Job was accepted and can be polled.
    Accepted(JobHandle),
    /// Broker is at capacity. The job was not accepted.
    Overloaded,
}

impl SubmitOutcome {
    /// Extracts the handle, panicking if the broker rejected the job.
    /// Intended for tests and contexts where rejection is unexpected.
    pub fn expect_accepted(self, msg: &str) -> JobHandle {
        match self {
            SubmitOutcome::Accepted(h) => h,
            SubmitOutcome::Overloaded => panic!("{msg}"),
        }
    }
}

/// Main broker runtime used to submit, cancel, and poll jobs.
pub struct Bits {
    pub(crate) router: Arc<Switch>,
    #[allow(dead_code)]
    pub(crate) route_factory: RouteFactory,
    pub(crate) jobs: Arc<DashMap<String, Arc<Job>>>,
    pub(crate) broker_id: String,
    pub(crate) site: String,
    pub(crate) env: String,
    pub(crate) broker_slot: u16,
    pub(crate) internal_poll_base_url: String,
    pub(crate) internal_poll_timeout: Duration,
    pub(crate) persist_after: Option<Duration>,
    pub(crate) job_store: Option<Arc<dyn PersistenceStore>>,
    pub(crate) internal_client: reqwest::Client,
    pub(crate) reconnect_buffer: Duration,
    pub(crate) shutdown: Arc<ShutdownSignal>,
    pub(crate) in_flight: Arc<AtomicUsize>,
    pub(crate) added_routes: Arc<std::sync::RwLock<Vec<RouteHandle>>>,
    job_count: Arc<AtomicUsize>,
    max_jobs: usize,
    sweeper_handle: Option<std::thread::JoinHandle<()>>,
    heartbeat_handle: Option<std::thread::JoinHandle<()>>,
    cleanup_tx: Option<std::sync::mpsc::Sender<String>>,
    cleanup_handle: Option<std::thread::JoinHandle<()>>,
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
        let job_count = Arc::new(AtomicUsize::new(0));
        let (site, env, broker_slot) = broker_id
            .rsplit_once('-')
            .and_then(|(prefix, slot)| {
                let (site, env) = prefix.split_once('-')?;
                Some((site.to_string(), env.to_string(), slot.parse::<u16>().ok()?))
            })
            .unwrap_or_else(|| ("tst".to_string(), "tst".to_string(), 0));
        let mut bits = Bits {
            router: Arc::new(router),
            route_factory: RouteFactory::default(),
            jobs: Arc::new(DashMap::new()),
            broker_id,
            site,
            env,
            broker_slot,
            internal_poll_base_url,
            internal_poll_timeout,
            persist_after,
            job_store,
            internal_client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(10))
                .build()
                .expect("failed to build reqwest client"),
            reconnect_buffer: DEFAULT_RECONNECT_BUFFER,
            shutdown: shutdown.clone(),
            in_flight: Arc::new(AtomicUsize::new(0)),
            added_routes: Arc::new(std::sync::RwLock::new(vec![])),
            job_count: job_count.clone(),
            max_jobs: DEFAULT_MAX_JOBS,
            sweeper_handle: None,
            heartbeat_handle: None,
            cleanup_tx: None,
            cleanup_handle: None,
        };
        if let Some(store) = &bits.job_store {
            let (tx, handle) = start_cleanup_worker(Arc::clone(store));
            bits.cleanup_tx = Some(tx);
            bits.cleanup_handle = Some(handle);
        }
        bits.sweeper_handle = Some(start_sweeper(
            bits.jobs.clone(),
            DEFAULT_SWEEP_INTERVAL,
            bits.job_store.clone(),
            shutdown,
            job_count,
        ));
        bits.heartbeat_handle = bits.start_broker_lease_heartbeat(broker_lease_ttl);
        bits
    }

    /// Builds a broker from the YAML configuration format used by the binaries.
    ///
    /// The `server:` section is parsed and used for internal validation but
    /// the resulting [`ServerConfig`](crate::server::ServerConfig) is not
    /// returned. Use [`parse_bootstrap`] + [`Bootstrap::into_parts`] when you
    /// need both.
    pub fn from_config(config: &str) -> Result<Self, BitsError> {
        let bootstrap = parse_bootstrap(config)?;
        if bootstrap.had_server_section {
            tracing::warn!(
                "server: section is parsed but not returned by Bits::from_config(); \
                 use parse_bootstrap().into_parts() to access ServerConfig"
            );
        }
        bootstrap.into_bits()
    }

    pub(crate) fn from_runtime_config(parsed: RuntimeConfig) -> Result<Self, BitsError> {
        let sweep_interval = parsed.sweep_interval.unwrap_or(DEFAULT_SWEEP_INTERVAL);
        let broker_id = format!("{}-{}-{}", parsed.site, parsed.env, parsed.broker_slot);
        let shutdown = Arc::new(ShutdownSignal::new());
        let job_count = Arc::new(AtomicUsize::new(0));
        let mut bits = Bits {
            router: Arc::new(parsed.router),
            route_factory: parsed.route_factory,
            jobs: Arc::new(DashMap::new()),
            broker_id,
            site: parsed.site,
            env: parsed.env,
            broker_slot: parsed.broker_slot,
            internal_poll_base_url: parsed.internal_poll_endpoint,
            internal_poll_timeout: parsed.internal_poll_timeout,
            persist_after: parsed.persist_after,
            job_store: parsed.job_store,
            internal_client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(10))
                .build()
                .map_err(|e| ConfigError::validation("internal_client", e.to_string()))?,
            reconnect_buffer: parsed.reconnect_buffer,
            shutdown: shutdown.clone(),
            in_flight: Arc::new(AtomicUsize::new(0)),
            added_routes: Arc::new(std::sync::RwLock::new(vec![])),
            job_count: job_count.clone(),
            max_jobs: parsed.max_jobs,
            sweeper_handle: None,
            heartbeat_handle: None,
            cleanup_tx: None,
            cleanup_handle: None,
        };

        if let Some(store) = &bits.job_store {
            let (tx, handle) = start_cleanup_worker(Arc::clone(store));
            bits.cleanup_tx = Some(tx);
            bits.cleanup_handle = Some(handle);
        }

        bits.sweeper_handle = Some(start_sweeper(
            bits.jobs.clone(),
            sweep_interval,
            bits.job_store.clone(),
            shutdown,
            job_count,
        ));
        bits.heartbeat_handle = bits.start_broker_lease_heartbeat(parsed.broker_lease_ttl);

        Ok(bits)
    }

    /// Returns the broker instance identifier.
    pub fn broker_id(&self) -> &str {
        &self.broker_id
    }

    /// Returns the validated site tag used for generated broker and request identifiers.
    pub fn site(&self) -> &str {
        &self.site
    }

    /// Returns the validated environment tag used for generated broker and request identifiers.
    pub fn env(&self) -> &str {
        &self.env
    }

    /// Returns the allocated broker slot for this process.
    pub fn broker_slot(&self) -> u16 {
        self.broker_slot
    }

    pub fn route_factory(&self) -> &RouteFactory {
        &self.route_factory
    }

    /// Returns the names of the top-level routes configured on this broker.
    pub fn route_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .router
            .route_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let added = self.added_routes.read().unwrap_or_else(|p| p.into_inner());
        for handle in added.iter() {
            names.extend(handle.router.route_names().iter().map(|s| s.to_string()));
        }
        names
    }

    /// Collects descriptors from every instantiated action in the routing tree.
    pub fn describe_actions(&self) -> Vec<serde_json::Value> {
        let mut out = self.router.describe_actions();
        let added = self.added_routes.read().unwrap_or_else(|p| p.into_inner());
        for handle in added.iter() {
            out.extend(handle.router.describe_actions());
        }
        out
    }

    /// Returns the names of routes added via add_route() — used for collection enumeration.
    pub fn added_route_names(&self) -> Vec<String> {
        self.added_routes
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|h| h.name.clone())
            .collect()
    }

    pub fn start_worker_server(&self) -> Result<(), BitsError> {
        self.route_factory.start_worker_server()
    }

    pub fn add_route(
        &self,
        name: &str,
        route_value: &serde_json::Value,
    ) -> Result<RouteHandle, BitsError> {
        let routes = self.route_factory.parse_route(name, route_value)?;
        let switch = Switch::new(routes);
        switch.validate()?;

        let handle = RouteHandle {
            name: name.to_string(),
            router: Arc::new(switch),
            jobs: self.jobs.clone(),
            broker_id: self.broker_id.clone(),
            site: self.site.clone(),
            env: self.env.clone(),
            broker_slot: self.broker_slot,
            job_store: self.job_store.clone(),
            persist_after: self.persist_after,
            reconnect_buffer: self.reconnect_buffer,
            in_flight: self.in_flight.clone(),
        };

        // Store a clone of the handle for enumeration
        self.added_routes
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .push(handle.clone());

        // Start the worker server if any remote pools were registered by this route.
        // Idempotent — safe to call on every add_route().
        self.route_factory.start_worker_server()?;

        Ok(handle)
    }

    /// Submit a job for processing through the routing pipeline.
    ///
    /// Must be called from within a Tokio runtime (`#[tokio::main]` or `#[tokio::test]`).
    /// Panics if no runtime is available.
    pub fn submit(&self, job: Job) -> SubmitOutcome {
        self.submit_with_state(job, false)
    }

    fn submit_with_state(&self, mut job: Job, already_persisted: bool) -> SubmitOutcome {
        if decode_job_id(&job.id).is_err() {
            job.id = self.new_job_id();
        }
        let job_id = job.id.clone();

        if already_persisted {
            // Recovery submits bypass the limit but still count so that
            // remove_job decrements stay balanced.
            self.job_count.fetch_add(1, Ordering::Relaxed);
        } else {
            // CAS loop: reserve a job slot before inserting into the map.
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
        }

        job.set_reconnect_deadline(Instant::now() + self.reconnect_buffer);

        let job = Arc::new(job);
        self.jobs.insert(job_id.clone(), job.clone());

        spawn_job(
            self.router.clone(),
            job,
            self.job_store.clone(),
            self.persist_after,
            self.broker_id.clone(),
            already_persisted,
            self.in_flight.clone(),
        );

        SubmitOutcome::Accepted(JobHandle { id: job_id })
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

        let Ok(owner) = slot_from_job_id(id) else {
            return PollOutcome::NotFound;
        };

        if owner != self.broker_id {
            match self.lookup_owner_lease(&owner).await {
                LeaseLookup::Active(lease) => {
                    if let Some(outcome) = self.try_proxy_with_lease(&lease, id, timeout).await {
                        return outcome;
                    }
                    return PollOutcome::Pending { id: id.to_string() };
                }
                LeaseLookup::Unknown => return PollOutcome::Pending { id: id.to_string() },
                LeaseLookup::MissingOrExpired => {}
            }
        }

        let Some(store) = &self.job_store else {
            return PollOutcome::NotFound;
        };

        // We only reach claim once the observed owner lease is missing/expired.
        // The claim result then determines whether we recover locally, retry proxying,
        // or surface terminal/liveness outcomes to the caller.
        match self.claim_with_backoff(store, id, &owner, timeout).await {
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
            Ok(ClaimResult::NotFound) if owner == self.broker_id => PollOutcome::NotFound,
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
            Err(err @ DbError::SlotExhausted { .. }) => {
                tracing::error!(job.id = %id, error = %err, "broker slot space exhausted during claim");
                PollOutcome::Pending { id: id.to_string() }
            }
        }
    }

    fn schedule_durable_cleanup(&self, id: &str, job: &crate::job::Job) {
        if job.persisted.load(Ordering::Acquire)
            && let Some(tx) = &self.cleanup_tx
            && tx.send(id.to_string()).is_err()
        {
            tracing::warn!("durable cleanup channel closed");
        }
    }

    async fn poll_local(&self, id: &str, timeout: Option<Duration>) -> Option<PollOutcome> {
        let job = self.jobs.get(id).map(|r| r.clone())?;

        let _guard = ConnectedGuard::new(
            job.active_pollers.clone(),
            job.reconnect_deadline_nanos.clone(),
            self.reconnect_buffer,
        );

        let notified = job.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        // Take the result while holding the result lock, then drop the guard
        // before touching self.jobs. The sweeper's remove_if also locks
        // job.result under the DashMap shard lock; holding result across a
        // DashMap operation here would invert that order and risk deadlock.
        let taken = job.result.lock().unwrap_or_else(|p| p.into_inner()).take();
        if let Some(result) = taken {
            self.remove_job(id);
            self.schedule_durable_cleanup(id, &job);
            return Some(PollOutcome::Ready(result));
        }

        let outcome = match timeout {
            Some(t) => match tokio::time::timeout(t, notified).await {
                Ok(()) => {
                    let taken = job.result.lock().unwrap_or_else(|p| p.into_inner()).take();
                    match taken {
                        Some(result) => {
                            self.remove_job(id);
                            self.schedule_durable_cleanup(id, &job);
                            PollOutcome::Ready(result)
                        }
                        None => PollOutcome::Pending { id: id.to_string() },
                    }
                }
                Err(_) => PollOutcome::Pending { id: id.to_string() },
            },
            None => {
                notified.await;
                let taken = job.result.lock().unwrap_or_else(|p| p.into_inner()).take();
                match taken {
                    Some(result) => {
                        self.remove_job(id);
                        self.schedule_durable_cleanup(id, &job);
                        PollOutcome::Ready(result)
                    }
                    None => PollOutcome::Pending { id: id.to_string() },
                }
            }
        };

        Some(outcome)
    }

    fn remove_job(&self, id: &str) {
        if self.jobs.remove(id).is_some() {
            // Saturating decrement: RouteHandle submits bypass the counter,
            // so their removal must not underflow.
            let _ = self
                .job_count
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1));
        }
    }

    fn new_job_id(&self) -> String {
        crate::polytope_id::encode(&self.site, &self.env, self.broker_slot, chrono::Utc::now())
            .expect("runtime site/env/slot should encode as a request ID")
    }
}

fn start_cleanup_worker(
    store: Arc<dyn PersistenceStore>,
) -> (std::sync::mpsc::Sender<String>, std::thread::JoinHandle<()>) {
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let handle = std::thread::Builder::new()
        .name("bits-cleanup-worker".into())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(err) => {
                    tracing::error!(error = %err, "cleanup worker: failed to build runtime");
                    return;
                }
            };
            while let Ok(job_id) = rx.recv() {
                if let Err(err) = runtime.block_on(store.delete_job(&job_id)) {
                    tracing::warn!(job.id = %job_id, error = %err, "durable record cleanup failed");
                }
            }
        })
        .expect("failed to spawn cleanup worker thread");
    (tx, handle)
}

impl Drop for Bits {
    fn drop(&mut self) {
        // 1. Close all queues/dispatchers so executor tasks exit cleanly.
        self.router.close_all();
        for handle in self
            .added_routes
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
        {
            handle.router.close_all();
        }

        // 2. Stop remote pool reapers and worker server HTTP listener.
        self.route_factory.shutdown_worker_server();

        // 3. Signal sync threads (sweeper, heartbeat) to stop.
        self.shutdown.stop();

        // 4. Close cleanup channel and join cleanup worker.
        self.cleanup_tx.take();
        if let Some(handle) = self.cleanup_handle.take()
            && let Err(panic) = handle.join()
        {
            tracing::error!("cleanup worker panicked during shutdown: {panic:?}");
        }

        // 5. Join sweeper thread.
        if let Some(handle) = self.sweeper_handle.take()
            && let Err(panic) = handle.join()
        {
            tracing::error!("sweeper thread panicked during shutdown: {panic:?}");
        }

        // 6. Join heartbeat thread (includes in-flight drain).
        if let Some(handle) = self.heartbeat_handle.take()
            && let Err(panic) = handle.join()
        {
            tracing::error!("heartbeat thread panicked during shutdown: {panic:?}");
        }
    }
}

#[cfg(test)]
mod route_handle_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::db::{BrokerLeaseStore, PersistenceStore, memory::MemoryStore};
    use crate::job::Job;
    use crate::polytope_id;

    fn bits_for_request_id_tests(
        broker_id: &str,
        job_store: Option<Arc<dyn PersistenceStore>>,
    ) -> Bits {
        Bits::from_router_for_tests(
            Switch::new(vec![]),
            broker_id.to_string(),
            "http://127.0.0.1:1/job".to_string(),
            Duration::from_millis(10),
            None,
            job_store,
            Duration::from_secs(60),
        )
    }

    #[tokio::test]
    async fn request_id_runtime_poll_rejects_legacy_tilde_id_as_not_found() {
        let memory_store = Arc::new(MemoryStore::new());
        memory_store
            .upsert_broker_lease(
                "legacy-owner",
                "http://127.0.0.1:1/job",
                Duration::from_secs(60),
            )
            .await
            .expect("lease should upsert");
        let store: Arc<dyn PersistenceStore> = memory_store;
        let bits = bits_for_request_id_tests("bol-dev-1", Some(store));

        let outcome = bits
            .poll(
                "legacy-owner~550e8400-e29b-41d4-a716-446655440000",
                Some(Duration::from_millis(1)),
            )
            .await;

        assert!(matches!(outcome, PollOutcome::NotFound));
    }

    #[tokio::test]
    async fn request_id_runtime_submit_with_legacy_tilde_id_generates_new_format_id() {
        let bits = bits_for_request_id_tests("bol-dev-42", None);
        let valid_id = polytope_id::encode("bol", "dev", 42, chrono::Utc::now()).unwrap();

        let preserved = bits
            .submit(Job::new_with_id(
                valid_id.clone(),
                serde_json::json!({"case": "valid"}),
            ))
            .expect_accepted("submit should not be rejected");
        let replaced = bits
            .submit(Job::new_with_id(
                "bol-dev-42~550e8400-e29b-41d4-a716-446655440000".to_string(),
                serde_json::json!({"case": "legacy"}),
            ))
            .expect_accepted("submit should not be rejected");

        assert_eq!(preserved.id, valid_id);
        assert_ne!(
            replaced.id,
            "bol-dev-42~550e8400-e29b-41d4-a716-446655440000"
        );
        assert!(!replaced.id.contains('~'));
        assert!(polytope_id::decode(&replaced.id).is_ok());
    }

    #[tokio::test]
    async fn request_id_runtime_route_handle_submit_with_legacy_tilde_id_generates_new_format_id() {
        let config = r#"
bits:
  site: bol
  env: dev
targets:
  my_target:
    type: http
    url: http://127.0.0.1:1
"#;
        let bits = Bits::from_config(config).expect("should build");
        let route_val = serde_json::json!([{"my_route": ["target::my_target"]}]);
        let handle = bits.add_route("my_route", &route_val).expect("add_route");
        let valid_id = polytope_id::encode(
            handle.site(),
            handle.env(),
            handle.broker_slot(),
            chrono::Utc::now(),
        )
        .unwrap();

        let preserved = handle.submit(Job::new_with_id(
            valid_id.clone(),
            serde_json::json!({"case": "valid"}),
        ));
        let replaced = handle.submit(Job::new_with_id(
            format!(
                "{}-{}-{}~550e8400-e29b-41d4-a716-446655440000",
                handle.site(),
                handle.env(),
                handle.broker_slot()
            ),
            serde_json::json!({"case": "legacy"}),
        ));

        assert_eq!(preserved.id, valid_id);
        assert_ne!(
            replaced.id,
            format!(
                "{}-{}-{}~550e8400-e29b-41d4-a716-446655440000",
                handle.site(),
                handle.env(),
                handle.broker_slot()
            )
        );
        assert!(!replaced.id.contains('~'));
        assert!(polytope_id::decode(&replaced.id).is_ok());
    }

    #[tokio::test]
    async fn submit_via_route_handle_pollable_via_bits() {
        let config = r#"
bits:
  site: tst
  env: dev
targets:
  my_target:
    type: http
    url: http://127.0.0.1:1
"#;
        let bits = Bits::from_config(config).expect("should build");
        let route_val = serde_json::json!([{"my_route": ["target::my_target"]}]);
        let handle = bits.add_route("my_route", &route_val).expect("add_route");

        let job = Job::new(serde_json::json!({}));
        let job_handle = handle.submit(job);

        let outcome = bits
            .poll(&job_handle.id, Some(Duration::from_secs(5)))
            .await;
        assert!(matches!(
            outcome,
            crate::PollOutcome::Ready(_) | crate::PollOutcome::Pending { .. }
        ));
    }

    #[tokio::test]
    async fn route_handles_share_jobs_map_pollable_via_bits() {
        let config = r#"
bits:
  site: tst
  env: dev
targets:
  my_target:
    type: http
    url: http://127.0.0.1:1
"#;
        let bits = Bits::from_config(config).expect("should build");
        let route_val_a = serde_json::json!([{"route_a": ["target::my_target"]}]);
        let route_val_b = serde_json::json!([{"route_b": ["target::my_target"]}]);
        let handle_a = bits
            .add_route("route_a", &route_val_a)
            .expect("add_route_a");
        let handle_b = bits
            .add_route("route_b", &route_val_b)
            .expect("add_route_b");

        let job_a = handle_a.submit(Job::new(serde_json::json!({"n": 1})));
        let job_b = handle_b.submit(Job::new(serde_json::json!({"n": 2})));

        let outcome_a = bits.poll(&job_a.id, Some(Duration::from_secs(5))).await;
        let outcome_b = bits.poll(&job_b.id, Some(Duration::from_secs(5))).await;

        assert!(matches!(
            outcome_a,
            crate::PollOutcome::Ready(_) | crate::PollOutcome::Pending { .. }
        ));
        assert!(matches!(
            outcome_b,
            crate::PollOutcome::Ready(_) | crate::PollOutcome::Pending { .. }
        ));
    }
}
