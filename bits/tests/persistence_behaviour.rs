#![allow(dead_code)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bits::actions::{Action, ActionError, TargetAction, TargetResult};
use bits::db::{
    BrokerLeaseRecord, BrokerLeaseStore, ClaimResult, DbError, JobStore, PersistenceStore,
    memory::MemoryStore,
};
use bits::routing::{Route, switch::Switch};
use bits::{Bits, Job, JobResult, PersistentJobRecord, PollOutcome};
use serde_json::json;
use tokio::net::TcpListener;

struct SleepTarget {
    ms: u64,
}

#[async_trait]
impl TargetAction for SleepTarget {
    async fn dispatch(&self, _job: &Job) -> Result<TargetResult, ActionError> {
        tokio::time::sleep(Duration::from_millis(self.ms)).await;
        Ok(TargetResult::Complete(JobResult::Error {
            message: "done".into(),
        }))
    }
}

fn test_bits(
    broker_id: &str,
    sleep_ms: u64,
    persist_after: Option<Duration>,
    store: Option<Arc<MemoryStore>>,
) -> Bits {
    let router = Switch::new(vec![Route::new(
        "default".into(),
        vec![Action::Target(
            Arc::new(SleepTarget { ms: sleep_ms }),
            None,
            None,
        )],
    )]);
    Bits::from_router_for_tests(
        router,
        broker_id.to_string(),
        "http://127.0.0.1:9/job".to_string(),
        Duration::from_millis(30),
        persist_after,
        store.map(|s| s as Arc<dyn PersistenceStore>),
        Duration::from_secs(5),
    )
}

async fn observed_owner(store: &MemoryStore, job_id: &str) -> Option<String> {
    match store
        .claim_if_owner(job_id, "__never_expected__", "__inspector__")
        .await
        .unwrap()
    {
        ClaimResult::Active { owner_broker_id } => Some(owner_broker_id),
        ClaimResult::NotFound => None,
        ClaimResult::Claimed(_) => {
            panic!("inspection should never claim when expected owner is impossible")
        }
    }
}

async fn wait_for_owner(store: &MemoryStore, job_id: &str, owner: &str, timeout_ms: u64) {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    while Instant::now() < deadline {
        if observed_owner(store, job_id).await.as_deref() == Some(owner) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "owner '{}' not observed for job '{}' within {}ms",
        owner, job_id, timeout_ms
    );
}

#[derive(Clone)]
struct OwnerState {
    status: StatusCode,
}

async fn owner_poll(Path(_id): Path<String>, State(state): State<OwnerState>) -> Response {
    match state.status {
        StatusCode::SEE_OTHER => (
            StatusCode::SEE_OTHER,
            [(header::LOCATION, "/job/x".to_string())],
        )
            .into_response(),
        status => status.into_response(),
    }
}

async fn start_owner_stub(status: StatusCode) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/job/{id}", get(owner_poll))
        .with_state(OwnerState { status });
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}/job")
}

struct BackendFailingStore {
    attempts: AtomicUsize,
}

struct UpsertFailingStore {
    inner: Arc<MemoryStore>,
}

impl UpsertFailingStore {
    fn new(inner: Arc<MemoryStore>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl JobStore for UpsertFailingStore {
    async fn upsert_job(&self, _record: PersistentJobRecord) -> Result<(), DbError> {
        Err(DbError::Backend("simulated upsert failure".into()))
    }

    async fn delete_job(&self, job_id: &str) -> Result<(), DbError> {
        self.inner.delete_job(job_id).await
    }

    async fn claim_if_owner(
        &self,
        job_id: &str,
        expected_owner_broker_id: &str,
        claimant_broker_id: &str,
    ) -> Result<ClaimResult, DbError> {
        self.inner
            .claim_if_owner(job_id, expected_owner_broker_id, claimant_broker_id)
            .await
    }
}

#[async_trait]
impl BrokerLeaseStore for UpsertFailingStore {
    async fn upsert_broker_lease(
        &self,
        broker_id: &str,
        internal_poll_base_url: &str,
        ttl: Duration,
    ) -> Result<(), DbError> {
        self.inner
            .upsert_broker_lease(broker_id, internal_poll_base_url, ttl)
            .await
    }

    async fn get_broker_lease(
        &self,
        broker_id: &str,
    ) -> Result<Option<BrokerLeaseRecord>, DbError> {
        self.inner.get_broker_lease(broker_id).await
    }

    async fn delete_broker_lease(&self, broker_id: &str) -> Result<(), DbError> {
        self.inner.delete_broker_lease(broker_id).await
    }
}

struct DeleteFailingStore {
    inner: Arc<MemoryStore>,
}

impl DeleteFailingStore {
    fn new(inner: Arc<MemoryStore>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl JobStore for DeleteFailingStore {
    async fn upsert_job(&self, record: PersistentJobRecord) -> Result<(), DbError> {
        self.inner.upsert_job(record).await
    }

    async fn delete_job(&self, _job_id: &str) -> Result<(), DbError> {
        Err(DbError::Backend("simulated delete failure".into()))
    }

    async fn claim_if_owner(
        &self,
        job_id: &str,
        expected_owner_broker_id: &str,
        claimant_broker_id: &str,
    ) -> Result<ClaimResult, DbError> {
        self.inner
            .claim_if_owner(job_id, expected_owner_broker_id, claimant_broker_id)
            .await
    }
}

#[async_trait]
impl BrokerLeaseStore for DeleteFailingStore {
    async fn upsert_broker_lease(
        &self,
        broker_id: &str,
        internal_poll_base_url: &str,
        ttl: Duration,
    ) -> Result<(), DbError> {
        self.inner
            .upsert_broker_lease(broker_id, internal_poll_base_url, ttl)
            .await
    }

    async fn get_broker_lease(
        &self,
        broker_id: &str,
    ) -> Result<Option<BrokerLeaseRecord>, DbError> {
        self.inner.get_broker_lease(broker_id).await
    }

    async fn delete_broker_lease(&self, broker_id: &str) -> Result<(), DbError> {
        self.inner.delete_broker_lease(broker_id).await
    }
}

impl BackendFailingStore {
    fn new() -> Self {
        Self {
            attempts: AtomicUsize::new(0),
        }
    }

    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl JobStore for BackendFailingStore {
    async fn upsert_job(&self, _record: PersistentJobRecord) -> Result<(), DbError> {
        Ok(())
    }

    async fn delete_job(&self, _job_id: &str) -> Result<(), DbError> {
        Ok(())
    }

    async fn claim_if_owner(
        &self,
        _job_id: &str,
        _expected_owner_broker_id: &str,
        _claimant_broker_id: &str,
    ) -> Result<ClaimResult, DbError> {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        Err(DbError::Backend("simulated backend outage".into()))
    }
}

#[async_trait]
impl BrokerLeaseStore for BackendFailingStore {
    async fn upsert_broker_lease(
        &self,
        _broker_id: &str,
        _internal_poll_base_url: &str,
        _ttl: Duration,
    ) -> Result<(), DbError> {
        Ok(())
    }

    async fn get_broker_lease(
        &self,
        _broker_id: &str,
    ) -> Result<Option<BrokerLeaseRecord>, DbError> {
        Ok(None)
    }

    async fn delete_broker_lease(&self, _broker_id: &str) -> Result<(), DbError> {
        Ok(())
    }
}

#[tokio::test]
async fn threshold_persistence_and_cleanup() {
    // Verifies the threshold persistence lifecycle end-to-end:
    // 1) a long-running job crosses `persist_after` and is written to durable store,
    // 2) ownership can be observed in the store while in-flight,
    // 3) durable record is removed after the result is consumed by polling.
    let store = Arc::new(MemoryStore::new());
    let bits = test_bits(
        "cleanup-broker",
        120,
        Some(Duration::from_millis(20)),
        Some(Arc::clone(&store)),
    );

    let handle = bits.submit(Job::new(json!({"kind": "cleanup"})));
    wait_for_owner(&store, &handle.id, "cleanup-broker", 300).await;

    let _ = bits.poll(&handle.id, Some(Duration::from_secs(1))).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(observed_owner(&store, &handle.id).await.is_none());
}

#[tokio::test]
async fn fast_jobs_do_not_persist() {
    // Ensures short-lived jobs stay fully ephemeral:
    // if execution finishes before `persist_after`, no durable job record is created.
    let store = Arc::new(MemoryStore::new());
    let bits = test_bits(
        "fast-broker",
        5,
        Some(Duration::from_millis(80)),
        Some(Arc::clone(&store)),
    );

    let handle = bits.submit(Job::new(json!({"kind": "fast"})));
    let _ = bits.poll(&handle.id, Some(Duration::from_secs(1))).await;

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(observed_owner(&store, &handle.id).await.is_none());
}

#[tokio::test]
async fn active_lease_prevents_reclaim_when_proxy_fails() {
    // Strict reclaim gate test:
    // even if proxying to owner fails (stub returns 500), reclaim must NOT happen
    // while owner lease is still active. Ownership should remain with the original owner.
    let store = Arc::new(MemoryStore::new());
    let owner_id = "owner-live";
    let owner_url = start_owner_stub(StatusCode::INTERNAL_SERVER_ERROR).await;
    let claimant = test_bits("claimant-a", 40, None, Some(Arc::clone(&store)));

    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    store
        .upsert_job(PersistentJobRecord {
            job_id: job_id.clone(),
            broker_id: owner_id.to_string(),
            original_request: json!({"x": 1}),
            user: json!({}),
            metadata: json!({}),
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();
    store
        .upsert_broker_lease(owner_id, &owner_url, Duration::from_secs(2))
        .await
        .unwrap();

    let outcome = claimant
        .poll(&job_id, Some(Duration::from_millis(40)))
        .await;
    assert!(matches!(outcome, PollOutcome::Pending { .. }));
    assert_eq!(
        observed_owner(&store, &job_id).await.as_deref(),
        Some(owner_id)
    );
}

#[tokio::test]
async fn expired_lease_enables_reclaim() {
    // Reclaim happy path:
    // with an expired owner lease, a different broker is allowed to claim durable ownership
    // and continue processing from restored job state.
    let store = Arc::new(MemoryStore::new());
    let owner_id = "owner-expired";
    let claimant_id = "claimant-b";
    let claimant = test_bits(claimant_id, 80, None, Some(Arc::clone(&store)));

    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    store
        .upsert_job(PersistentJobRecord {
            job_id: job_id.clone(),
            broker_id: owner_id.to_string(),
            original_request: json!({"x": 2}),
            user: json!({}),
            metadata: json!({}),
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();
    store
        .upsert_broker_lease(
            owner_id,
            "http://127.0.0.1:1/job",
            Duration::from_millis(20),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let outcome = claimant
        .poll(&job_id, Some(Duration::from_millis(30)))
        .await;
    assert!(matches!(outcome, PollOutcome::Pending { .. }));
    assert_eq!(
        observed_owner(&store, &job_id).await.as_deref(),
        Some(claimant_id)
    );
}

#[tokio::test]
async fn expired_lease_without_record_is_job_lost() {
    // Terminal missing-record behavior:
    // when owner lease is expired and no durable record exists for the job id,
    // poll must return `JobLost` (not perpetual pending).
    let store = Arc::new(MemoryStore::new());
    let owner_id = "owner-missing";
    let claimant = test_bits("claimant-c", 10, None, Some(Arc::clone(&store)));

    store
        .upsert_broker_lease(
            owner_id,
            "http://127.0.0.1:1/job",
            Duration::from_millis(20),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let outcome = claimant
        .poll(&job_id, Some(Duration::from_millis(30)))
        .await;
    assert!(matches!(outcome, PollOutcome::JobLost));
}

#[tokio::test]
async fn config_rejects_invalid_threshold_ordering() {
    // Config safety check:
    // `persist_after + persist_guard` must be strictly less than `poll_timeout`
    // so durable persistence has time to happen before poll timeout behavior.
    let cfg = r#"
bits:
  poll_timeout_ms: 1000
  persist_after_ms: 900
  persist_guard_ms: 200
routes:
  - default: []
"#;
    let err = Bits::from_config(cfg)
        .err()
        .expect("expected invalid config to fail")
        .to_string();
    assert!(err.contains("persist_after_ms + bits.persist_guard_ms"));
}

#[tokio::test]
async fn backend_claim_errors_backoff_within_single_poll() {
    // Backend outage resilience test:
    // claim attempts that return backend errors should back off and retry within
    // the same poll call, rather than hammering storage with immediate retries.
    // We assert both elapsed backoff time and multiple attempts in one poll.
    let store = Arc::new(BackendFailingStore::new());
    let router = Switch::new(vec![Route::new("default".into(), vec![])]);
    let bits = Bits::from_router_for_tests(
        router,
        "claimer-backoff".to_string(),
        "http://127.0.0.1:9/job".to_string(),
        Duration::from_millis(30),
        None,
        Some(store.clone() as Arc<dyn PersistenceStore>),
        Duration::from_secs(5),
    );

    let owner = "expired-owner";
    let job_id = format!("{owner}~{}", uuid::Uuid::new_v4());
    let started = Instant::now();
    let outcome = bits.poll(&job_id, Some(Duration::from_millis(220))).await;
    let elapsed = started.elapsed();

    assert!(matches!(outcome, PollOutcome::Pending { .. }));
    assert!(
        elapsed >= Duration::from_millis(90),
        "expected backoff delay, got {elapsed:?}"
    );
    assert!(
        store.attempts() >= 2,
        "expected multiple claim attempts, got {}",
        store.attempts()
    );
}

#[tokio::test]
async fn upsert_failure_does_not_set_persisted_flag() {
    // A failed durable upsert should keep the job running in memory without creating a durable record.
    let inner = Arc::new(MemoryStore::new());
    let failing = Arc::new(UpsertFailingStore::new(Arc::clone(&inner)));
    let router = Switch::new(vec![Route::new(
        "default".into(),
        vec![Action::Target(
            Arc::new(SleepTarget { ms: 200 }),
            None,
            None,
        )],
    )]);
    let bits = Bits::from_router_for_tests(
        router,
        "upsert-failing".to_string(),
        "http://127.0.0.1:9/job".to_string(),
        Duration::from_millis(30),
        Some(Duration::from_millis(20)),
        Some(failing as Arc<dyn PersistenceStore>),
        Duration::from_secs(5),
    );

    let handle = bits.submit(Job::new(json!({"kind": "upsert-failure"})));
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(matches!(
        bits.poll(&handle.id, Some(Duration::from_millis(20))).await,
        PollOutcome::Pending { .. }
    ));
    assert!(observed_owner(&inner, &handle.id).await.is_none());
}

#[tokio::test]
async fn delete_failure_leaves_record_for_reclaim() {
    // If durable cleanup delete fails after result consumption, the record must remain reclaimable.
    let inner = Arc::new(MemoryStore::new());
    let failing = Arc::new(DeleteFailingStore::new(Arc::clone(&inner)));
    let router = Switch::new(vec![Route::new(
        "default".into(),
        vec![Action::Target(Arc::new(SleepTarget { ms: 80 }), None, None)],
    )]);
    let bits = Bits::from_router_for_tests(
        router,
        "delete-failing".to_string(),
        "http://127.0.0.1:9/job".to_string(),
        Duration::from_millis(30),
        Some(Duration::from_millis(20)),
        Some(failing as Arc<dyn PersistenceStore>),
        Duration::from_secs(5),
    );

    let handle = bits.submit(Job::new(json!({"kind": "delete-failure"})));
    wait_for_owner(&inner, &handle.id, "delete-failing", 300).await;
    tokio::time::sleep(Duration::from_millis(120)).await;
    let _ = bits.poll(&handle.id, Some(Duration::from_secs(1))).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        observed_owner(&inner, &handle.id).await.as_deref(),
        Some("delete-failing")
    );
}

#[cfg(not(feature = "tikv"))]
#[tokio::test]
async fn config_rejects_tiny_broker_lease_ttl() {
    // TiKV lease TTL values below one second must be rejected at config-parse time.
    let cfg = r#"
bits:
  tikv:
    endpoints:
      - 127.0.0.1:2379
    broker_lease_ttl_secs: 0.5
routes:
  - default: []
"#;
    let err = Bits::from_config(cfg)
        .err()
        .expect("expected tiny lease TTL config to fail")
        .to_string();
    assert!(err.contains("broker_lease_ttl_secs"));
}

#[tokio::test]
async fn durable_record_survives_until_poll_consumes_result() {
    // Durable ownership should persist after completion and only be cleaned once polling consumes the result.
    let store = Arc::new(MemoryStore::new());
    let bits = test_bits(
        "poll-consume-cleanup",
        150,
        Some(Duration::from_millis(20)),
        Some(Arc::clone(&store)),
    );

    let handle = bits.submit(Job::new(json!({"kind": "durable-until-consume"})));
    wait_for_owner(&store, &handle.id, "poll-consume-cleanup", 300).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        observed_owner(&store, &handle.id).await.as_deref(),
        Some("poll-consume-cleanup")
    );

    let _ = bits.poll(&handle.id, Some(Duration::from_secs(1))).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(observed_owner(&store, &handle.id).await.is_none());
}
