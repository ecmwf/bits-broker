#![allow(dead_code)]

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use bits::actions::{Action, ActionError, TargetAction, TargetResult};
use bits::db::{memory::MemoryStore, BrokerLeaseStore, ClaimResult, JobStore, PersistenceStore};
use bits::routing::{Route, switch::Switch};
use bits::{Bits, Job, JobResult, PollOutcome, PersistentJobRecord};
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
        vec![Action::Target(Arc::new(SleepTarget { ms: sleep_ms }), None)],
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

#[tokio::test]
async fn threshold_persistence_and_cleanup() {
    let store = Arc::new(MemoryStore::new());
    let bits = test_bits(
        "cleanup-broker",
        120,
        Some(Duration::from_millis(20)),
        Some(Arc::clone(&store)),
    );

    let handle = bits.submit(Job::new(json!({"kind": "cleanup"})));
    wait_for_owner(&store, &handle.id, "cleanup-broker", 300).await;

    tokio::time::sleep(Duration::from_millis(220)).await;
    assert!(observed_owner(&store, &handle.id).await.is_none());
}

#[tokio::test]
async fn fast_jobs_do_not_persist() {
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

    let outcome = claimant.poll(&job_id, Some(Duration::from_millis(40))).await;
    assert!(matches!(outcome, PollOutcome::Pending { .. }));
    assert_eq!(observed_owner(&store, &job_id).await.as_deref(), Some(owner_id));
}

#[tokio::test]
async fn expired_lease_enables_reclaim() {
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
        .upsert_broker_lease(owner_id, "http://127.0.0.1:1/job", Duration::from_millis(20))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let outcome = claimant.poll(&job_id, Some(Duration::from_millis(30))).await;
    assert!(matches!(outcome, PollOutcome::Pending { .. }));
    assert_eq!(observed_owner(&store, &job_id).await.as_deref(), Some(claimant_id));
}

#[tokio::test]
async fn expired_lease_without_record_is_job_lost() {
    let store = Arc::new(MemoryStore::new());
    let owner_id = "owner-missing";
    let claimant = test_bits("claimant-c", 10, None, Some(Arc::clone(&store)));

    store
        .upsert_broker_lease(owner_id, "http://127.0.0.1:1/job", Duration::from_millis(20))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let outcome = claimant.poll(&job_id, Some(Duration::from_millis(30))).await;
    assert!(matches!(outcome, PollOutcome::JobLost));
}

#[tokio::test]
async fn config_rejects_invalid_threshold_ordering() {
    let cfg = r#"
bits:
  poll_timeout_ms: 1000
  persist_after_ms: 900
  persist_guard_ms: 200
routes:
  default: []
"#;
    let err = Bits::from_config(cfg)
        .err()
        .expect("expected invalid config to fail")
        .to_string();
    assert!(err.contains("persist_after_ms + bits.persist_guard_ms"));
}
