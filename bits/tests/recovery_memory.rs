mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bits::db::{PersistenceStore, memory::MemoryStore};
use bits::server::CODE_JOB_ERROR;
use bits::{Job, JobResult, PollOutcome};
use common::recovery::{
    BackendFailingStore, LeaseLookupFailingStore, TargetBehavior, insert_job_record,
    observed_owner, poll_until_terminal, read_success_body, single_target_switch,
    start_broker_server, start_error_owner_stub, start_gone_owner_stub, start_not_found_owner_stub,
    start_pending_owner_stub, start_redirect_owner_stub, start_server_error_owner_stub,
    start_success_owner_stub, wait_for_no_owner, wait_for_owner, wait_for_ready,
};
use serde_json::json;
use tokio::net::TcpListener;

async fn status_owner_poll(Path(_id): Path<String>, status: StatusCode) -> Response {
    status.into_response()
}

async fn start_status_owner_stub(status: StatusCode) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new().route(
        "/job/{id}",
        get(move |path| async move { status_owner_poll(path, status).await }),
    );
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}/job")
}

async fn shared_store() -> Arc<dyn PersistenceStore> {
    Arc::new(MemoryStore::new()) as Arc<dyn PersistenceStore>
}

#[tokio::test]
async fn threshold_persistence_and_cleanup() {
    // Long-running jobs should cross the persistence threshold, become durable
    // while in flight, then remove their durable record after completion.
    let store = shared_store().await;
    let broker = start_broker_server(
        "cleanup-broker",
        single_target_switch(TargetBehavior::Success {
            delay: Duration::from_millis(120),
            content_type: "application/json".into(),
            body: br#"{"ok":true}"#.to_vec(),
        }),
        Some(Duration::from_millis(20)),
        Some(Arc::clone(&store)),
        Duration::from_secs(1),
        Duration::from_secs(5),
    )
    .await;

    let handle = broker
        .bits
        .submit(Job::new(json!({"kind": "cleanup"})))
        .expect_accepted("submit should not be rejected");
    wait_for_owner(
        &store,
        &handle.id,
        "cleanup-broker",
        Duration::from_millis(300),
    )
    .await;
    let result = wait_for_ready(&broker.bits, &handle.id, Duration::from_secs(1)).await;
    let (_content_type, body) = read_success_body(result).await;
    assert_eq!(String::from_utf8(body).unwrap(), r#"{"ok":true}"#);
    wait_for_no_owner(&store, &handle.id, Duration::from_millis(300)).await;
}

#[tokio::test]
async fn fast_jobs_do_not_persist() {
    // Jobs that finish before `persist_after` should stay ephemeral and never
    // create a durable ownership record.
    let store = shared_store().await;
    let broker = start_broker_server(
        "fast-broker",
        single_target_switch(TargetBehavior::Success {
            delay: Duration::from_millis(5),
            content_type: "text/plain".into(),
            body: b"fast".to_vec(),
        }),
        Some(Duration::from_millis(80)),
        Some(Arc::clone(&store)),
        Duration::from_secs(1),
        Duration::from_secs(5),
    )
    .await;

    let handle = broker
        .bits
        .submit(Job::new(json!({"kind": "fast"})))
        .expect_accepted("submit should not be rejected");
    let result = wait_for_ready(&broker.bits, &handle.id, Duration::from_secs(1)).await;
    let (_content_type, body) = read_success_body(result).await;
    assert_eq!(body, b"fast");
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(observed_owner(&store, &handle.id).await.is_none());
}

#[tokio::test]
async fn active_lease_proxies_success_result() {
    // When the original owner is still live, another broker should proxy the
    // terminal success result instead of reclaiming the job.
    let store = shared_store().await;
    let owner_id = "owner-success";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let owner_url = start_success_owner_stub("text/plain", b"proxied-body".to_vec()).await;
    insert_job_record(&store, &job_id, owner_id, json!({"job": "success"})).await;
    store
        .upsert_broker_lease(owner_id, &owner_url, Duration::from_secs(2))
        .await
        .unwrap();

    let claimant = start_broker_server(
        "claimant-success",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(200),
        Duration::from_secs(5),
    )
    .await;

    let result = wait_for_ready(&claimant.bits, &job_id, Duration::from_secs(1)).await;
    let (content_type, body) = read_success_body(result).await;
    assert_eq!(content_type, "text/plain");
    assert_eq!(body, b"proxied-body");
    assert_eq!(
        observed_owner(&store, &job_id).await.as_deref(),
        Some(owner_id)
    );
}

#[tokio::test]
async fn active_lease_proxies_redirect_result() {
    // Live-owner polling should preserve redirect semantics across brokers.
    let store = shared_store().await;
    let owner_id = "owner-redirect";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let owner_url = start_redirect_owner_stub("https://example.test/final").await;
    insert_job_record(&store, &job_id, owner_id, json!({"job": "redirect"})).await;
    store
        .upsert_broker_lease(owner_id, &owner_url, Duration::from_secs(2))
        .await
        .unwrap();

    let claimant = start_broker_server(
        "claimant-redirect",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(200),
        Duration::from_secs(5),
    )
    .await;

    match wait_for_ready(&claimant.bits, &job_id, Duration::from_secs(1)).await {
        JobResult::Redirect { location, .. } => {
            assert_eq!(location, "https://example.test/final");
        }
        other => panic!("expected redirect result, got {other:?}"),
    }
}

#[tokio::test]
async fn active_lease_proxies_error_result() {
    // Live-owner polling should surface job-level errors through the proxy path.
    let store = shared_store().await;
    let owner_id = "owner-error";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let owner_url = start_error_owner_stub("bad request").await;
    insert_job_record(&store, &job_id, owner_id, json!({"job": "error"})).await;
    store
        .upsert_broker_lease(owner_id, &owner_url, Duration::from_secs(2))
        .await
        .unwrap();

    let claimant = start_broker_server(
        "claimant-error",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(200),
        Duration::from_secs(5),
    )
    .await;

    match wait_for_ready(&claimant.bits, &job_id, Duration::from_secs(1)).await {
        JobResult::Error { message } => assert_eq!(
            message,
            json!({"code": CODE_JOB_ERROR, "message": "bad request", "retryable": false})
                .to_string()
        ),
        other => panic!("expected error result, got {other:?}"),
    }
}

#[tokio::test]
async fn active_lease_proxies_gone_as_cancelled() {
    // A proxied 410/Gone from the live owner should map to a cancelled result.
    let store = shared_store().await;
    let owner_id = "owner-gone";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let owner_url = start_gone_owner_stub().await;
    insert_job_record(&store, &job_id, owner_id, json!({"job": "gone"})).await;
    store
        .upsert_broker_lease(owner_id, &owner_url, Duration::from_secs(2))
        .await
        .unwrap();

    let claimant = start_broker_server(
        "claimant-gone",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(200),
        Duration::from_secs(5),
    )
    .await;

    assert!(matches!(
        wait_for_ready(&claimant.bits, &job_id, Duration::from_secs(1)).await,
        JobResult::Cancelled
    ));
}

#[tokio::test]
async fn active_lease_proxies_not_found() {
    let store = shared_store().await;
    let owner_id = "owner-not-found";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let owner_url = start_not_found_owner_stub().await;
    insert_job_record(&store, &job_id, owner_id, json!({"job": "missing"})).await;
    store
        .upsert_broker_lease(owner_id, &owner_url, Duration::from_secs(2))
        .await
        .unwrap();

    let claimant = start_broker_server(
        "claimant-not-found",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(200),
        Duration::from_secs(5),
    )
    .await;

    assert!(matches!(
        claimant
            .bits
            .poll(&job_id, Some(Duration::from_secs(1)))
            .await,
        PollOutcome::Pending { .. }
    ));
    assert_eq!(
        observed_owner(&store, &job_id).await.as_deref(),
        Some(owner_id)
    );
}

#[tokio::test]
async fn owner_404_without_durable_record_is_not_found() {
    let store = shared_store().await;
    let owner_id = "owner-not-found-missing-record";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let owner_url = start_not_found_owner_stub().await;
    store
        .upsert_broker_lease(owner_id, &owner_url, Duration::from_secs(2))
        .await
        .unwrap();

    let claimant = start_broker_server(
        "claimant-not-found-missing-record",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(200),
        Duration::from_secs(5),
    )
    .await;

    assert!(matches!(
        claimant
            .bits
            .poll(&job_id, Some(Duration::from_secs(1)))
            .await,
        PollOutcome::NotFound
    ));
}

#[tokio::test]
async fn active_lease_proxy_pending_when_location_points_back_to_job() {
    // If the owner redirects back to the same job poll URL, the claimant should
    // keep the client in the pending loop rather than treating it as terminal.
    let store = shared_store().await;
    let owner_id = "owner-pending";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let owner_url = start_pending_owner_stub(&job_id).await;
    insert_job_record(&store, &job_id, owner_id, json!({"job": "pending"})).await;
    store
        .upsert_broker_lease(owner_id, &owner_url, Duration::from_secs(2))
        .await
        .unwrap();

    let claimant = start_broker_server(
        "claimant-pending",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(200),
        Duration::from_secs(5),
    )
    .await;

    assert!(matches!(
        claimant
            .bits
            .poll(&job_id, Some(Duration::from_millis(400)))
            .await,
        PollOutcome::Pending { .. }
    ));
    assert_eq!(
        observed_owner(&store, &job_id).await.as_deref(),
        Some(owner_id)
    );
}

#[tokio::test]
async fn active_lease_prevents_reclaim_when_proxy_fails() {
    // Proxy failure alone must not trigger reclaim while the recorded owner
    // lease is still active.
    let store = shared_store().await;
    let owner_id = "owner-live";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let owner_url = start_server_error_owner_stub().await;
    insert_job_record(&store, &job_id, owner_id, json!({"job": "server-error"})).await;
    store
        .upsert_broker_lease(owner_id, &owner_url, Duration::from_secs(2))
        .await
        .unwrap();

    let claimant = start_broker_server(
        "claimant-a",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(200),
        Duration::from_secs(5),
    )
    .await;

    assert!(matches!(
        claimant
            .bits
            .poll(&job_id, Some(Duration::from_millis(250)))
            .await,
        PollOutcome::Pending { .. }
    ));
    assert_eq!(
        observed_owner(&store, &job_id).await.as_deref(),
        Some(owner_id)
    );
}

#[tokio::test]
async fn expired_lease_enables_reclaim_and_completion() {
    // Once the owner lease has expired, another broker should claim the durable
    // job, restore it, and deliver the final result locally.
    let store = shared_store().await;
    let owner_id = "owner-expired";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    insert_job_record(&store, &job_id, owner_id, json!({"job": "reclaim"})).await;
    store
        .upsert_broker_lease(
            owner_id,
            "http://127.0.0.1:1/job",
            Duration::from_millis(20),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let claimant = start_broker_server(
        "claimant-b",
        single_target_switch(TargetBehavior::Success {
            delay: Duration::from_millis(40),
            content_type: "application/json".into(),
            body: br#"{"reclaimed":true}"#.to_vec(),
        }),
        None,
        Some(Arc::clone(&store)),
        Duration::from_secs(1),
        Duration::from_secs(5),
    )
    .await;

    let result = wait_for_ready(&claimant.bits, &job_id, Duration::from_secs(1)).await;
    let (_content_type, body) = read_success_body(result).await;
    assert_eq!(String::from_utf8(body).unwrap(), r#"{"reclaimed":true}"#);
    wait_for_no_owner(&store, &job_id, Duration::from_millis(250)).await;
}

#[tokio::test]
async fn recently_expired_lease_stays_pending_within_clock_skew_buffer() {
    let store = shared_store().await;
    let owner_id = "owner-skew-buffer";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    insert_job_record(&store, &job_id, owner_id, json!({"job": "skew-buffer"})).await;
    store
        .upsert_broker_lease(owner_id, "http://127.0.0.1:1/job", Duration::from_secs(1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1030)).await;

    let claimant = start_broker_server(
        "claimant-skew-buffer",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(200),
        Duration::from_secs(5),
    )
    .await;

    assert!(matches!(
        claimant
            .bits
            .poll(&job_id, Some(Duration::from_millis(250)))
            .await,
        PollOutcome::Pending { .. }
    ));
    assert_eq!(
        observed_owner(&store, &job_id).await.as_deref(),
        Some(owner_id)
    );
}

#[tokio::test]
async fn expired_lease_without_record_is_job_lost() {
    // If the owner lease is expired and the durable job record is gone, polling
    // should return JobLost rather than spin forever.
    let store = shared_store().await;
    let owner_id = "owner-missing";
    store
        .upsert_broker_lease(
            owner_id,
            "http://127.0.0.1:1/job",
            Duration::from_millis(20),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let claimant = start_broker_server(
        "claimant-c",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(200),
        Duration::from_secs(5),
    )
    .await;

    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    assert!(matches!(
        claimant
            .bits
            .poll(&job_id, Some(Duration::from_millis(250)))
            .await,
        PollOutcome::JobLost
    ));
}

#[tokio::test]
async fn competing_claimants_only_one_wins() {
    // Concurrent reclaim attempts should result in exactly one broker becoming
    // the effective winner and producing the terminal result.
    let store = shared_store().await;
    let owner_id = "owner-race";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    insert_job_record(&store, &job_id, owner_id, json!({"job": "race"})).await;
    store
        .upsert_broker_lease(
            owner_id,
            "http://127.0.0.1:1/job",
            Duration::from_millis(20),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let claimant_a = start_broker_server(
        "claimant-race-a",
        single_target_switch(TargetBehavior::Success {
            delay: Duration::from_millis(80),
            content_type: "text/plain".into(),
            body: b"a".to_vec(),
        }),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(400),
        Duration::from_secs(5),
    )
    .await;
    let claimant_b = start_broker_server(
        "claimant-race-b",
        single_target_switch(TargetBehavior::Success {
            delay: Duration::from_millis(80),
            content_type: "text/plain".into(),
            body: b"b".to_vec(),
        }),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(400),
        Duration::from_secs(5),
    )
    .await;

    let (outcome_a, outcome_b) = tokio::join!(
        claimant_a
            .bits
            .poll(&job_id, Some(Duration::from_millis(250))),
        claimant_b
            .bits
            .poll(&job_id, Some(Duration::from_millis(250)))
    );

    let result = match (outcome_a, outcome_b) {
        (PollOutcome::Ready(result), _) => result,
        (_, PollOutcome::Ready(result)) => result,
        (PollOutcome::Pending { .. }, PollOutcome::Pending { .. }) => {
            tokio::select! {
                result = poll_until_terminal(&claimant_a.bits, &job_id, Duration::from_secs(1)) => result,
                result = poll_until_terminal(&claimant_b.bits, &job_id, Duration::from_secs(1)) => result,
            }
        }
        (left, right) => {
            panic!("expected one claimant to produce a ready result, got {left:?} and {right:?}")
        }
    };
    let (_content_type, body) = read_success_body(result).await;
    assert!(body == b"a" || body == b"b");
}

#[tokio::test]
async fn lease_lookup_backend_failure_returns_pending() {
    // Lease registry outages should degrade to Pending so clients retry rather
    // than incorrectly reclaiming or losing the job.
    let inner = shared_store().await;
    let store: Arc<dyn PersistenceStore> = Arc::new(LeaseLookupFailingStore::new(inner));
    let claimant = start_broker_server(
        "claimant-lease-error",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(200),
        Duration::from_secs(5),
    )
    .await;
    let job_id = format!("owner-lease-error~{}", uuid::Uuid::new_v4());

    assert!(matches!(
        claimant
            .bits
            .poll(&job_id, Some(Duration::from_millis(250)))
            .await,
        PollOutcome::Pending { .. }
    ));
}

#[tokio::test]
async fn backend_claim_errors_backoff_within_single_poll() {
    // Claim backend failures should back off and retry within one poll request
    // instead of hammering the persistence backend.
    let raw_store = Arc::new(BackendFailingStore::new());
    let store = raw_store.clone() as Arc<dyn PersistenceStore>;
    let claimant = start_broker_server(
        "claimer-backoff",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(250),
        Duration::from_secs(5),
    )
    .await;
    let owner = "expired-owner";
    let job_id = format!("{owner}~{}", uuid::Uuid::new_v4());
    let started = Instant::now();
    let outcome = claimant
        .bits
        .poll(&job_id, Some(Duration::from_millis(220)))
        .await;
    let elapsed = started.elapsed();

    assert!(matches!(outcome, PollOutcome::Pending { .. }));
    assert!(
        elapsed >= Duration::from_millis(90),
        "expected backoff delay, got {elapsed:?}"
    );
    let attempts = raw_store.attempts();
    assert!(
        attempts >= 2,
        "expected multiple claim attempts, got {attempts}"
    );
}

#[tokio::test]
async fn proxy_auth_error_returns_pending() {
    // A proxied 401 from a live owner should return pending and preserve ownership.
    let store = shared_store().await;
    let owner_id = "owner-auth";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let owner_url = start_status_owner_stub(StatusCode::UNAUTHORIZED).await;
    insert_job_record(&store, &job_id, owner_id, json!({"job": "auth"})).await;
    store
        .upsert_broker_lease(owner_id, &owner_url, Duration::from_secs(2))
        .await
        .unwrap();

    let claimant = start_broker_server(
        "claimant-auth",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(200),
        Duration::from_secs(5),
    )
    .await;

    assert!(matches!(
        claimant
            .bits
            .poll(&job_id, Some(Duration::from_millis(250)))
            .await,
        PollOutcome::Pending { .. }
    ));
    assert_eq!(
        observed_owner(&store, &job_id).await.as_deref(),
        Some(owner_id)
    );
}

#[tokio::test]
async fn proxy_throttled_returns_pending() {
    // A proxied 429 from a live owner should return pending and preserve ownership.
    let store = shared_store().await;
    let owner_id = "owner-throttled";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let owner_url = start_status_owner_stub(StatusCode::TOO_MANY_REQUESTS).await;
    insert_job_record(&store, &job_id, owner_id, json!({"job": "throttled"})).await;
    store
        .upsert_broker_lease(owner_id, &owner_url, Duration::from_secs(2))
        .await
        .unwrap();

    let claimant = start_broker_server(
        "claimant-throttled",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(200),
        Duration::from_secs(5),
    )
    .await;

    assert!(matches!(
        claimant
            .bits
            .poll(&job_id, Some(Duration::from_millis(250)))
            .await,
        PollOutcome::Pending { .. }
    ));
    assert_eq!(
        observed_owner(&store, &job_id).await.as_deref(),
        Some(owner_id)
    );
}

#[tokio::test]
async fn cancel_not_preserved_across_reclaim() {
    // Reclaimed jobs should restore runnable state and complete, not surface as cancelled.
    let store = shared_store().await;
    let owner_id = "owner-cancel-state";
    let claimant_id = "claimant-cancel-state";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    insert_job_record(&store, &job_id, owner_id, json!({"job": "cancel-reclaim"})).await;
    store
        .upsert_broker_lease(
            owner_id,
            "http://127.0.0.1:1/job",
            Duration::from_millis(20),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let claimant = start_broker_server(
        claimant_id,
        single_target_switch(TargetBehavior::Success {
            delay: Duration::from_millis(40),
            content_type: "text/plain".into(),
            body: b"reclaimed-ok".to_vec(),
        }),
        None,
        Some(Arc::clone(&store)),
        Duration::from_secs(1),
        Duration::from_secs(5),
    )
    .await;

    let result = wait_for_ready(&claimant.bits, &job_id, Duration::from_secs(1)).await;
    let (_content_type, body) = read_success_body(result).await;
    assert_eq!(body, b"reclaimed-ok");
    wait_for_no_owner(&store, &job_id, Duration::from_millis(300)).await;
}
