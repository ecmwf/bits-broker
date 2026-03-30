#![cfg(feature = "tikv")]

mod common;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bits::db::{PersistenceStore, tikv::TiKvStore};
use bits::server::CODE_JOB_ERROR;
use common::recovery::{
    TargetBehavior, insert_job_record, observed_owner, poll_until_terminal, read_success_body,
    single_target_switch, start_broker_server, start_error_owner_stub, start_gone_owner_stub,
    start_not_found_owner_stub, start_pending_owner_stub, start_redirect_owner_stub,
    start_server_error_owner_stub, start_success_owner_stub, test_client, wait_for_no_owner,
    wait_for_ready,
};
use serde_json::json;
use tokio::sync::Mutex;

fn tikv_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn tikv_endpoints() -> Option<Vec<String>> {
    if let Ok(raw) = std::env::var("BITS_TIKV_ENDPOINTS") {
        let endpoints = raw
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if !endpoints.is_empty() {
            return Some(endpoints);
        }
    }

    Some(vec![common::recovery::ensure_tiup_playground()])
}

async fn shared_store() -> Option<Arc<dyn PersistenceStore>> {
    let endpoints = tikv_endpoints()?;
    Some(Arc::new(TiKvStore::new(endpoints)) as Arc<dyn PersistenceStore>)
}

#[tokio::test]
async fn fast_jobs_do_not_persist() {
    // TiKV-backed fast jobs should remain ephemeral when they finish before the
    // persistence threshold elapses.
    let _guard = tikv_test_lock().lock().await;
    let Some(store) = shared_store().await else {
        return;
    };

    let broker = start_broker_server(
        "fast-broker-tikv",
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

    let handle = broker.bits.submit(bits::Job::new(json!({"kind": "fast"})));
    let result = wait_for_ready(&broker.bits, &handle.id, Duration::from_secs(2)).await;
    let (_content_type, body) = read_success_body(result).await;
    assert_eq!(body, b"fast");
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(observed_owner(&store, &handle.id).await.is_none());
}

#[tokio::test]
async fn active_lease_proxies_success_result() {
    // With a live owner lease in TiKV, a different broker should proxy success
    // instead of reclaiming ownership.
    let _guard = tikv_test_lock().lock().await;
    let Some(store) = shared_store().await else {
        return;
    };
    let owner_id = "owner-success-tikv";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let owner_url = start_success_owner_stub("text/plain", b"proxied-body".to_vec()).await;
    insert_job_record(&store, &job_id, owner_id, json!({"job": "success"})).await;
    store
        .upsert_broker_lease(owner_id, &owner_url, Duration::from_secs(2))
        .await
        .unwrap();

    let claimant = start_broker_server(
        "claimant-success-tikv",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(200),
        Duration::from_secs(5),
    )
    .await;

    let result = wait_for_ready(&claimant.bits, &job_id, Duration::from_secs(2)).await;
    let (content_type, body) = read_success_body(result).await;
    assert_eq!(content_type, "text/plain");
    assert_eq!(body, b"proxied-body");
}

#[tokio::test]
async fn active_lease_proxies_redirect_result() {
    // TiKV-backed live-owner proxying should preserve redirect results.
    let _guard = tikv_test_lock().lock().await;
    let Some(store) = shared_store().await else {
        return;
    };
    let owner_id = "owner-redirect-tikv";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let owner_url = start_redirect_owner_stub("https://example.test/final").await;
    insert_job_record(&store, &job_id, owner_id, json!({"job": "redirect"})).await;
    store
        .upsert_broker_lease(owner_id, &owner_url, Duration::from_secs(2))
        .await
        .unwrap();

    let claimant = start_broker_server(
        "claimant-redirect-tikv",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(200),
        Duration::from_secs(5),
    )
    .await;

    match wait_for_ready(&claimant.bits, &job_id, Duration::from_secs(2)).await {
        bits::JobResult::Redirect { location, .. } => {
            assert_eq!(location, "https://example.test/final");
        }
        other => panic!("expected redirect result, got {other:?}"),
    }
}

#[tokio::test]
async fn active_lease_proxies_error_result() {
    // TiKV-backed live-owner proxying should preserve job-level errors.
    let _guard = tikv_test_lock().lock().await;
    let Some(store) = shared_store().await else {
        return;
    };
    let owner_id = "owner-error-tikv";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let owner_url = start_error_owner_stub("bad request").await;
    insert_job_record(&store, &job_id, owner_id, json!({"job": "error"})).await;
    store
        .upsert_broker_lease(owner_id, &owner_url, Duration::from_secs(2))
        .await
        .unwrap();

    let claimant = start_broker_server(
        "claimant-error-tikv",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(200),
        Duration::from_secs(5),
    )
    .await;

    match wait_for_ready(&claimant.bits, &job_id, Duration::from_secs(2)).await {
        bits::JobResult::Error { message } => assert_eq!(
            message,
            json!({"code": CODE_JOB_ERROR, "message": "bad request", "retryable": false})
                .to_string()
        ),
        other => panic!("expected error result, got {other:?}"),
    }
}

#[tokio::test]
async fn active_lease_proxies_gone_as_cancelled() {
    // A proxied 410/Gone from a TiKV-tracked live owner should map to cancelled.
    let _guard = tikv_test_lock().lock().await;
    let Some(store) = shared_store().await else {
        return;
    };
    let owner_id = "owner-gone-tikv";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let owner_url = start_gone_owner_stub().await;
    insert_job_record(&store, &job_id, owner_id, json!({"job": "gone"})).await;
    store
        .upsert_broker_lease(owner_id, &owner_url, Duration::from_secs(2))
        .await
        .unwrap();

    let claimant = start_broker_server(
        "claimant-gone-tikv",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_millis(200),
        Duration::from_secs(5),
    )
    .await;

    assert!(matches!(
        wait_for_ready(&claimant.bits, &job_id, Duration::from_secs(2)).await,
        bits::JobResult::Cancelled
    ));
}

#[tokio::test]
async fn active_lease_proxies_not_found() {
    // A TiKV-backed live owner returning 404 should remain NotFound, not a reclaim.
    let _guard = tikv_test_lock().lock().await;
    let Some(store) = shared_store().await else {
        return;
    };
    let owner_id = "owner-not-found-tikv";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let owner_url = start_not_found_owner_stub().await;
    insert_job_record(&store, &job_id, owner_id, json!({"job": "missing"})).await;
    store
        .upsert_broker_lease(owner_id, &owner_url, Duration::from_secs(2))
        .await
        .unwrap();

    let claimant = start_broker_server(
        "claimant-not-found-tikv",
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
            .poll(&job_id, Some(Duration::from_secs(2)))
            .await,
        bits::PollOutcome::Pending { .. }
    ));
    assert_eq!(
        observed_owner(&store, &job_id).await.as_deref(),
        Some(owner_id)
    );
}

#[tokio::test]
async fn owner_404_without_durable_record_is_not_found() {
    let _guard = tikv_test_lock().lock().await;
    let Some(store) = shared_store().await else {
        return;
    };
    let owner_id = "owner-not-found-missing-record-tikv";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let owner_url = start_not_found_owner_stub().await;
    store
        .upsert_broker_lease(owner_id, &owner_url, Duration::from_secs(2))
        .await
        .unwrap();

    let claimant = start_broker_server(
        "claimant-not-found-missing-record-tikv",
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
            .poll(&job_id, Some(Duration::from_secs(2)))
            .await,
        bits::PollOutcome::NotFound
    ));
}

#[tokio::test]
async fn active_lease_proxy_pending_when_location_points_back_to_job() {
    // Self-referential proxy redirects should keep the client pending in the
    // TiKV-backed recovery path.
    let _guard = tikv_test_lock().lock().await;
    let Some(store) = shared_store().await else {
        return;
    };
    let owner_id = "owner-pending-tikv";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let owner_url = start_pending_owner_stub(&job_id).await;
    insert_job_record(&store, &job_id, owner_id, json!({"job": "pending"})).await;
    store
        .upsert_broker_lease(owner_id, &owner_url, Duration::from_secs(2))
        .await
        .unwrap();

    let claimant = start_broker_server(
        "claimant-pending-tikv",
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
        bits::PollOutcome::Pending { .. }
    ));
}

#[tokio::test]
async fn active_lease_prevents_reclaim_when_proxy_fails() {
    // Even with TiKV, proxy failure must not trigger reclaim while the owner
    // lease is still active.
    let _guard = tikv_test_lock().lock().await;
    let Some(store) = shared_store().await else {
        return;
    };
    let owner_id = "owner-live-tikv";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    let owner_url = start_server_error_owner_stub().await;
    insert_job_record(&store, &job_id, owner_id, json!({"job": "server-error"})).await;
    store
        .upsert_broker_lease(owner_id, &owner_url, Duration::from_secs(2))
        .await
        .unwrap();

    let claimant = start_broker_server(
        "claimant-a-tikv",
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
        bits::PollOutcome::Pending { .. }
    ));
    assert_eq!(
        observed_owner(&store, &job_id).await.as_deref(),
        Some(owner_id)
    );
}

#[tokio::test]
async fn expired_lease_enables_reclaim_and_completion() {
    // Once the TiKV lease expires, another broker should claim, restore, and
    // complete the durable job.
    let _guard = tikv_test_lock().lock().await;
    let Some(store) = shared_store().await else {
        return;
    };
    let owner_id = "owner-expired-tikv";
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
        "claimant-b-tikv",
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

    let result = wait_for_ready(&claimant.bits, &job_id, Duration::from_secs(2)).await;
    let (_content_type, body) = read_success_body(result).await;
    assert_eq!(String::from_utf8(body).unwrap(), r#"{"reclaimed":true}"#);
    wait_for_no_owner(&store, &job_id, Duration::from_millis(500)).await;
}

#[tokio::test]
async fn recently_expired_lease_stays_pending_within_clock_skew_buffer() {
    let _guard = tikv_test_lock().lock().await;
    let Some(store) = shared_store().await else {
        return;
    };
    let owner_id = "owner-skew-buffer-tikv";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    insert_job_record(&store, &job_id, owner_id, json!({"job": "skew-buffer"})).await;
    store
        .upsert_broker_lease(owner_id, "http://127.0.0.1:1/job", Duration::from_secs(1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1030)).await;

    let claimant = start_broker_server(
        "claimant-skew-buffer-tikv",
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
        bits::PollOutcome::Pending { .. }
    ));
    assert_eq!(
        observed_owner(&store, &job_id).await.as_deref(),
        Some(owner_id)
    );
}

#[tokio::test]
async fn expired_lease_without_record_is_job_lost() {
    // TiKV-backed polling should return JobLost when the owner lease is expired
    // and no durable record remains.
    let _guard = tikv_test_lock().lock().await;
    let Some(store) = shared_store().await else {
        return;
    };
    let owner_id = "owner-missing-tikv";
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
        "claimant-c-tikv",
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
        bits::PollOutcome::JobLost
    ));
}

#[tokio::test]
async fn competing_claimants_only_one_wins() {
    // Under TiKV optimistic transactions, concurrent reclaim attempts should
    // still yield exactly one terminal winner.
    let _guard = tikv_test_lock().lock().await;
    let Some(store) = shared_store().await else {
        return;
    };
    let owner_id = "owner-race-tikv";
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
        "claimant-race-a-tikv",
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
        "claimant-race-b-tikv",
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
        (bits::PollOutcome::Ready(result), _) => result,
        (_, bits::PollOutcome::Ready(result)) => result,
        (bits::PollOutcome::Pending { .. }, bits::PollOutcome::Pending { .. }) => {
            tokio::select! {
                result = poll_until_terminal(&claimant_a.bits, &job_id, Duration::from_secs(2)) => result,
                result = poll_until_terminal(&claimant_b.bits, &job_id, Duration::from_secs(2)) => result,
            }
        }
        (left, right) => {
            panic!("expected one claimant to produce a ready result, got {left:?} and {right:?}")
        }
    };
    let (_content_type, body) = read_success_body(result).await;
    assert!(body == b"a" || body == b"b");
}

async fn poll_wrong_broker_until_terminal(
    client: &reqwest::Client,
    broker: &common::recovery::BrokerServer,
    job_id: &str,
) -> reqwest::Response {
    for _ in 0..6 {
        let response = client.get(broker.job_url(job_id)).send().await.unwrap();
        if response.status() != reqwest::StatusCode::SEE_OTHER {
            return response;
        }
        let location = response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        if !location.contains(job_id) {
            return response;
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    panic!("wrong-broker polling never reached a terminal response for {job_id}");
}

#[tokio::test]
async fn wrong_broker_reconnect_proxies_success_result() {
    // End-to-end TiKV sticky-session drift: reconnecting to the wrong broker
    // should still proxy the successful response from the live owner.
    let _guard = tikv_test_lock().lock().await;
    let Some(store) = shared_store().await else {
        return;
    };
    let owner = start_broker_server(
        "broker-a-success-tikv",
        single_target_switch(TargetBehavior::Success {
            delay: Duration::from_millis(120),
            content_type: "application/json".into(),
            body: br#"{"broker":"a"}"#.to_vec(),
        }),
        Some(Duration::from_millis(20)),
        Some(Arc::clone(&store)),
        Duration::from_millis(40),
        Duration::from_secs(5),
    )
    .await;
    let standby = start_broker_server(
        "broker-b-success-tikv",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_secs(1),
        Duration::from_secs(5),
    )
    .await;

    let client = test_client();
    let submit = client
        .post(owner.submit_url())
        .json(&json!({"kind": "sticky-success"}))
        .send()
        .await
        .unwrap();
    let location = submit
        .headers()
        .get(reqwest::header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let job_id = location.trim_start_matches("/job/");

    let proxied = poll_wrong_broker_until_terminal(&client, &standby, job_id).await;
    assert_eq!(proxied.status(), reqwest::StatusCode::OK);
    let body = proxied.bytes().await.unwrap();
    assert_eq!(body.as_ref(), br#"{"broker":"a"}"#);
}

#[tokio::test]
async fn wrong_broker_reconnect_proxies_redirect_result() {
    // End-to-end TiKV wrong-broker reconnects should preserve terminal redirects.
    let _guard = tikv_test_lock().lock().await;
    let Some(store) = shared_store().await else {
        return;
    };
    let owner = start_broker_server(
        "broker-a-redirect-tikv",
        single_target_switch(TargetBehavior::Redirect {
            delay: Duration::from_millis(120),
            location: "https://example.test/download".into(),
            message: "go elsewhere".into(),
        }),
        Some(Duration::from_millis(20)),
        Some(Arc::clone(&store)),
        Duration::from_millis(40),
        Duration::from_secs(5),
    )
    .await;
    let standby = start_broker_server(
        "broker-b-redirect-tikv",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_secs(1),
        Duration::from_secs(5),
    )
    .await;

    let client = test_client();
    let submit = client
        .post(owner.submit_url())
        .json(&json!({"kind": "sticky-redirect"}))
        .send()
        .await
        .unwrap();
    let location = submit
        .headers()
        .get(reqwest::header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let job_id = location.trim_start_matches("/job/");

    let proxied = poll_wrong_broker_until_terminal(&client, &standby, job_id).await;
    assert_eq!(proxied.status(), reqwest::StatusCode::SEE_OTHER);
    assert_eq!(
        proxied.headers().get(reqwest::header::LOCATION).unwrap(),
        "https://example.test/download"
    );
}

#[tokio::test]
async fn sticky_session_failure_returns_gone_when_no_durable_record_exists() {
    // End-to-end TiKV lost-job case: wrong-broker reconnects should return 410
    // when the owner is gone and the durable record has already been removed.
    let _guard = tikv_test_lock().lock().await;
    let Some(store) = shared_store().await else {
        return;
    };
    let standby = start_broker_server(
        "broker-b-gone-tikv",
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_secs(1),
        Duration::from_secs(5),
    )
    .await;

    let owner_id = "broker-a-missing-tikv";
    let job_id = format!("{owner_id}~{}", uuid::Uuid::new_v4());
    insert_job_record(&store, &job_id, owner_id, json!({"kind": "lost"})).await;
    store.delete_job(&job_id).await.unwrap();
    store
        .upsert_broker_lease(
            owner_id,
            "http://127.0.0.1:1/job",
            Duration::from_millis(20),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let client = test_client();
    let response = client.get(standby.job_url(&job_id)).send().await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::GONE);
}
