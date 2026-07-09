// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bits::db::{PersistenceStore, memory::MemoryStore};
use common::recovery::{
    LeaseWriteGateStore, TargetBehavior, broker_identity, insert_job_record, new_recovery_job_id,
    single_target_switch, start_broker_server, test_client, wait_for_owner,
};
use serde_json::json;

async fn shared_store() -> Arc<dyn PersistenceStore> {
    Arc::new(MemoryStore::new()) as Arc<dyn PersistenceStore>
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
    // Simulate sticky-session drift: a client reconnects to the wrong broker,
    // which should proxy the successful response from the live owner.
    let store = shared_store().await;
    let owner_id = broker_identity("tst", "htt", 10);
    let standby_id = broker_identity("tst", "htt", 11);
    let owner = start_broker_server(
        owner_id,
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
        standby_id,
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
    assert_eq!(submit.status(), reqwest::StatusCode::SEE_OTHER);
    let location = submit
        .headers()
        .get(reqwest::header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let job_id = location.trim_start_matches("/job/");
    wait_for_owner(&store, job_id, owner_id, Duration::from_millis(250)).await;

    let proxied = poll_wrong_broker_until_terminal(&client, &standby, job_id).await;
    assert_eq!(proxied.status(), reqwest::StatusCode::OK);
    assert_eq!(
        proxied
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .unwrap(),
        "application/json"
    );
    let body = proxied.bytes().await.unwrap();
    assert_eq!(body.as_ref(), br#"{"broker":"a"}"#);
}

#[tokio::test]
async fn wrong_broker_reconnect_proxies_redirect_result() {
    // Wrong-broker reconnects should also preserve terminal redirects produced
    // by the live owner.
    let store = shared_store().await;
    let owner_id = broker_identity("tst", "htt", 12);
    let standby_id = broker_identity("tst", "htt", 13);
    let owner = start_broker_server(
        owner_id,
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
        standby_id,
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
async fn sticky_session_failure_reclaims_after_owner_lease_expires() {
    // Simulate sticky-session failure plus owner loss: after the original
    // broker stops renewing its lease, the standby broker should reclaim and
    // complete the persisted job.
    let base_store = shared_store().await;
    let gated = Arc::new(LeaseWriteGateStore::new(Arc::clone(&base_store)));
    let lease_flag = gated.lease_flag();
    let store = gated.clone() as Arc<dyn PersistenceStore>;

    let owner_id = broker_identity("tst", "htt", 14);
    let standby_id = broker_identity("tst", "htt", 15);
    let owner = start_broker_server(
        owner_id,
        single_target_switch(TargetBehavior::Success {
            delay: Duration::from_millis(300),
            content_type: "application/json".into(),
            body: br#"{"broker":"a"}"#.to_vec(),
        }),
        Some(Duration::from_millis(20)),
        Some(Arc::clone(&store)),
        Duration::from_millis(40),
        Duration::from_millis(120),
    )
    .await;
    let standby = start_broker_server(
        standby_id,
        single_target_switch(TargetBehavior::Success {
            delay: Duration::from_millis(30),
            content_type: "application/json".into(),
            body: br#"{"broker":"b"}"#.to_vec(),
        }),
        None,
        Some(Arc::clone(&store)),
        Duration::from_secs(1),
        Duration::from_millis(120),
    )
    .await;

    let client = test_client();
    let submit = client
        .post(owner.submit_url())
        .json(&json!({"kind": "sticky-reclaim"}))
        .send()
        .await
        .unwrap();
    assert_eq!(submit.status(), reqwest::StatusCode::SEE_OTHER);
    let location = submit
        .headers()
        .get(reqwest::header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let job_id = location.trim_start_matches("/job/").to_string();

    wait_for_owner(&store, &job_id, owner_id, Duration::from_millis(250)).await;
    lease_flag.store(false, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(180)).await;

    let reclaimed = client.get(standby.job_url(&job_id)).send().await.unwrap();
    assert_eq!(reclaimed.status(), reqwest::StatusCode::OK);
    let body = reclaimed.bytes().await.unwrap();
    assert_eq!(body.as_ref(), br#"{"broker":"b"}"#);
}

#[tokio::test]
async fn sticky_session_failure_returns_gone_when_no_durable_record_exists() {
    // If a reconnect lands on the wrong broker after the owner is gone and the
    // durable record has already disappeared, the HTTP surface should return 410.
    let store = shared_store().await;
    let standby = start_broker_server(
        broker_identity("tst", "htt", 16),
        single_target_switch(TargetBehavior::Never),
        None,
        Some(Arc::clone(&store)),
        Duration::from_secs(1),
        Duration::from_secs(5),
    )
    .await;

    let owner_id = broker_identity("tst", "htt", 1);
    let job_id = new_recovery_job_id("tst", "htt", 1);
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
