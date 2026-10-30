#![cfg(all(feature = "nats", feature = "test-util"))]
// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Regression tests for `bits-jobs` tombstone accumulation.
//!
//! `delete_job` used a plain KV `purge()`, which leaves a permanent PURGE
//! tombstone in the stream (the bucket has no `max_age`). Every job that
//! outlived `persist_after_secs` therefore left one message behind forever,
//! so the stream grew with total historical throughput. Same root cause as
//! the `bits-leases-userlimits` bloat, fixed the same way: a per-message TTL
//! on the tombstone only.

mod common;

use std::time::Duration;

use bits::db::nats::NatsStore;
use bits::db::{BrokerSlotStore, ClaimResult, JobStore, PersistentJobRecord};
use common::recovery::{ensure_nats_server, new_recovery_job_id};
use serde_json::json;

fn store_with_short_ttls() -> NatsStore {
    let unique = uuid::Uuid::new_v4();
    NatsStore::new(
        ensure_nats_server(),
        format!("tomb-jobs-{unique}"),
        format!("tomb-leases-{unique}"),
        Duration::from_secs(30),
        1,
        Duration::from_secs(10),
    )
    .with_job_tombstone_ttl(Duration::from_secs(1))
    .with_job_delete_marker_ttl(Duration::from_secs(1))
}

fn record(job_id: &str) -> PersistentJobRecord {
    PersistentJobRecord {
        job_id: job_id.to_string(),
        broker_id: "tst-tmb-1".to_string(),
        original_request: json!({"kind": "tombstone"}),
        user: json!({}),
        metadata: json!({}),
        created_at: chrono::Utc::now(),
    }
}

async fn assert_not_found(store: &NatsStore, job_id: &str) {
    match store
        .claim_if_owner(job_id, "tst-tmb-1", "tst-tmb-2")
        .await
        .expect("claim")
    {
        ClaimResult::NotFound => {}
        ClaimResult::Claimed(_) => panic!("deleted job must not be claimable"),
        ClaimResult::Active { .. } => panic!("deleted job must not be reported active"),
    }
}

async fn wait_for_message_count(store: &NatsStore, expected: u64, within: Duration) {
    let deadline = std::time::Instant::now() + within;
    loop {
        let count = store.debug_jobs_stream_message_count().await.unwrap();
        if count == expected {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "jobs stream still has {count} raw message(s) after {within:?}, expected {expected} \
             (deleted-job tombstones are not expiring)"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn deleted_job_tombstone_self_expires() {
    let store = store_with_short_ttls();
    let job_id = new_recovery_job_id("tst", "tmb", 1);

    store.upsert_job(record(&job_id)).await.expect("upsert");
    assert_eq!(store.debug_jobs_stream_message_count().await.unwrap(), 1);

    store.delete_job(&job_id).await.expect("delete");
    assert_not_found(&store, &job_id).await;
    assert_eq!(
        store.debug_jobs_stream_message_count().await.unwrap(),
        1,
        "the purge tombstone should still exist immediately after delete"
    );

    wait_for_message_count(&store, 0, Duration::from_secs(10)).await;
    assert_not_found(&store, &job_id).await;
}

#[tokio::test]
async fn tombstone_ttl_does_not_touch_live_records_or_the_slot_counter() {
    let store = store_with_short_ttls();
    let live = new_recovery_job_id("tst", "tmb", 2);
    let deleted = new_recovery_job_id("tst", "tmb", 3);

    store.allocate_broker_slot("tst", "tmb").await.unwrap();
    store.upsert_job(record(&live)).await.unwrap();
    store.upsert_job(record(&deleted)).await.unwrap();
    store.delete_job(&deleted).await.unwrap();

    // Counter + live record remain once the deleted job's tombstone expires.
    wait_for_message_count(&store, 2, Duration::from_secs(10)).await;
    assert!(matches!(
        store
            .claim_if_owner(&live, "tst-tmb-1", "tst-tmb-1")
            .await
            .unwrap(),
        ClaimResult::Claimed(_)
    ));
    assert_eq!(store.allocate_broker_slot("tst", "tmb").await.unwrap(), 1);
}

#[tokio::test]
async fn delete_job_works_on_a_jobs_bucket_that_predates_the_ttl_fix() {
    let store = store_with_short_ttls();
    store
        .debug_precreate_legacy_jobs_bucket()
        .await
        .expect("precreate legacy jobs bucket");

    let job_id = new_recovery_job_id("tst", "tmb", 4);
    store.upsert_job(record(&job_id)).await.unwrap();
    store
        .delete_job(&job_id)
        .await
        .expect("delete must succeed against a reconciled legacy bucket");
    assert_not_found(&store, &job_id).await;
}
