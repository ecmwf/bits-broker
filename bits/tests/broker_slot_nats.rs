#![cfg(feature = "nats")]

// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bits::db::nats::NatsStore;
use bits::db::{BrokerSlotStore, DbError};

fn unique_bucket(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4())
}

fn nats_url() -> String {
    std::env::var("BITS_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string())
}

fn new_store(url: String, jobs_bucket: String) -> NatsStore {
    NatsStore::new(
        url,
        jobs_bucket,
        unique_bucket("bits-slot-leases"),
        Duration::from_secs(10),
        1,
        Duration::from_secs(10),
    )
}

#[tokio::test]
async fn nats_slot_first_allocation_is_zero() {
    let url = nats_url();
    let store = new_store(url, unique_bucket("bits-slot-jobs"));

    let slot = BrokerSlotStore::allocate_broker_slot(&store, "ams", "prd")
        .await
        .unwrap();

    assert_eq!(slot, 0);
}

#[tokio::test]
async fn nats_slot_allocation_survives_store_recreation() {
    let url = nats_url();
    let jobs_bucket = unique_bucket("bits-slot-jobs");
    let first_store = new_store(url.clone(), jobs_bucket.clone());

    assert_eq!(
        BrokerSlotStore::allocate_broker_slot(&first_store, "ams", "prd")
            .await
            .unwrap(),
        0
    );
    drop(first_store);

    let recreated_store = new_store(url, jobs_bucket);
    assert_eq!(
        BrokerSlotStore::allocate_broker_slot(&recreated_store, "ams", "prd")
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn nats_slot_allocation_is_safe_under_concurrent_callers() {
    let url = nats_url();
    let store = Arc::new(new_store(url, unique_bucket("bits-slot-jobs")));
    let caller_count = 128_u16;
    let mut handles = Vec::new();

    for _ in 0..caller_count {
        let store = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            BrokerSlotStore::allocate_broker_slot(&*store, "ams", "prd").await
        }));
    }

    let mut slots = Vec::new();
    for handle in handles {
        slots.push(handle.await.unwrap().unwrap());
    }
    slots.sort_unstable();

    let unique_slots: HashSet<u16> = slots.iter().copied().collect();
    assert_eq!(unique_slots.len(), usize::from(caller_count));
    assert_eq!(slots, (0..caller_count).collect::<Vec<_>>());
}

#[tokio::test]
async fn nats_slot_exhaustion_returns_actionable_error_at_u16_max() {
    let url = nats_url();
    let store = new_store(url, unique_bucket("bits-slot-jobs"));

    for expected_slot in 0..=u16::MAX {
        let slot = BrokerSlotStore::allocate_broker_slot(&store, "ams", "prd")
            .await
            .unwrap();
        assert_eq!(slot, expected_slot);
    }

    let error = BrokerSlotStore::allocate_broker_slot(&store, "ams", "prd")
        .await
        .unwrap_err();

    match &error {
        DbError::SlotExhausted { site, env, ceiling } => {
            assert_eq!(site, "ams");
            assert_eq!(env, "prd");
            assert_eq!(*ceiling, u16::MAX);
        }
        other => panic!("expected SlotExhausted, got {other:?}"),
    }

    let message = error.to_string();
    assert!(message.contains("slot ceiling"));
    assert!(message.contains("version byte"));
    assert!(message.contains("bump"));
}
