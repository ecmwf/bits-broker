#![cfg(feature = "tikv")]

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bits::db::tikv::TiKvStore;
use bits::db::{BrokerSlotStore, DbError};

fn tikv_endpoints() -> Vec<String> {
    if let Ok(raw) = std::env::var("BITS_TIKV_ENDPOINTS") {
        let endpoints = raw
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if !endpoints.is_empty() {
            return endpoints;
        }
    }

    vec![common::recovery::ensure_tiup_playground()]
}

fn new_store(endpoints: Vec<String>) -> TiKvStore {
    TiKvStore::new(endpoints, Duration::from_secs(10))
}

fn unique_tags() -> (String, String) {
    let value = uuid::Uuid::new_v4().as_u128();
    let site = short_tag(value);
    let env = short_tag(value / (36 * 36 * 36));
    (site, env)
}

fn short_tag(mut value: u128) -> String {
    const ALPHABET: &[u8; 36] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut tag = [b'a'; 3];
    for byte in &mut tag {
        *byte = ALPHABET[(value % 36) as usize];
        value /= 36;
    }
    String::from_utf8(tag.to_vec()).unwrap()
}

#[tokio::test]
async fn tikv_slot_first_allocation_is_zero() {
    let endpoints = tikv_endpoints();
    let store = new_store(endpoints);
    let (site, env) = unique_tags();

    let slot = BrokerSlotStore::allocate_broker_slot(&store, &site, &env)
        .await
        .unwrap();

    assert_eq!(slot, 0);
}

#[tokio::test]
async fn tikv_slot_allocation_survives_store_recreation() {
    let endpoints = tikv_endpoints();
    let (site, env) = unique_tags();
    let first_store = new_store(endpoints.clone());

    assert_eq!(
        BrokerSlotStore::allocate_broker_slot(&first_store, &site, &env)
            .await
            .unwrap(),
        0
    );
    drop(first_store);

    let recreated_store = new_store(endpoints);
    assert_eq!(
        BrokerSlotStore::allocate_broker_slot(&recreated_store, &site, &env)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn tikv_slot_allocation_is_safe_under_concurrent_callers() {
    let endpoints = tikv_endpoints();
    let store = Arc::new(new_store(endpoints));
    let (site, env) = unique_tags();
    let caller_count = 128_u16;
    let mut handles = Vec::new();

    for _ in 0..caller_count {
        let store = Arc::clone(&store);
        let site = site.clone();
        let env = env.clone();
        handles.push(tokio::spawn(async move {
            BrokerSlotStore::allocate_broker_slot(&*store, &site, &env).await
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
async fn tikv_slot_exhaustion_returns_actionable_error_at_u16_max() {
    let endpoints = tikv_endpoints();
    let store = new_store(endpoints);
    let (site, env) = unique_tags();

    for expected_slot in 0..=u16::MAX {
        let slot = BrokerSlotStore::allocate_broker_slot(&store, &site, &env)
            .await
            .unwrap();
        assert_eq!(slot, expected_slot);
    }

    let error = BrokerSlotStore::allocate_broker_slot(&store, &site, &env)
        .await
        .unwrap_err();

    match &error {
        DbError::SlotExhausted {
            site: error_site,
            env: error_env,
            ceiling,
        } => {
            assert_eq!(error_site, &site);
            assert_eq!(error_env, &env);
            assert_eq!(*ceiling, u16::MAX);
        }
        other => panic!("expected SlotExhausted, got {other:?}"),
    }

    let message = error.to_string();
    assert!(message.contains("slot ceiling"));
    assert!(message.contains("version byte"));
    assert!(message.contains("bump"));
}
