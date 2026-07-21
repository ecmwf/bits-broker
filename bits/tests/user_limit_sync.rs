// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Per-dispatcher, per-user admission limit synchronised across a dispatcher's
//! broker replicas: strict cap, per-user isolation, recovery, reclaim, and lazy
//! local enforcement. The cap is per (dispatcher, user) — never a global tally.
//!
//! Two `Dispatcher`s that share ONE `MemoryStore` and ONE `scope` but carry
//! different `broker_id`s stand in for two replicas of the SAME dispatcher
//! (one route target running on two brokers).

use std::collections::HashMap;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use bits::Job;
use bits::actions::{ActionError, CheckResult};
use bits::db::PersistenceStore;
use bits::db::memory::MemoryStore;
use bits::db::{BrokerLeaseStore, UserLimitStore};
use bits::dispatcher::{
    DispatchGuard, Dispatcher, ExecutorKind, QueueKind, RealmLimit, UserLimitConfig,
};
use futures::future::BoxFuture;
use futures::poll;
use tokio::sync::oneshot;

const SCOPE: &str = "shared-route";

fn broker(
    store: Arc<dyn PersistenceStore>,
    broker_id: &str,
    max: usize,
) -> Dispatcher<CheckResult> {
    broker_cfg(
        store,
        broker_id,
        UserLimitConfig {
            default: Some(max),
            realms: HashMap::new(),
        },
    )
}

fn broker_cfg(
    store: Arc<dyn PersistenceStore>,
    broker_id: &str,
    cfg: UserLimitConfig,
) -> Dispatcher<CheckResult> {
    Dispatcher::<CheckResult>::from_config(
        Some(&QueueKind::Fifo),
        Some(&ExecutorKind::AsyncPool {
            concurrency: Some(16),
        }),
        None,
        None,
        1000,
    )
    .expect("dispatcher config should not error")
    .expect("dispatcher should be created")
    .with_user_limit(Some(cfg), SCOPE, broker_id, Some(store))
}

fn user_job(name: &str) -> Job {
    let mut job = Job::new(serde_json::json!({}));
    *job.user_mut() = serde_json::json!({ "auth": { "realm": "test", "username": name } });
    job
}

fn user_job_roles(name: &str, roles: &[&str]) -> Job {
    let mut job = Job::new(serde_json::json!({}));
    *job.user_mut() =
        serde_json::json!({ "auth": { "realm": "test", "username": name, "roles": roles } });
    job
}

fn held(rx: oneshot::Receiver<()>) -> BoxFuture<'static, Result<CheckResult, ActionError>> {
    Box::pin(async move {
        let _ = rx.await;
        Ok(CheckResult::Pass)
    })
}

fn instant() -> BoxFuture<'static, Result<CheckResult, ActionError>> {
    Box::pin(async move { Ok(CheckResult::Pass) })
}

#[tokio::test]
async fn strict_cap_synced_across_replicas() {
    let store: Arc<dyn PersistenceStore> = Arc::new(MemoryStore::new());
    let a = broker(store.clone(), "broker-a", 2);
    let b = broker(store.clone(), "broker-b", 2);

    // One alice job in-flight on each replica => 2 for this dispatcher (the cap).
    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    let j1 = a.dispatch(&user_job("alice"), DispatchGuard::None, held(rx1));
    let j2 = b.dispatch(&user_job("alice"), DispatchGuard::None, held(rx2));
    tokio::pin!(j1);
    tokio::pin!(j2);
    assert!(matches!(poll!(j1.as_mut()), Poll::Pending));
    assert!(matches!(poll!(j2.as_mut()), Poll::Pending));

    // A 3rd alice job on EITHER replica exceeds this dispatcher's cap of 2.
    let on_a = a
        .dispatch(&user_job("alice"), DispatchGuard::None, instant())
        .await;
    assert!(
        matches!(on_a, Err(ActionError::UserLimitExceeded(_))),
        "3rd alice on broker-a must be rejected (cap synced across replicas), got {on_a:?}"
    );
    let on_b = b
        .dispatch(&user_job("alice"), DispatchGuard::None, instant())
        .await;
    assert!(
        matches!(on_b, Err(ActionError::UserLimitExceeded(_))),
        "3rd alice on broker-b must be rejected (cap synced across replicas), got {on_b:?}"
    );

    // A different user is unaffected by alice's usage.
    let (tx_bob, rx_bob) = oneshot::channel();
    let jb = b.dispatch(&user_job("bob"), DispatchGuard::None, held(rx_bob));
    tokio::pin!(jb);
    assert!(
        matches!(poll!(jb.as_mut()), Poll::Pending),
        "bob must be admitted"
    );

    // Complete one alice job; its slot frees for the dispatcher (allow the
    // spawned store-release task to run).
    let _ = tx1.send(());
    assert!(j1.await.is_ok());
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;

    let readmit = a
        .dispatch(&user_job("alice"), DispatchGuard::None, instant())
        .await;
    assert!(
        readmit.is_ok(),
        "after one alice job completed, a new alice job should be admitted, got {readmit:?}"
    );

    let _ = tx2.send(());
    let _ = tx_bob.send(());
    assert!(j2.await.is_ok());
    assert!(jb.await.is_ok());
}

#[tokio::test]
async fn reclaim_removes_dead_broker_entries_only() {
    let store = Arc::new(MemoryStore::new());

    // An entry owned by a broker with NO live lease (crashed), and one owned by
    // a broker with a live lease.
    store
        .reserve_user_slot(SCOPE, "alice", "job-dead", "dead-broker")
        .await
        .unwrap();
    store
        .reserve_user_slot(SCOPE, "alice", "job-live", "live-broker")
        .await
        .unwrap();
    store
        .upsert_broker_lease("live-broker", "http://live:8080", Duration::from_secs(60))
        .await
        .unwrap();

    let removed = store.reclaim_user_slots().await.unwrap();
    assert_eq!(
        removed, 1,
        "only the dead broker's entry should be reclaimed"
    );

    let remaining = store.list_user_slots(SCOPE, "alice").await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].owner_broker_id, "live-broker");
}

#[tokio::test]
async fn lazy_mode_enforces_local_cap() {
    // max > 3 (here 12) -> lazy mode. A single broker still enforces the cap via the
    // local gate (and the periodically-reconciled per-dispatcher view).
    let store: Arc<dyn PersistenceStore> = Arc::new(MemoryStore::new());
    let a = broker(store.clone(), "broker-a", 12);

    let mut holds = Vec::new();
    for _ in 0..12 {
        let (tx, rx) = oneshot::channel();
        let mut fut = a.dispatch(&user_job("carol"), DispatchGuard::None, held(rx));
        assert!(
            matches!(poll!(&mut fut), Poll::Pending),
            "each of the first 12 carol jobs should be admitted"
        );
        holds.push((tx, fut));
    }

    let over = a
        .dispatch(&user_job("carol"), DispatchGuard::None, instant())
        .await;
    assert!(
        matches!(over, Err(ActionError::UserLimitExceeded(_))),
        "the 13th carol job should be rejected, got {over:?}"
    );

    for (tx, fut) in holds {
        let _ = tx.send(());
        assert!(fut.await.is_ok());
    }
}

#[tokio::test]
async fn mixed_strict_and_lazy_users_in_one_scope() {
    // One dispatcher/scope with role-derived ceilings: a strict user (max 2 <= 3)
    // and a lazy user (max 6 > 3) run simultaneously; each is capped at its own
    // ceiling and they do not interfere.
    let store: Arc<dyn PersistenceStore> = Arc::new(MemoryStore::new());
    let mut realms = HashMap::new();
    realms.insert(
        "test".to_string(),
        RealmLimit {
            default: None,
            roles: [("strict".to_string(), 2usize), ("lazy".to_string(), 6usize)]
                .into_iter()
                .collect(),
        },
    );
    let a = broker_cfg(
        store.clone(),
        "broker-a",
        UserLimitConfig {
            default: None,
            realms,
        },
    );

    // Strict user: fill to cap 2.
    let mut strict_holds = Vec::new();
    for _ in 0..2 {
        let (tx, rx) = oneshot::channel();
        let mut fut = a.dispatch(
            &user_job_roles("sam", &["strict"]),
            DispatchGuard::None,
            held(rx),
        );
        assert!(matches!(poll!(&mut fut), Poll::Pending));
        strict_holds.push((tx, fut));
    }
    let strict_over = a
        .dispatch(
            &user_job_roles("sam", &["strict"]),
            DispatchGuard::None,
            instant(),
        )
        .await;
    let Err(ActionError::UserLimitExceeded(msg)) = strict_over else {
        panic!("3rd strict-user job must be rejected at its cap of 2, got {strict_over:?}");
    };
    assert!(
        msg.contains("(2)"),
        "rejection message must report the strict user's resolved ceiling of 2: {msg}"
    );

    // Lazy user runs concurrently: fill to cap 6, unaffected by the strict user.
    let mut lazy_holds = Vec::new();
    for _ in 0..6 {
        let (tx, rx) = oneshot::channel();
        let mut fut = a.dispatch(
            &user_job_roles("leo", &["lazy"]),
            DispatchGuard::None,
            held(rx),
        );
        assert!(
            matches!(poll!(&mut fut), Poll::Pending),
            "each of the first 6 lazy-user jobs should be admitted"
        );
        lazy_holds.push((tx, fut));
    }
    let lazy_over = a
        .dispatch(
            &user_job_roles("leo", &["lazy"]),
            DispatchGuard::None,
            instant(),
        )
        .await;
    let Err(ActionError::UserLimitExceeded(msg)) = lazy_over else {
        panic!("7th lazy-user job must be rejected at its cap of 6, got {lazy_over:?}");
    };
    assert!(
        msg.contains("(6)"),
        "rejection message must report the lazy user's resolved ceiling of 6: {msg}"
    );

    // The strict user is still exactly at its own cap (unaffected by the lazy user).
    let strict_still_over = a
        .dispatch(
            &user_job_roles("sam", &["strict"]),
            DispatchGuard::None,
            instant(),
        )
        .await;
    assert!(
        matches!(strict_still_over, Err(ActionError::UserLimitExceeded(_))),
        "strict user must remain capped at 2 while the lazy user is at 6, got {strict_still_over:?}"
    );

    for (tx, fut) in strict_holds.into_iter().chain(lazy_holds) {
        let _ = tx.send(());
        assert!(fut.await.is_ok());
    }
}
