//! Per-dispatcher, per-user admission limit synchronised across a dispatcher's
//! broker replicas: strict cap, per-user isolation, recovery, reclaim, and lazy
//! local enforcement. The cap is per (dispatcher, user) — never a global tally.
//!
//! Two `Dispatcher`s that share ONE `MemoryStore` and ONE `scope` but carry
//! different `broker_id`s stand in for two replicas of the SAME dispatcher
//! (one route target running on two brokers).

use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use bits::Job;
use bits::actions::{ActionError, CheckResult};
use bits::db::PersistenceStore;
use bits::db::memory::MemoryStore;
use bits::db::{BrokerLeaseStore, UserLimitStore};
use bits::dispatcher::{DispatchGuard, Dispatcher, ExecutorKind, QueueKind, UserLimitConfig};
use futures::future::BoxFuture;
use futures::poll;
use tokio::sync::oneshot;

const SCOPE: &str = "shared-route";

fn broker(
    store: Arc<dyn PersistenceStore>,
    broker_id: &str,
    max: usize,
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
    .with_user_limit(
        Some(UserLimitConfig {
            max,
            key: vec!["/auth/username".to_string()],
        }),
        SCOPE,
        broker_id,
        Some(store),
    )
}

fn user_job(name: &str) -> Job {
    let mut job = Job::new(serde_json::json!({}));
    *job.user_mut() = serde_json::json!({ "auth": { "username": name } });
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
