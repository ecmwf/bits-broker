//! NATS-backed validation of the cross-broker per-user limit.
//!
//! Requires a JetStream-enabled NATS server (set BITS_NATS_URL, default
//! nats://127.0.0.1:4222). Gated behind the `nats` feature, so the default
//! `cargo test --workspace` (no server) skips it.

#![cfg(feature = "nats")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use bits::Job;
use bits::actions::{ActionError, CheckResult};
use bits::db::nats::NatsStore;
use bits::db::{BrokerLeaseStore, PersistenceStore, UserLimitStore};
use bits::dispatcher::{DispatchGuard, Dispatcher, ExecutorKind, QueueKind, UserLimitConfig};
use futures::future::BoxFuture;
use tokio::sync::oneshot;

const SCOPE: &str = "route-nats";

fn nats_url() -> String {
    std::env::var("BITS_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string())
}

fn store() -> Arc<dyn PersistenceStore> {
    let unique = uuid::Uuid::new_v4();
    Arc::new(NatsStore::new(
        nats_url(),
        format!("ul-jobs-{unique}"),
        format!("ul-leases-{unique}"),
        Duration::from_secs(30),
        1,
        Duration::from_secs(10),
    ))
}

fn user_job(name: &str) -> Job {
    let mut job = Job::new(serde_json::json!({}));
    *job.user_mut() = serde_json::json!({ "auth": { "username": name } });
    job
}

fn broker(store: Arc<dyn PersistenceStore>, id: &str, max: usize) -> Dispatcher<CheckResult> {
    Dispatcher::<CheckResult>::from_config(
        Some(&QueueKind::Fifo),
        Some(&ExecutorKind::AsyncPool {
            concurrency: Some(16),
        }),
        None,
        None,
        1000,
    )
    .expect("dispatcher config")
    .expect("dispatcher")
    .with_user_limit(
        Some(UserLimitConfig {
            max,
            key: vec!["/auth/username".to_string()],
        }),
        SCOPE,
        id,
        Some(store),
    )
}

fn held(rx: oneshot::Receiver<()>) -> BoxFuture<'static, Result<CheckResult, ActionError>> {
    Box::pin(async move {
        let _ = rx.await;
        Ok(CheckResult::Pass)
    })
}

async fn wait_for_count(store: &Arc<dyn PersistenceStore>, user: &str, want: usize) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let n = store.list_user_slots(SCOPE, user).await.unwrap().len();
        if n == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {want} slots for {user}, saw {n}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn nats_store_reserve_list_release_and_seq_monotonic() {
    let store = store();

    let s1 = store
        .reserve_user_slot(SCOPE, "alice", "job-1", "broker-a")
        .await
        .unwrap();
    let s2 = store
        .reserve_user_slot(SCOPE, "alice", "job-2", "broker-b")
        .await
        .unwrap();
    assert!(s2 > s1, "seq must be store-monotonic ({s1} then {s2})");

    // Idempotent: re-reserving the same job returns its original seq.
    let s1_again = store
        .reserve_user_slot(SCOPE, "alice", "job-1", "broker-a")
        .await
        .unwrap();
    assert_eq!(s1, s1_again, "re-reserve must return the existing seq");

    let mut entries = store.list_user_slots(SCOPE, "alice").await.unwrap();
    entries.sort_by_key(|e| e.seq);
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].job_id, "job-1");
    assert_eq!(entries[0].owner_broker_id, "broker-a");
    assert_eq!(entries[0].seq, s1, "list seq must equal the reserve seq");
    assert_eq!(entries[1].job_id, "job-2");
    assert_eq!(entries[1].owner_broker_id, "broker-b");

    // A different user is a separate namespace.
    assert!(
        store
            .list_user_slots(SCOPE, "bob")
            .await
            .unwrap()
            .is_empty()
    );

    store
        .release_user_slot(SCOPE, "alice", "job-1")
        .await
        .unwrap();
    let remaining = store.list_user_slots(SCOPE, "alice").await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].job_id, "job-2");
}

#[tokio::test]
async fn nats_reclaim_removes_dead_broker_entries_only() {
    let store = store();

    store
        .reserve_user_slot(SCOPE, "carol", "job-dead", "dead-broker")
        .await
        .unwrap();
    store
        .reserve_user_slot(SCOPE, "carol", "job-live", "live-broker")
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

    let remaining = store.list_user_slots(SCOPE, "carol").await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].owner_broker_id, "live-broker");
}

#[tokio::test]
async fn nats_strict_cap_synced_across_replicas() {
    let store = store();
    let a = broker(store.clone(), "broker-a", 2);
    let b = broker(store.clone(), "broker-b", 2);

    // One held alice job on each replica => 2 in-flight for this dispatcher (cap).
    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    let a2 = a.clone();
    let b2 = b.clone();
    let h1 = tokio::spawn(async move {
        a2.dispatch(&user_job("alice"), DispatchGuard::None, held(rx1))
            .await
    });
    let h2 = tokio::spawn(async move {
        b2.dispatch(&user_job("alice"), DispatchGuard::None, held(rx2))
            .await
    });

    // Wait until both are admitted (visible in the shared store).
    wait_for_count(&store, "alice", 2).await;

    // A 3rd alice job on either replica exceeds this dispatcher's cap of 2.
    let rejected = a
        .dispatch(
            &user_job("alice"),
            DispatchGuard::None,
            Box::pin(async { Ok(CheckResult::Pass) }),
        )
        .await;
    assert!(
        matches!(rejected, Err(ActionError::UserLimitExceeded(_))),
        "3rd alice must be rejected over NATS (cap synced across replicas), got {rejected:?}"
    );

    // Complete the two held jobs; their slots free for the dispatcher.
    let _ = tx1.send(());
    let _ = tx2.send(());
    assert!(h1.await.unwrap().is_ok());
    assert!(h2.await.unwrap().is_ok());
    wait_for_count(&store, "alice", 0).await;

    // Admission recovers.
    let readmit = a
        .dispatch(
            &user_job("alice"),
            DispatchGuard::None,
            Box::pin(async { Ok(CheckResult::Pass) }),
        )
        .await;
    assert!(
        readmit.is_ok(),
        "alice should be admitted again, got {readmit:?}"
    );
}
