// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Per-user hard admission cap on a dispatcher (queued + in-flight).

use std::task::Poll;

use bits::actions::{ActionError, CheckResult};
use bits::dispatcher::{DispatchGuard, Dispatcher, ExecutorKind, QueueKind, UserLimitConfig};
use bits::{Job, parse_bootstrap};
use futures::future::BoxFuture;
use futures::poll;
use tokio::sync::oneshot;

fn user_job(name: &str) -> Job {
    let mut job = Job::new(serde_json::json!({}));
    *job.user_mut() = serde_json::json!({ "auth": { "realm": "test", "username": name } });
    job
}

/// Work that blocks until released, so the job stays in-flight (holding its
/// per-user slot) for the duration of the test.
fn held_work(rx: oneshot::Receiver<()>) -> BoxFuture<'static, Result<CheckResult, ActionError>> {
    Box::pin(async move {
        let _ = rx.await;
        Ok(CheckResult::Pass)
    })
}

fn instant_work() -> BoxFuture<'static, Result<CheckResult, ActionError>> {
    Box::pin(async move { Ok(CheckResult::Pass) })
}

fn limited_dispatcher(max: usize) -> Dispatcher<CheckResult> {
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
        Some(UserLimitConfig { max }),
        "test-scope",
        "broker-0",
        None,
    )
}

#[tokio::test]
async fn rejects_when_user_at_cap_and_recovers_after_completion() {
    let dispatcher = limited_dispatcher(2);

    // Two in-flight jobs for alice, held open.
    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    let alice1 = dispatcher.dispatch(&user_job("alice"), DispatchGuard::None, held_work(rx1));
    let alice2 = dispatcher.dispatch(&user_job("alice"), DispatchGuard::None, held_work(rx2));
    tokio::pin!(alice1);
    tokio::pin!(alice2);
    // Poll to run admission (increments alice -> 2) and suspend awaiting the work.
    assert!(matches!(poll!(alice1.as_mut()), Poll::Pending));
    assert!(matches!(poll!(alice2.as_mut()), Poll::Pending));

    // Third concurrent alice job is over the cap -> rejected.
    let rejected = dispatcher
        .dispatch(&user_job("alice"), DispatchGuard::None, instant_work())
        .await;
    assert!(
        matches!(rejected, Err(ActionError::UserLimitExceeded(_))),
        "3rd concurrent alice job must be rejected, got {rejected:?}"
    );

    // A different user is unaffected (per-user, not global).
    let (tx_bob, rx_bob) = oneshot::channel();
    let bob = dispatcher.dispatch(&user_job("bob"), DispatchGuard::None, held_work(rx_bob));
    tokio::pin!(bob);
    assert!(
        matches!(poll!(bob.as_mut()), Poll::Pending),
        "bob should be admitted despite alice being at her cap"
    );

    // Complete alice's two in-flight jobs; their slots free.
    let _ = tx1.send(());
    let _ = tx2.send(());
    assert!(alice1.await.is_ok());
    assert!(alice2.await.is_ok());

    // Alice can be admitted again now that she is back under the cap.
    let after = dispatcher
        .dispatch(&user_job("alice"), DispatchGuard::None, instant_work())
        .await;
    assert!(
        after.is_ok(),
        "alice should be admitted after her earlier jobs completed, got {after:?}"
    );

    let _ = tx_bob.send(());
    assert!(bob.await.is_ok());
}

#[tokio::test]
async fn unidentifiable_user_is_not_limited_fail_open() {
    // max = 1, yet two jobs with no /auth/{realm,username} must both be
    // admitted: an unidentifiable (anonymous) request is never limited.
    let dispatcher = limited_dispatcher(1);

    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    let a = dispatcher.dispatch(
        &Job::new(serde_json::json!({})),
        DispatchGuard::None,
        held_work(rx1),
    );
    let b = dispatcher.dispatch(
        &Job::new(serde_json::json!({})),
        DispatchGuard::None,
        held_work(rx2),
    );
    tokio::pin!(a);
    tokio::pin!(b);
    assert!(matches!(poll!(a.as_mut()), Poll::Pending));
    assert!(
        matches!(poll!(b.as_mut()), Poll::Pending),
        "a 2nd anonymous job must not be rejected despite max=1"
    );

    let _ = tx1.send(());
    let _ = tx2.send(());
    assert!(a.await.is_ok());
    assert!(b.await.is_ok());
}

#[tokio::test]
async fn config_accepts_user_limit() {
    let yaml = r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - target::http:
          url: "http://localhost:9999"
        dispatcher:
          user_limit:
            max: 5
"#;
    assert!(
        parse_bootstrap(yaml).is_ok(),
        "valid user_limit should parse"
    );
}

#[tokio::test]
async fn config_accepts_per_target_user_limit_on_registry_targets() {
    // Mirrors the location-config shape: named registry targets each carry their
    // OWN dispatcher.user_limit, so the cap is per-target (bits scopes the limit
    // by the registry entry name). This is what makes different targets have
    // different, independent limits.
    let yaml = r#"
bits:
  site: tst
  env: dev
  worker_server:
    port: 9001
    advertised_addr: "127.0.0.1:9001"
targets:
  mars_pool:
    type: remote
    dispatcher:
      queue: cost_weighted
      executor:
        type: remote_pool
        heartbeat_timeout_secs: 60
      user_limit:
        max: 3
  mars_area_pool:
    type: remote
    dispatcher:
      queue: cost_weighted
      executor:
        type: remote_pool
        heartbeat_timeout_secs: 60
      user_limit:
        max: 2
routes:
  - mars:
      - target::mars_pool
  - area:
      - target::mars_area_pool
"#;
    assert!(
        parse_bootstrap(yaml).is_ok(),
        "per-target user_limit on registry targets should parse"
    );
}

#[test]
fn config_rejects_zero_max() {
    let yaml = r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - target::http:
          url: "http://localhost:9999"
        dispatcher:
          user_limit:
            max: 0
"#;
    let err = match parse_bootstrap(yaml) {
        Ok(_) => panic!("zero max must be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("dispatcher.user_limit.max"),
        "unexpected error: {err}"
    );
}
