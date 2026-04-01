mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bits::{Bits, Job, PollOutcome};

fn fast_sweep_config() -> &'static str {
    r#"
bits:
  sweep_interval_secs: 0.05
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 0
          concurrency: 1
"#
}

fn slow_sweep_config() -> &'static str {
    r#"
bits:
  sweep_interval_secs: 30.0
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 0
          concurrency: 1
"#
}

#[tokio::test]
async fn sweeper_does_not_remove_job_during_reconnect_window() {
    let _ = common::TargetDummyDelay::new(0);

    let bits = Arc::new(Bits::from_config(fast_sweep_config()).unwrap());
    let handle = bits.submit(Job::new(serde_json::json!({})));

    // The reconnect buffer is 5s. Sleep 300ms (6 sweep cycles at 50ms) —
    // the sweeper runs multiple times but client_present() stays true
    // because the reconnect deadline hasn't expired yet.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let outcome = bits.poll(&handle.id, Some(Duration::from_secs(2))).await;
    assert!(
        matches!(outcome, PollOutcome::Ready(_)),
        "expected Ready, got {outcome:?}"
    );
}

#[tokio::test]
async fn drop_completes_promptly_via_condvar_wakeup() {
    let _ = common::TargetDummyDelay::new(0);

    // Use a 30-second sweep interval. Without condvar wakeup, join()
    // would block for up to 30s. With condvar, it completes instantly.
    let bits = Bits::from_config(slow_sweep_config()).unwrap();
    let _ = bits.submit(Job::new(serde_json::json!({})));

    tokio::time::sleep(Duration::from_millis(100)).await;

    let start = Instant::now();
    drop(bits);
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(2),
        "drop took {elapsed:?}; expected well under the 30s sweep interval"
    );
}

#[tokio::test]
async fn concurrent_poll_and_sweep_do_not_deadlock() {
    let _ = common::TargetDummyDelay::new(0);

    let bits = Arc::new(Bits::from_config(fast_sweep_config()).unwrap());

    let mut handles = Vec::new();
    for _ in 0..20 {
        let bits = bits.clone();
        handles.push(tokio::spawn(async move {
            let h = bits.submit(Job::new(serde_json::json!({})));
            bits.poll(&h.id, Some(Duration::from_secs(5))).await
        }));
    }

    // All 20 submit-then-poll cycles must complete without deadlock.
    let results = futures::future::join_all(handles).await;
    for r in results {
        let outcome = r.expect("task should not panic");
        assert!(
            matches!(outcome, PollOutcome::Ready(_)),
            "expected Ready, got {outcome:?}"
        );
    }
}

#[tokio::test]
async fn sweeper_removes_completed_job_after_reconnect_window() {
    let _ = common::TargetDummyDelay::new(0);

    let bits = Arc::new(Bits::from_config(fast_sweep_config()).unwrap());
    let handle = bits.submit(Job::new(serde_json::json!({})));

    // Wait long enough for the job to complete (~instant with duration_ms 0)
    // *and* for the 5-second reconnect buffer to expire, plus a comfortable
    // margin for at least several sweep cycles to run.
    tokio::time::sleep(Duration::from_millis(6_500)).await;

    // The sweeper should have removed the completed job by now because no
    // client ever polled (active_pollers stayed at 0) and the reconnect
    // deadline is well past.
    let outcome = bits.poll(&handle.id, None).await;
    assert!(
        matches!(outcome, PollOutcome::NotFound),
        "expected NotFound after sweep, got {outcome:?}"
    );
}

#[tokio::test]
async fn panicking_action_produces_failed_result_and_broker_continues() {
    let _ = common::TargetPanicking;
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
routes:
  - panicking:
      - target::panicking: ~
"#;
    let bits = Arc::new(Bits::from_config(config).unwrap());

    let handle = bits.submit(Job::new(serde_json::json!({})));
    let outcome = bits.poll(&handle.id, Some(Duration::from_secs(5))).await;
    assert!(
        matches!(outcome, PollOutcome::Ready(bits::JobResult::Failed { .. })),
        "panicking action should produce Failed, got {outcome:?}"
    );

    let handle2 = bits.submit(Job::new(serde_json::json!({})));
    let outcome2 = bits.poll(&handle2.id, Some(Duration::from_secs(5))).await;
    assert!(
        matches!(outcome2, PollOutcome::Ready(bits::JobResult::Failed { .. })),
        "same broker should still accept jobs after panic, got {outcome2:?}"
    );
}
