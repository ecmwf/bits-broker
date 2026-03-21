mod common;

use std::sync::Arc;
use std::time::Duration;

use bits::{Bits, Job, JobResult, PollOutcome};

fn default_config() -> &'static str {
    r#"
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 0
          concurrency: 1
"#
}

fn slow_config() -> &'static str {
    r#"
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 200
          concurrency: 1
"#
}

#[tokio::test]
async fn poll_nonexistent_id_returns_not_found() {
    let _ = common::TargetDummyDelay::new(0);
    let bits = Bits::from_config(default_config()).unwrap();

    let outcome = bits
        .poll("no-such-job", Some(Duration::from_millis(50)))
        .await;
    assert!(matches!(outcome, PollOutcome::NotFound));
}

#[tokio::test]
async fn poll_with_timeout_returns_pending_then_ready() {
    let _ = common::TargetDummyDelay::new(0);
    let bits = Bits::from_config(slow_config()).unwrap();

    let handle = bits.submit(Job::new(serde_json::json!({})));

    let outcome = bits.poll(&handle.id, Some(Duration::from_millis(50))).await;
    assert!(
        matches!(outcome, PollOutcome::Pending { .. }),
        "expected Pending on short timeout, got {outcome:?}"
    );

    let outcome = bits.poll(&handle.id, Some(Duration::from_secs(5))).await;
    assert!(
        matches!(outcome, PollOutcome::Ready(JobResult::Redirect { .. })),
        "expected Ready(Redirect) on long timeout, got {outcome:?}"
    );
}

#[tokio::test]
async fn poll_after_result_consumed_returns_not_found() {
    let _ = common::TargetDummyDelay::new(0);
    let bits = Bits::from_config(default_config()).unwrap();

    let handle = bits.submit(Job::new(serde_json::json!({})));

    let outcome = bits.poll(&handle.id, Some(Duration::from_secs(2))).await;
    assert!(
        matches!(outcome, PollOutcome::Ready(JobResult::Redirect { .. })),
        "expected Ready(Redirect), got {outcome:?}"
    );

    let outcome = bits
        .poll(&handle.id, Some(Duration::from_millis(100)))
        .await;
    assert!(
        matches!(outcome, PollOutcome::NotFound),
        "result already consumed, expected NotFound, got {outcome:?}"
    );
}

#[tokio::test]
async fn many_concurrent_submits_all_complete() {
    let _ = common::TargetDummyDelay::new(0);
    let bits = Arc::new(Bits::from_config(default_config()).unwrap());

    let handles: Vec<_> = (0..50)
        .map(|i| {
            let bits = bits.clone();
            tokio::spawn(async move {
                let h = bits.submit(Job::new(serde_json::json!({"i": i})));
                bits.poll(&h.id, Some(Duration::from_secs(5))).await
            })
        })
        .collect();

    let results = futures::future::join_all(handles).await;
    for (i, result) in results.into_iter().enumerate() {
        let outcome = result.expect("task should not panic");
        assert!(
            matches!(outcome, PollOutcome::Ready(JobResult::Redirect { .. })),
            "job {i} expected Ready(Redirect), got {outcome:?}"
        );
    }
}

#[tokio::test]
async fn submit_returns_unique_ids() {
    let _ = common::TargetDummyDelay::new(0);
    let bits = Bits::from_config(default_config()).unwrap();

    let h1 = bits.submit(Job::new(serde_json::json!({})));
    let h2 = bits.submit(Job::new(serde_json::json!({})));
    let h3 = bits.submit(Job::new(serde_json::json!({})));

    assert_ne!(h1.id, h2.id);
    assert_ne!(h2.id, h3.id);
    assert_ne!(h1.id, h3.id);
}
