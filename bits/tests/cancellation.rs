mod common;

use std::sync::Arc;
use std::time::Duration;

use bits::{Bits, Job, JobResult, PollOutcome};

#[tokio::test]
async fn cancel_stops_job_before_target() {
    let _ = common::CheckDummyDelay::new(100);
    let _ = common::TargetDummyDelay::new(0);

    // The check runs for 100ms, giving us a window to cancel before the target is reached.
    let config = r#"
routes:
  - default:
      - check::dummy_delay:
          duration_ms: 100
      - target::dummy_dispatch:
          duration_ms: 0
          concurrency: 1
"#;

    let bits = Arc::new(Bits::from_config(config).unwrap());
    let handle = bits.submit(Job::new(serde_json::json!({})));

    // Fire the cancel mid-check, while poll() is already waiting for the result.
    let bits_cancel = bits.clone();
    let cancel_id = handle.id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        bits_cancel.cancel(&cancel_id);
    });

    let outcome = bits.poll(&handle.id, Some(Duration::from_secs(2))).await;
    assert!(matches!(outcome, PollOutcome::Ready(JobResult::Cancelled)));
}

#[tokio::test]
async fn cancel_after_completion_has_no_effect() {
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 0
          concurrency: 1
"#;

    let bits = Arc::new(Bits::from_config(config).unwrap());
    let handle = bits.submit(Job::new(serde_json::json!({})));

    let outcome = bits.poll(&handle.id, Some(Duration::from_secs(2))).await;
    assert!(matches!(
        outcome,
        PollOutcome::Ready(JobResult::Redirect { .. })
    ));

    bits.cancel(&handle.id);

    let outcome = bits
        .poll(&handle.id, Some(Duration::from_millis(100)))
        .await;
    assert!(
        matches!(outcome, PollOutcome::NotFound),
        "job already consumed, expected NotFound, got {outcome:?}"
    );
}

#[tokio::test]
async fn double_cancel_does_not_panic() {
    let _ = common::CheckDummyDelay::new(200);
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
routes:
  - default:
      - check::dummy_delay:
          duration_ms: 200
      - target::dummy_dispatch:
          duration_ms: 0
          concurrency: 1
"#;

    let bits = Arc::new(Bits::from_config(config).unwrap());
    let handle = bits.submit(Job::new(serde_json::json!({})));

    bits.cancel(&handle.id);
    bits.cancel(&handle.id);

    let outcome = bits.poll(&handle.id, Some(Duration::from_secs(2))).await;
    assert!(matches!(outcome, PollOutcome::Ready(JobResult::Cancelled)));
}

#[tokio::test]
async fn cancel_nonexistent_job_does_not_panic() {
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 0
          concurrency: 1
"#;

    let bits = Bits::from_config(config).unwrap();
    bits.cancel("does-not-exist");
}
