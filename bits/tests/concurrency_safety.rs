mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bits::{Bits, Job, PollOutcome};

fn fast_sweep_config() -> &'static str {
    r#"
bits:
  job_cleanup_interval_ms: 50
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
  job_cleanup_interval_ms: 30000
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
