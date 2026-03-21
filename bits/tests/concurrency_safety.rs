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

#[tokio::test]
async fn completed_job_survives_sweeps_while_reconnect_window_active() {
    let _ = common::TargetDummyDelay::new(0);

    let bits = Arc::new(Bits::from_config(fast_sweep_config()).unwrap());
    let handle = bits.submit(Job::new(serde_json::json!({})));

    // Let several sweep cycles pass while the reconnect window (5s) is still active.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Job should still be available — sweeper must not remove it during the reconnect window.
    let outcome = bits.poll(&handle.id, Some(Duration::from_secs(2))).await;
    assert!(
        matches!(outcome, PollOutcome::Ready(_)),
        "expected Ready, got {outcome:?}"
    );
}

#[tokio::test]
async fn drop_completes_promptly_via_condvar_wakeup() {
    let _ = common::TargetDummyDelay::new(0);

    let bits = Bits::from_config(fast_sweep_config()).unwrap();
    let _ = bits.submit(Job::new(serde_json::json!({})));

    // Give the job time to complete and the sweeper/heartbeat threads to enter their sleep.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let start = Instant::now();
    drop(bits);
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_millis(500),
        "drop took {elapsed:?}; expected < 500ms (condvar should wake sleeping threads)"
    );
}
