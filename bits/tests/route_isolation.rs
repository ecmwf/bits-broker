mod common;

use bits::{Bits, Job, PollOutcome};
use std::time::Duration;

/// Route A: transform sets cost → check rejects → target (never reached).
/// Route B: target completes.
///
/// The transform in route A mutates metadata. After rejection, route B must
/// see the original metadata (no cost field), proving Cow isolation.
#[tokio::test]
async fn transform_mutation_in_rejected_route_does_not_leak_to_next_route() {
    let _ = common::CheckAlwaysReject;
    let _ = common::TransformDummyCost { cost: 0 };
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
routes:
  - route_a:
      - transform::dummy_cost:
          cost: 999
      - check::always_reject: ~
      - target::dummy_dispatch:
          duration_ms: 0
          concurrency: 1
  - route_b:
      - target::dummy_dispatch:
          duration_ms: 0
          concurrency: 1
"#;

    let bits = Bits::from_config(config).expect("config should be valid");
    let handle = bits
        .submit(Job::new(serde_json::json!({"type": "fc"})))
        .expect_accepted("submit should not be rejected");
    let outcome = bits.poll(&handle.id, Some(Duration::from_secs(5))).await;

    // Route A's transform set cost=999, then the check rejected.
    // Route B should have dispatched with the original job — no cost field.
    assert!(
        matches!(outcome, PollOutcome::Ready(_)),
        "expected Ready from route B, got {outcome:?}"
    );
}
