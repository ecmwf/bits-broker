mod common;

use bits::{Action, Bits, Job, PollOutcome};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn multi_route_submit_and_poll() {
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
bits:
  site: tst
  env: dev
targets:
  fast_target:
    type: dummy_dispatch
    duration_ms: 0
"#;

    let bits = Bits::from_config(config).expect("config should be valid");

    let route_a_val = serde_json::json!([{"route_a": ["target::fast_target"]}]);
    let route_b_val = serde_json::json!([{"route_b": ["target::fast_target"]}]);

    let handle_a = bits
        .add_route("route_a", &route_a_val)
        .expect("add route_a");
    let handle_b = bits
        .add_route("route_b", &route_b_val)
        .expect("add route_b");

    let job_a = handle_a.submit(Job::new(serde_json::json!({"route": "a"})));
    let job_b = handle_b.submit(Job::new(serde_json::json!({"route": "b"})));

    let outcome_a = bits.poll(&job_a.id, Some(Duration::from_secs(5))).await;
    let outcome_b = bits.poll(&job_b.id, Some(Duration::from_secs(5))).await;

    assert!(
        matches!(outcome_a, PollOutcome::Ready(_)),
        "expected Ready from route_a, got {outcome_a:?}"
    );
    assert!(
        matches!(outcome_b, PollOutcome::Ready(_)),
        "expected Ready from route_b, got {outcome_b:?}"
    );
}

#[tokio::test]
async fn multi_route_shared_target_arc() {
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
bits:
  site: tst
  env: dev
targets:
  shared_target:
    type: dummy_dispatch
    duration_ms: 0
"#;

    let bits = Bits::from_config(config).expect("config should be valid");

    let route_a_val = serde_json::json!([{"route_a": ["target::shared_target"]}]);
    let route_b_val = serde_json::json!([{"route_b": ["target::shared_target"]}]);

    let factory = bits.route_factory();
    let routes_a = factory
        .parse_route("route_a", &route_a_val)
        .expect("parse route_a");
    let routes_b = factory
        .parse_route("route_b", &route_b_val)
        .expect("parse route_b");

    let target_a = match routes_a[0].actions.first() {
        Some(Action::Target(t, _, _, _)) => Arc::clone(t),
        _ => panic!("expected Target action in route_a"),
    };
    let target_b = match routes_b[0].actions.first() {
        Some(Action::Target(t, _, _, _)) => Arc::clone(t),
        _ => panic!("expected Target action in route_b"),
    };

    assert!(
        Arc::ptr_eq(&target_a, &target_b),
        "routes referencing the same target should share the same Arc"
    );
}

#[tokio::test]
async fn added_route_names_returns_collection_names() {
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
bits:
  site: tst
  env: dev
targets:
  my_target:
    type: dummy_dispatch
    duration_ms: 0
"#;

    let bits = Bits::from_config(config).expect("config should be valid");

    let route_val = serde_json::json!([{"my_route": ["target::my_target"]}]);
    bits.add_route("ecmwf", &route_val).expect("add ecmwf");
    bits.add_route("opendata", &route_val)
        .expect("add opendata");

    let mut names = bits.added_route_names();
    names.sort();
    assert_eq!(names, vec!["ecmwf", "opendata"]);
}

#[tokio::test]
async fn add_route_unknown_target_returns_error() {
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
bits:
  site: tst
  env: dev
targets:
  my_target:
    type: dummy_dispatch
    duration_ms: 0
"#;

    let bits = Bits::from_config(config).expect("config should be valid");

    let bad_route_val = serde_json::json!([{"bad_route": ["target::nonexistent_target"]}]);
    let result = bits.add_route("bad_route", &bad_route_val);

    assert!(
        result.is_err(),
        "add_route with unknown target should return error"
    );
    let err_msg = result.err().unwrap().to_string();
    assert!(
        err_msg.contains("nonexistent_target"),
        "error message should mention the unknown target name, got: {err_msg}"
    );
}
