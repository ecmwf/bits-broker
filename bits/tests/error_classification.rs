mod common;

use bits::{Bits, BitsError, ConfigError, RoutingError};

fn must_fail(config: &str) -> BitsError {
    Bits::from_config(config)
        .err()
        .expect("expected from_config to fail")
}

#[test]
fn invalid_yaml_produces_config_yaml_variant() {
    let err = must_fail("{{{{not yaml");
    assert!(
        matches!(err, BitsError::Config(ConfigError::Yaml(_))),
        "expected Config(Yaml), got {err:?}"
    );
    assert_eq!(err.code(), "CONFIG_YAML_SYNTAX");
    assert!(!err.is_retryable());
}

#[test]
fn non_mapping_yaml_produces_config_validation() {
    let err = must_fail("just a string");
    assert!(
        matches!(err, BitsError::Config(ConfigError::Validation { .. })),
        "expected Config(Validation), got {err:?}"
    );
    assert_eq!(err.code(), "CONFIG_VALIDATION");
}

#[test]
fn empty_persistence_url_produces_config_validation_not_missing() {
    let config = r#"
bits:
  site: tst
  env: dev
  persistence:
    type: nats
    url: ""
routes:
  - default: []
"#;
    let err = must_fail(config);
    assert!(
        matches!(
            err,
            BitsError::Config(ConfigError::Validation { ref path, .. })
            if path.contains("url")
        ),
        "empty URL should produce Validation, not MissingField, got {err:?}"
    );
    assert_eq!(err.code(), "CONFIG_VALIDATION");
}

#[test]
#[cfg(not(feature = "tikv"))]
fn feature_disabled_produces_config_feature_disabled() {
    let config = r#"
bits:
  site: tst
  env: dev
  persistence:
    type: tikv
    endpoints:
      - 127.0.0.1:2379
routes:
  - default: []
"#;
    let err = must_fail(config);
    assert!(
        matches!(err, BitsError::Config(ConfigError::FeatureDisabled { .. })),
        "expected Config(FeatureDisabled), got {err:?}"
    );
    assert_eq!(err.code(), "CONFIG_FEATURE_DISABLED");
}

#[test]
fn unknown_action_produces_routing_invalid_action() {
    let config = r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - banana::something: ~
"#;
    let err = must_fail(config);
    assert!(
        matches!(err, BitsError::Routing(RoutingError::InvalidAction { .. })),
        "expected Routing(InvalidAction), got {err:?}"
    );
    assert_eq!(err.code(), "ROUTING_INVALID_ACTION");
    assert!(!err.is_retryable());
}

#[test]
fn route_without_target_is_rejected() {
    let _ = common::CheckDummyDelay::new(0);

    let config = r#"
bits:
  site: tst
  env: dev
routes:
  - no_target:
      - check::dummy_delay:
          duration_ms: 0
"#;
    let err = must_fail(config);
    assert!(
        matches!(err, BitsError::Routing(RoutingError::MissingTarget { .. })),
        "expected Routing(MissingTarget), got {err:?}"
    );
    assert_eq!(err.code(), "ROUTING_MISSING_TARGET");
    assert!(!err.is_retryable());
}

#[test]
fn invalid_persistence_ttl_produces_config_validation() {
    let config = r#"
bits:
  site: tst
  env: dev
  persistence:
    type: tikv
    endpoints:
      - 127.0.0.1:2379
    broker_lease_ttl_secs: 0.1
routes:
  - default: []
"#;
    let err = must_fail(config);
    assert!(
        matches!(
            err,
            BitsError::Config(ConfigError::Validation { ref path, .. })
            if path.contains("broker_lease_ttl_secs")
        ),
        "expected Config(Validation) for TTL, got {err:?}"
    );
    assert_eq!(err.code(), "CONFIG_VALIDATION");
}

#[test]
fn routes_as_map_produces_routing_error() {
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
bits:
  site: tst
  env: dev
routes:
  default:
    - target::dummy_dispatch:
        duration_ms: 0
        concurrency: 1
"#;
    let err = must_fail(config);
    assert!(
        matches!(err, BitsError::Routing(_)),
        "expected Routing error for non-array routes, got {err:?}"
    );
}

#[test]
fn unknown_server_field_is_rejected() {
    let config = r#"
bits:
  site: tst
  env: dev
server:
  host: "0.0.0.0"
  port: 8080
  poll_timeout_secss: 25.0
routes:
  - default: []
"#;
    let err = must_fail(config);
    let msg = err.to_string();
    assert!(
        msg.contains("unknown field"),
        "expected unknown field rejection for server typo, got: {msg}"
    );
}

#[test]
fn unknown_worker_server_field_is_rejected() {
    let config = r#"
bits:
  site: tst
  env: dev
  worker_server:
    host: "0.0.0.0"
    portt: 9001
routes:
  - default: []
"#;
    let err = must_fail(config);
    let msg = err.to_string();
    assert!(
        msg.contains("unknown field"),
        "expected unknown field rejection for worker_server typo, got: {msg}"
    );
}

#[test]
fn zero_sweep_interval_is_rejected() {
    let config = r#"
bits:
  site: tst
  env: dev
  sweep_interval_secs: 0.0
routes:
  - default: []
"#;
    let err = must_fail(config);
    let msg = err.to_string();
    assert!(
        msg.contains("must be greater than zero"),
        "expected zero rejection for sweep_interval_secs, got: {msg}"
    );
}

#[test]
fn negative_duration_is_rejected() {
    let config = r#"
bits:
  site: tst
  env: dev
  internal_poll_timeout_secs: -1.0
routes:
  - default: []
"#;
    let err = must_fail(config);
    let msg = err.to_string();
    assert!(
        msg.contains("internal_poll_timeout_secs"),
        "expected rejection for negative duration, got: {msg}"
    );
}

#[test]
fn zero_poll_timeout_is_rejected() {
    let config = r#"
bits:
  site: tst
  env: dev
server:
  poll_timeout_secs: 0.0
routes:
  - default: []
"#;
    let err = must_fail(config);
    let msg = err.to_string();
    assert!(
        msg.contains("must be greater than zero"),
        "expected zero rejection for poll_timeout_secs, got: {msg}"
    );
}

#[test]
fn persist_after_without_persistence_succeeds() {
    let config = r#"
bits:
  site: tst
  env: dev
  persist_after_secs: 5.0
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 10
          concurrency: 1
"#;
    let result = Bits::from_config(config);
    if let Err(e) = &result {
        panic!("persist_after_secs without persistence should succeed (warn + ignore), got: {e}");
    }
}

#[test]
fn config_errors_are_not_retryable() {
    let cases = [must_fail("{{{{"), must_fail("just a string")];
    for err in &cases {
        assert!(
            !err.is_retryable(),
            "{} should not be retryable",
            err.code()
        );
    }
}
