mod common;

use bits::Bits;
use bits::dispatcher::{ExecutorKind, QueueKind, RemotePoolConfig};

#[test]
fn executor_kind_deserializes_async_pool() {
    let kind: ExecutorKind = serde_yaml::from_str("type: async_pool").unwrap();
    assert!(matches!(
        kind,
        ExecutorKind::AsyncPool { concurrency: None }
    ));
}

#[test]
fn executor_kind_deserializes_async_pool_with_concurrency() {
    let kind: ExecutorKind = serde_yaml::from_str("type: async_pool\nconcurrency: 4").unwrap();
    assert!(matches!(
        kind,
        ExecutorKind::AsyncPool {
            concurrency: Some(4)
        }
    ));
}

#[test]
fn executor_kind_deserializes_thread_pool_with_concurrency() {
    let kind: ExecutorKind = serde_yaml::from_str("type: thread_pool\nconcurrency: 8").unwrap();
    assert!(matches!(
        kind,
        ExecutorKind::ThreadPool {
            concurrency: Some(8)
        }
    ));
}

#[test]
fn queue_kind_deserializes_age_priority() {
    let kind: QueueKind = serde_yaml::from_str("age_priority").unwrap();
    assert!(matches!(kind, QueueKind::AgePriority));
}

#[test]
fn remote_pool_config_defaults_apply() {
    let cfg: RemotePoolConfig = serde_yaml::from_str("{}").unwrap();
    assert_eq!(cfg.heartbeat_timeout_secs, 60.0);
}

#[test]
fn remote_pool_config_bind_field_is_rejected() {
    let result: Result<RemotePoolConfig, _> = serde_yaml::from_str("bind: \"0.0.0.0:9001\"");
    let err = result.err().unwrap().to_string();
    assert!(
        err.contains("bits.worker_server"),
        "unexpected error: {err}"
    );
}

#[test]
fn remote_pool_config_accepts_heartbeat_timeout() {
    let cfg: RemotePoolConfig = serde_yaml::from_str("heartbeat_timeout_secs: 30.5").unwrap();
    assert_eq!(cfg.heartbeat_timeout_secs, 30.5);
}

#[tokio::test]
async fn named_remote_target_without_executor_injects_default_remote_pool() {
    let config = r#"
bits:
  worker_server:
    host: "127.0.0.1"
    port: 0
targets:
  my_pool:
    type: remote
routes:
  - default:
      - target::my_pool
"#;

    let bits = Bits::from_config(config);
    assert!(
        bits.is_ok(),
        "named remote target without executor should parse successfully"
    );
}

#[tokio::test]
async fn named_remote_target_with_worker_server_parses() {
    let config = r#"
bits:
  worker_server:
    host: "127.0.0.1"
    port: 0
targets:
  mars:
    type: remote
    dispatcher:
      executor:
        type: remote_pool
routes:
  - default:
      - target::mars
"#;
    assert!(
        Bits::from_config(config).is_ok(),
        "valid config should parse"
    );
}

#[test]
fn inline_remote_target_is_rejected() {
    let config = r#"
bits:
  worker_server:
    host: "127.0.0.1"
    port: 0
routes:
  - default:
      - target::remote: ~
"#;
    let err = Bits::from_config(config).err().unwrap().to_string();
    assert!(err.contains("named registry entry"), "unexpected: {err}");
}

#[test]
fn remote_target_without_worker_server_is_rejected() {
    let config = r#"
targets:
  mars:
    type: remote
routes:
  - default:
      - target::mars
"#;
    let err = Bits::from_config(config).err().unwrap().to_string();
    assert!(err.contains("bits.worker_server"), "unexpected: {err}");
}

#[test]
fn remote_pool_rejected_for_non_remote_target() {
    let config = r#"
routes:
  - default:
      - target::http:
          url: "http://example.com"
        dispatcher:
          executor:
            type: remote_pool
"#;

    let err = Bits::from_config(config).err().unwrap().to_string();
    assert!(
        err.contains("executor: remote_pool requires a 'remote' target action"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn remote_pool_rejects_zero_heartbeat_timeout() {
    let config = r#"
bits:
  worker_server:
    host: "127.0.0.1"
    port: 0
targets:
  pool:
    type: remote
    dispatcher:
      executor:
        type: remote_pool
        heartbeat_timeout_secs: 0
routes:
  - default:
      - target::pool
"#;
    let err = Bits::from_config(config).err().unwrap().to_string();
    assert!(
        err.contains("heartbeat_timeout_secs"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn remote_pool_rejects_negative_heartbeat_timeout() {
    let config = r#"
bits:
  worker_server:
    host: "127.0.0.1"
    port: 0
targets:
  pool:
    type: remote
    dispatcher:
      executor:
        type: remote_pool
        heartbeat_timeout_secs: -5.0
routes:
  - default:
      - target::pool
"#;
    let err = Bits::from_config(config).err().unwrap().to_string();
    assert!(
        err.contains("heartbeat_timeout_secs"),
        "unexpected error: {err}"
    );
}

#[test]
fn remote_target_rejected_with_non_remote_executor() {
    let config = r#"
bits:
  worker_server:
    host: "127.0.0.1"
    port: 0
targets:
  mars:
    type: remote
    dispatcher:
      executor:
        type: thread_pool
routes:
  - default:
      - target::mars
"#;

    let err = Bits::from_config(config).err().unwrap().to_string();
    assert!(
        err.contains("'remote' target requires executor: remote_pool"),
        "unexpected error: {err}"
    );
}

#[test]
fn concurrency_at_dispatcher_level_is_rejected() {
    let config = r#"
routes:
  - default:
      - target::http:
          url: "http://example.com"
        dispatcher:
          concurrency: 4
"#;

    let err = Bits::from_config(config).err().unwrap().to_string();
    assert!(
        err.contains("dispatcher.concurrency removed"),
        "unexpected error: {err}"
    );
}

#[test]
fn empty_route_is_rejected() {
    let config = r#"
routes:
  - default: []
"#;

    let err = Bits::from_config(config).err().unwrap().to_string();
    assert!(
        err.contains("route 'default' must not be empty"),
        "unexpected error: {err}"
    );
}

#[test]
fn route_must_end_in_terminal_action() {
    let _ = common::CheckDummyDelay::new(1);

    let config = r#"
routes:
  - default:
      - check::dummy_delay:
          duration_ms: 1
"#;

    let err = Bits::from_config(config).err().unwrap().to_string();
    assert!(
        err.contains("route 'default' must end with a target or switch"),
        "unexpected error: {err}"
    );
}

#[test]
fn action_after_target_is_rejected() {
    let config = r#"
routes:
  - default:
      - target::http:
          url: "http://example.com"
      - target::http:
          url: "http://example.com/after"
"#;

    let err = Bits::from_config(config).err().unwrap().to_string();
    assert!(
        err.contains("route 'default' has unreachable action(s) after terminal step at index 0"),
        "unexpected error: {err}"
    );
}

#[test]
fn action_after_switch_is_rejected() {
    let config = r#"
routes:
  - default:
      - switch:
          - nested:
              - target::http:
                  url: "http://example.com"
      - target::http:
          url: "http://example.com/after"
"#;

    let err = Bits::from_config(config).err().unwrap().to_string();
    assert!(
        err.contains("route 'default' has unreachable action(s) after terminal step at index 0"),
        "unexpected error: {err}"
    );
}

#[test]
fn nested_switch_routes_are_validated_recursively() {
    let _ = common::CheckDummyDelay::new(1);

    let config = r#"
routes:
  - default:
      - switch:
          - nested:
              - check::dummy_delay:
                  duration_ms: 1
"#;

    let err = Bits::from_config(config).err().unwrap().to_string();
    assert!(
        err.contains("route 'nested' must end with a target or switch"),
        "unexpected error: {err}"
    );
}

#[test]
fn completely_invalid_yaml_is_rejected() {
    assert!(Bits::from_config("{{{{not yaml at all").is_err());
}

#[test]
fn non_mapping_yaml_is_rejected() {
    assert!(Bits::from_config("just a plain string").is_err());
}

#[test]
fn missing_routes_section_is_rejected() {
    let config = r#"
bits:
  job_cleanup_interval_ms: 100
"#;
    let bits = Bits::from_config(config);
    assert!(
        bits.is_ok(),
        "missing routes section should now be accepted: {:?}",
        bits.err()
    );
}

#[test]
fn unknown_action_namespace_is_rejected() {
    let config = r#"
routes:
  - default:
      - banana::something: ~
"#;
    let err = Bits::from_config(config).err().unwrap().to_string();
    assert!(
        err.to_lowercase().contains("unknown"),
        "expected unknown namespace error, got: {err}"
    );
}

#[test]
fn unknown_action_name_is_rejected() {
    let config = r#"
routes:
  - default:
      - target::this_target_does_not_exist: ~
"#;
    let err = Bits::from_config(config).err().unwrap().to_string();
    assert!(
        err.to_lowercase().contains("unknown"),
        "expected unknown action error, got: {err}"
    );
}

#[test]
fn routes_must_be_array() {
    let config = r#"
routes:
  default:
    - target::http:
        url: "http://example.com"
"#;
    let err = Bits::from_config(config).err().unwrap().to_string();
    assert!(err.contains("array"), "expected array error, got: {err}");
}
