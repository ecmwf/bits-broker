use bits::dispatcher::{ExecutorKind, QueueKind, RemotePoolConfig};
use bits::Bits;

#[test]
fn executor_kind_deserializes_async_pool() {
    let kind: ExecutorKind = serde_yaml::from_str("async_pool").unwrap();
    assert!(matches!(kind, ExecutorKind::AsyncPool));
}

#[test]
fn queue_kind_deserializes_age_priority() {
    let kind: QueueKind = serde_yaml::from_str("age_priority").unwrap();
    assert!(matches!(kind, QueueKind::AgePriority));
}

#[test]
fn remote_pool_config_defaults_apply() {
    let cfg: RemotePoolConfig = serde_yaml::from_str("{}").unwrap();
    assert_eq!(cfg.bind, "0.0.0.0:9001");
    assert_eq!(cfg.heartbeat_timeout_secs, 60.0);
}

#[tokio::test]
async fn remote_target_without_executor_injects_default_remote_pool() {
    let config = r#"
routes:
  default:
    - target::remote: ~
"#;

    let bits = Bits::from_config(config);
    assert!(bits.is_ok(), "target::remote without executor should parse successfully");
}

#[test]
fn remote_pool_rejected_for_non_remote_target() {
    let config = r#"
routes:
  default:
    - target::http:
        url: "http://example.com"
      dispatcher:
        executor:
          remote_pool: {}
"#;

    let err = Bits::from_config(config).err().unwrap().to_string();
    assert!(
        err.contains("executor: remote_pool requires a 'remote' target action"),
        "unexpected error: {err}"
    );
}

#[test]
fn remote_target_rejected_with_non_remote_executor() {
    let config = r#"
routes:
  default:
    - target::remote: ~
      dispatcher:
        executor: thread_pool
"#;

    let err = Bits::from_config(config).err().unwrap().to_string();
    assert!(
        err.contains("'remote' target requires executor: remote_pool"),
        "unexpected error: {err}"
    );
}
