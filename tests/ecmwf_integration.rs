use bits::{Bits, Job, JobResult};
use bits_ecmwf as _;
use serde_json::json;

#[tokio::test]
async fn test_inline_actions() {
    let config = r#"
routes:
  test_pipeline:
    - check::match:
        class: "od"
    - transform::metkit_expansion:
        expand_parameters: true
    - target::mars_destination:
        endpoint: "mars.example.com:8080"
"#;
    let bits = Bits::from_config(config).expect("Failed to parse config");
    let job = Job::new(json!({"class": "od"}));
    match bits.process(job).await {
        JobResult::Success { content_type, size, .. } => {
            assert_eq!(content_type, "application/json");
            assert_eq!(size, 55);
        }
        r => panic!("Expected success, got: {:?}", r),
    }
}

#[tokio::test]
async fn test_named_registries() {
    let config = r#"
checks:
  is_od:
    type: match
    class: "od"

targets:
  mars:
    type: mars_destination
    endpoint: "mars.example.com:8080"

routes:
  test_pipeline:
    - check::is_od
    - target::mars
"#;
    let bits = Bits::from_config(config).expect("Failed to parse config");
    let job = Job::new(json!({"class": "od"}));
    match bits.process(job).await {
        JobResult::Success { .. } => {}
        r => panic!("Expected success, got: {:?}", r),
    }
}

#[tokio::test]
async fn test_persist_sets_flag() {
    let config = r#"
routes:
  test_pipeline:
    - check::match:
        class: "od"
    - persist
    - target::mars_destination:
        endpoint: "mars.example.com:8080"
"#;
    let bits = Bits::from_config(config).expect("Failed to parse config");
    let job = Job::new(json!({"class": "od"}));
    match bits.process(job).await {
        JobResult::Success { .. } => {}
        r => panic!("Expected success, got: {:?}", r),
    }
}

#[tokio::test]
async fn test_nested_switch() {
    let config = r#"
routes:
  complex_pipeline:
    - check::match:
        class: "ea"
    - switch:
        privileged:
          - check::has_license:
              license: "era5"
          - target::mars_destination:
              endpoint: "mars.example.com:8080"
        public:
          - target::dss_destination:
              endpoint: "dss.example.com:9090"
"#;
    let bits = Bits::from_config(config).expect("Failed to parse config");
    let job = Job::new(json!({"class": "ea"}));
    match bits.process(job).await {
        JobResult::Success { content_type, .. } => {
            assert_eq!(content_type, "application/json");
        }
        r => panic!("Expected success, got: {:?}", r),
    }
}

#[test]
fn test_ecmwf_actions_registered() {
    let actions = bits::list_actions();
    assert!(actions.contains(&"match".to_string()));
    assert!(actions.contains(&"has_license".to_string()));
    assert!(actions.contains(&"mars_destination".to_string()));
    assert!(actions.contains(&"dss_destination".to_string()));
    assert!(actions.contains(&"metkit_expansion".to_string()));
}
