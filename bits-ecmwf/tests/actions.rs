use std::fs;
use std::path::PathBuf;

use bits::actions::{CheckAction, CheckResult, TransformAction, TransformResult};
use bits::job::Job;
use bits_ecmwf::check::{DateChecker, Match};
use bits_ecmwf::transform_patch::PatchRequest;
use bits_ecmwf::schedule::{ScheduleCatalog, ScheduleReleased};
use bits_ecmwf::transform_request_coercion::RequestCoercion;
use chrono::{TimeZone, Utc};
use serde_json::json;

fn fixture_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "bits-ecmwf-{name}-{}-{}.xml",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    path
}

#[test]
fn request_coercion_deserialises_from_null() {
    let action: RequestCoercion = serde_json::from_value(serde_json::Value::Null).unwrap();
    assert_eq!(action.config.allow_ranges, bits_ecmwf::coercion::CoercionConfig::default().allow_ranges);
    assert_eq!(action.config.allow_lists, bits_ecmwf::coercion::CoercionConfig::default().allow_lists);
}

#[test]
fn request_coercion_deserialises_from_empty_object() {
    let action: RequestCoercion = serde_json::from_value(json!({})).unwrap();
    assert_eq!(action.config.allow_ranges, bits_ecmwf::coercion::CoercionConfig::default().allow_ranges);
}

#[tokio::test]
async fn request_coercion_normalises_mars_fields() {
    let mut job = Job::new(json!({
        "date": "2024-01-15",
        "time": 12,
        "step": 6,
        "expver": 7,
        "param": "2t/msl",
        "activity": "SCENARIOMIP"
    }));

    let transform = RequestCoercion::default();
    let result = transform.execute(&mut job).await.unwrap();
    assert!(matches!(result, TransformResult::Continue));
    assert_eq!(job.request["date"], "20240115");
    assert_eq!(job.request["time"], "1200");
    assert_eq!(job.request["step"], "6");
    assert_eq!(job.request["expver"], "0007");
    assert_eq!(job.request["activity"], "scenariomip");
    assert_eq!(job.request["param"], json!(["2t", "msl"]));
}

#[tokio::test]
async fn request_coercion_rejects_duplicate_lists() {
    let mut job = Job::new(json!({"param": "2t/2t"}));
    let transform = RequestCoercion::default();
    let err = transform.execute(&mut job).await.unwrap_err().to_string();
    assert!(err.contains("Duplicate values found in list"));
}

#[tokio::test]
async fn date_checker_accepts_old_dates_and_rejects_recent_dates() {
    let checker = DateChecker {
        key: "date".into(),
        allowed_values: vec![">2d".into()],
    };

    let old_date = (chrono::Local::now().date_naive() - chrono::Duration::days(5))
        .format("%Y%m%d")
        .to_string();
    let new_date = (chrono::Local::now().date_naive() - chrono::Duration::days(1))
        .format("%Y%m%d")
        .to_string();

    let pass = checker
        .evaluate(&Job::new(json!({"date": old_date})))
        .await
        .unwrap();
    assert!(matches!(pass, CheckResult::Pass));

    let reject = checker
        .evaluate(&Job::new(json!({"date": new_date})))
        .await
        .unwrap();
    assert!(matches!(reject, CheckResult::Reject { .. }));
}

#[tokio::test]
async fn schedule_catalog_checks_release_times() {
    let xml = r#"
garbage header
<schedule>
  <product>
    <class>od</class>
    <stream>oper</stream>
    <domain>g</domain>
    <time>12:00</time>
    <step>0006</step>
    <type>fc</type>
    <release_time>13:00:00</release_time>
    <release_delta_day>0</release_delta_day>
  </product>
</schedule>
"#;
    let catalog = ScheduleCatalog::from_raw_xml(xml).unwrap();
    let request = json!({
        "class": "od",
        "stream": "oper",
        "domain": "g",
        "time": "1200",
        "step": "6",
        "type": "fc",
        "date": "20240115"
    });

    let before_release = Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 0).unwrap();
    let after_release = Utc.with_ymd_and_hms(2024, 1, 15, 13, 30, 0).unwrap();

    let err = catalog
        .assert_request_released(&request, before_release)
        .unwrap_err()
        .to_string();
    assert!(err.contains("Data not released yet"));

    catalog
        .assert_request_released(&request, after_release)
        .unwrap();
}

#[tokio::test]
async fn schedule_released_action_reads_raw_xml_file() {
    let path = fixture_path("schedule");
    fs::write(
        &path,
        r#"junk
<schedule>
  <product>
    <class>od</class>
    <stream>oper</stream>
    <domain>g</domain>
    <time>12:00</time>
    <step>0006</step>
    <type>fc</type>
    <release_time>13:00:00</release_time>
    <release_delta_day>0</release_delta_day>
  </product>
</schedule>
"#,
    )
    .unwrap();

    let action = ScheduleReleased {
        path: path.display().to_string(),
        now_rfc3339: Some("2024-01-15T14:00:00Z".into()),
    };
    let result = action
        .evaluate(&Job::new(json!({
            "class": "od",
            "stream": "oper",
            "domain": "g",
            "time": "1200",
            "step": "6",
            "type": "fc",
            "date": "20240115"
        })))
        .await
        .unwrap();

    fs::remove_file(path).ok();
    assert!(matches!(result, CheckResult::Pass));
}

#[tokio::test]
async fn match_single_field() {
    let action: Match = serde_json::from_value(json!({"class": "od"})).unwrap();
    let pass = action
        .evaluate(&Job::new(json!({"class": "od", "stream": "oper"})))
        .await
        .unwrap();
    assert!(matches!(pass, CheckResult::Pass));

    let reject = action
        .evaluate(&Job::new(json!({"class": "ea"})))
        .await
        .unwrap();
    assert!(matches!(reject, CheckResult::Reject { .. }));
}

#[tokio::test]
async fn match_multiple_fields() {
    let action: Match =
        serde_json::from_value(json!({"class": "od", "stream": "oper"})).unwrap();

    let pass = action
        .evaluate(&Job::new(json!({"class": "od", "stream": "oper", "type": "fc"})))
        .await
        .unwrap();
    assert!(matches!(pass, CheckResult::Pass));

    let reject = action
        .evaluate(&Job::new(json!({"class": "od", "stream": "enfo"})))
        .await
        .unwrap();
    assert!(matches!(reject, CheckResult::Reject { .. }));
}

#[tokio::test]
async fn match_rejects_missing_key() {
    let action: Match = serde_json::from_value(json!({"domain": "g"})).unwrap();
    let reject = action
        .evaluate(&Job::new(json!({"class": "od"})))
        .await
        .unwrap();
    assert!(matches!(reject, CheckResult::Reject { .. }));
}

#[tokio::test]
async fn patch_request_sets_fields() {
    let action: PatchRequest =
        serde_json::from_value(json!({"set": {"domain": "g", "class": "od"}})).unwrap();
    let mut job = Job::new(json!({"param": "2t"}));
    action.execute(&mut job).await.unwrap();
    assert_eq!(job.request["domain"], "g");
    assert_eq!(job.request["class"], "od");
    assert_eq!(job.request["param"], "2t");
}

#[tokio::test]
async fn patch_request_overwrites_existing() {
    let action: PatchRequest =
        serde_json::from_value(json!({"set": {"domain": "g"}})).unwrap();
    let mut job = Job::new(json!({"domain": "m"}));
    action.execute(&mut job).await.unwrap();
    assert_eq!(job.request["domain"], "g");
}
