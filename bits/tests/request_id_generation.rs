mod common;

use std::collections::HashSet;

use bits::request_id::{self, DecodedId};
use bits::{Bits, Job};
use chrono::{DateTime, Duration, Utc};

const CROCKFORD_LOWER: &str = "0123456789abcdefghjkmnpqrstvwxyz";

fn config_with_site_env(site: &str, env: &str) -> String {
    format!(
        r#"
bits:
  site: {site}
  env: {env}
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 0
          concurrency: 1
"#
    )
}

fn assert_lowercase_crockford_id(id: &str) {
    assert_eq!(
        id.chars().count(),
        26,
        "request ID should be 26 Crockford characters: {id}"
    );
    assert!(
        id.chars().all(|ch| CROCKFORD_LOWER.contains(ch)),
        "request ID should contain only lower-case Crockford characters: {id}"
    );
}

fn custom_epoch() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(request_id::CUSTOM_EPOCH)
        .expect("custom epoch should parse")
        .with_timezone(&Utc)
}

fn decode_and_assert_generated_id(
    id: &str,
    site: &str,
    env: &str,
    broker_slot: u16,
    earliest: DateTime<Utc>,
    latest: DateTime<Utc>,
) -> DecodedId {
    assert_lowercase_crockford_id(id);
    let decoded = request_id::decode(id)
        .unwrap_or_else(|err| panic!("request ID should decode successfully: {id}: {err}"));

    assert_eq!(decoded.site, site);
    assert_eq!(decoded.env, env);
    assert_eq!(decoded.broker_slot, broker_slot);
    assert!(
        decoded.timestamp >= custom_epoch(),
        "timestamp should be after custom epoch: {}",
        decoded.timestamp
    );
    assert!(
        decoded.timestamp >= earliest - Duration::seconds(1),
        "timestamp {} should be within generation range starting at {}",
        decoded.timestamp,
        earliest
    );
    assert!(
        decoded.timestamp <= latest + Duration::seconds(1),
        "timestamp {} should be within generation range ending at {}",
        decoded.timestamp,
        latest
    );

    decoded
}

#[tokio::test]
async fn request_id_generation_broker_emits_configured_site_env_and_fresh_slot() {
    let _ = common::TargetDummyDelay::new(0);
    let site = "bol";
    let env = "dev";
    let bits = Bits::from_config(&config_with_site_env(site, env)).expect("broker should start");

    assert_eq!(bits.site(), site);
    assert_eq!(bits.env(), env);

    let earliest = Utc::now();
    let handles = (0..8)
        .map(|sequence| {
            bits.submit(Job::new(serde_json::json!({"sequence": sequence})))
                .expect_accepted("submit should not be rejected")
        })
        .collect::<Vec<_>>();
    let latest = Utc::now();

    let mut ids = HashSet::new();
    for handle in handles {
        assert!(
            ids.insert(handle.id.clone()),
            "generated request IDs should be unique across repeated submissions: {}",
            handle.id
        );
        decode_and_assert_generated_id(&handle.id, site, env, bits.broker_slot(), earliest, latest);
    }
}

#[tokio::test]
async fn request_id_generation_added_route_emits_configured_site_env_and_same_slot() {
    let _ = common::TargetDummyDelay::new(0);
    let site = "ams";
    let env = "prd";
    let bits = Bits::from_config(&config_with_site_env(site, env)).expect("broker should start");
    let route = serde_json::json!([{
        "added": [{
            "target::dummy_dispatch": {
                "duration_ms": 0,
                "concurrency": 1
            }
        }]
    }]);
    let added = bits
        .add_route("added", &route)
        .expect("added route should parse");

    let broker_reference = bits
        .submit(Job::new(serde_json::json!({"via": "broker"})))
        .expect_accepted("submit should not be rejected");
    let earliest = Utc::now();
    let handles = (0..8)
        .map(|sequence| added.submit(Job::new(serde_json::json!({"sequence": sequence}))))
        .collect::<Vec<_>>();
    let latest = Utc::now();

    let broker_decoded = decode_and_assert_generated_id(
        &broker_reference.id,
        site,
        env,
        bits.broker_slot(),
        earliest - Duration::seconds(1),
        latest,
    );
    assert_eq!(added.site(), site);
    assert_eq!(added.env(), env);
    assert_eq!(added.broker_slot(), broker_decoded.broker_slot);

    let mut ids = HashSet::from([broker_reference.id.clone()]);
    for handle in handles {
        assert!(
            ids.insert(handle.id.clone()),
            "added route generated request IDs should be unique: {}",
            handle.id
        );
        let decoded = decode_and_assert_generated_id(
            &handle.id,
            site,
            env,
            bits.broker_slot(),
            earliest,
            latest,
        );
        assert_eq!(
            decoded.broker_slot, broker_decoded.broker_slot,
            "added route should use the same allocated broker slot"
        );
    }
}
