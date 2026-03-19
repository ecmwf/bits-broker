use bits::{Bits, Job, JobResult, PollOutcome};
use reqwest::Client;
use std::sync::OnceLock;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct MatchClass {
    class: String,
}

#[async_trait]
impl bits::CheckAction for MatchClass {
    async fn evaluate(&self, job: &Job) -> Result<bits::CheckResult, bits::ActionError> {
        match job.request.get("class").and_then(|v| v.as_str()) {
            Some(v) if v == self.class => Ok(bits::CheckResult::Pass),
            _ => Ok(bits::CheckResult::Reject {
                reason: format!("class {} not matched", self.class),
            }),
        }
    }
}

fn ensure_match_class_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        bits::register_runtime_action(
            "test_match_class",
            std::sync::Arc::new(|config| {
                let action: MatchClass = serde_json::from_value(config)
                    .map_err(|e| bits::ActionError::ConfigError(e.to_string()))?;
                Ok(bits::Action::Check(std::sync::Arc::new(action), None))
            }),
        )
        .expect("register test_match_class");
    });
}

async fn free_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().port()
}

async fn wait_for_pool(port: u16, pool: &str) {
    let client = Client::new();
    for _ in 0..100 {
        if client
            .get(format!("http://127.0.0.1:{port}/{pool}/work?timeout_ms=0"))
            .send()
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("pool {pool} on port {port} never ready");
}

fn make_two_pool_bits(port: u16, queue_a: &str, queue_b: &str, hb_a: f64, hb_b: f64) -> Bits {
    ensure_match_class_registered();
    let config = format!(
        r#"
bits:
  worker_server:
    host: "127.0.0.1"
    port: {port}
targets:
  pool_a:
    type: remote
    dispatcher:
      queue: {queue_a}
      executor:
        type: remote_pool
        heartbeat_timeout_secs: {hb_a}
  pool_b:
    type: remote
    dispatcher:
      queue: {queue_b}
      executor:
        type: remote_pool
        heartbeat_timeout_secs: {hb_b}
routes:
  - default:
      - switch:
          - for_a:
              - check::test_match_class:
                  class: a
              - target::pool_a
          - for_b:
              - check::test_match_class:
                  class: b
              - target::pool_b
"#
    );
    Bits::from_config(&config).expect("config error")
}

fn make_dedup_pool_bits(port: u16) -> Bits {
    let config = format!(
        r#"
bits:
  worker_server:
    host: "127.0.0.1"
    port: {port}
targets:
  pool_a:
    type: remote
routes:
  - route1:
      - target::pool_a
  - route2:
      - target::pool_a
"#
    );
    Bits::from_config(&config).expect("config error")
}

#[tokio::test]
async fn two_pools_jobs_are_isolated() {
    let port = free_port().await;
    let bits = make_two_pool_bits(port, "fifo", "fifo", 60.0, 60.0);
    wait_for_pool(port, "pool_a").await;
    wait_for_pool(port, "pool_b").await;

    let handle = bits.submit(Job::new(serde_json::json!({"class": "b", "k": 1})));
    let client = Client::new();

    let empty_a = client
        .get(format!("http://127.0.0.1:{port}/pool_a/work?timeout_ms=50"))
        .send()
        .await
        .unwrap();
    assert_eq!(empty_a.status(), 204);

    let work_b = client
        .get(format!(
            "http://127.0.0.1:{port}/pool_b/work?timeout_ms=5000"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(work_b.status(), 200);
    let work: serde_json::Value = work_b.json().await.unwrap();
    assert_eq!(work["request"]["class"], "b");

    let job_id = work["job_id"].as_str().unwrap();
    let done = client
        .post(format!(
            "http://127.0.0.1:{port}/pool_b/complete/data/{job_id}"
        ))
        .header("content-type", "application/json")
        .body(r#"{"ok": true}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(done.status(), 200);

    match bits.poll(&handle.id, Some(Duration::from_secs(5))).await {
        PollOutcome::Ready(JobResult::Success { content_type, .. }) => {
            assert_eq!(content_type, "application/json")
        }
        other => panic!("expected Success, got {:?}", other),
    }
}

#[tokio::test]
async fn both_pools_concurrent() {
    let port = free_port().await;
    let bits = make_two_pool_bits(port, "fifo", "fifo", 60.0, 60.0);
    wait_for_pool(port, "pool_a").await;
    wait_for_pool(port, "pool_b").await;

    let h_a = bits.submit(Job::new(serde_json::json!({"class": "a", "name": "ja"})));
    let h_b = bits.submit(Job::new(serde_json::json!({"class": "b", "name": "jb"})));
    let client = Client::new();

    let (r_a, r_b) = tokio::join!(
        client
            .get(format!(
                "http://127.0.0.1:{port}/pool_a/work?timeout_ms=5000"
            ))
            .send(),
        client
            .get(format!(
                "http://127.0.0.1:{port}/pool_b/work?timeout_ms=5000"
            ))
            .send(),
    );

    let r_a = r_a.unwrap();
    let r_b = r_b.unwrap();
    assert_eq!(r_a.status(), 200);
    assert_eq!(r_b.status(), 200);

    let w_a: serde_json::Value = r_a.json().await.unwrap();
    let w_b: serde_json::Value = r_b.json().await.unwrap();
    assert_eq!(w_a["request"]["class"], "a");
    assert_eq!(w_b["request"]["class"], "b");

    let id_a = w_a["job_id"].as_str().unwrap();
    let id_b = w_b["job_id"].as_str().unwrap();
    assert_ne!(id_a, id_b);

    let done_a = client
        .post(format!(
            "http://127.0.0.1:{port}/pool_a/complete/reject/{id_a}"
        ))
        .json(&serde_json::json!({"reason": "done a"}))
        .send()
        .await
        .unwrap();
    let done_b = client
        .post(format!(
            "http://127.0.0.1:{port}/pool_b/complete/reject/{id_b}"
        ))
        .json(&serde_json::json!({"reason": "done b"}))
        .send()
        .await
        .unwrap();
    assert_eq!(done_a.status(), 200);
    assert_eq!(done_b.status(), 200);

    assert!(matches!(
        bits.poll(&h_a.id, Some(Duration::from_secs(5))).await,
        PollOutcome::Ready(JobResult::Error { .. })
    ));
    assert!(matches!(
        bits.poll(&h_b.id, Some(Duration::from_secs(5))).await,
        PollOutcome::Ready(JobResult::Error { .. })
    ));
}

#[tokio::test]
async fn per_pool_queue_policy() {
    let port = free_port().await;
    let bits = make_two_pool_bits(port, "cost_weighted", "fifo", 60.0, 60.0);
    wait_for_pool(port, "pool_a").await;
    wait_for_pool(port, "pool_b").await;

    let mut a_expensive = Job::new(serde_json::json!({"class": "a", "label": "a_expensive"}));
    a_expensive.metadata["cost"] = serde_json::json!(100u64);
    let h_a_expensive = bits.submit(a_expensive);

    let mut a_cheap = Job::new(serde_json::json!({"class": "a", "label": "a_cheap"}));
    a_cheap.metadata["cost"] = serde_json::json!(1u64);
    let h_a_cheap = bits.submit(a_cheap);

    let mut b_expensive = Job::new(serde_json::json!({"class": "b", "label": "b_expensive"}));
    b_expensive.metadata["cost"] = serde_json::json!(100u64);
    let h_b_expensive = bits.submit(b_expensive);

    let mut b_cheap = Job::new(serde_json::json!({"class": "b", "label": "b_cheap"}));
    b_cheap.metadata["cost"] = serde_json::json!(1u64);
    let h_b_cheap = bits.submit(b_cheap);

    let client = Client::new();

    let a_first: serde_json::Value = client
        .get(format!(
            "http://127.0.0.1:{port}/pool_a/work?timeout_ms=5000"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let a_second: serde_json::Value = client
        .get(format!(
            "http://127.0.0.1:{port}/pool_a/work?timeout_ms=5000"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(a_first["request"]["label"], "a_cheap");
    assert_eq!(a_second["request"]["label"], "a_expensive");

    let b_first: serde_json::Value = client
        .get(format!(
            "http://127.0.0.1:{port}/pool_b/work?timeout_ms=5000"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let b_second: serde_json::Value = client
        .get(format!(
            "http://127.0.0.1:{port}/pool_b/work?timeout_ms=5000"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(b_first["request"]["label"], "b_expensive");
    assert_eq!(b_second["request"]["label"], "b_cheap");

    for (pool, id) in [
        ("pool_a", a_first["job_id"].as_str().unwrap()),
        ("pool_a", a_second["job_id"].as_str().unwrap()),
        ("pool_b", b_first["job_id"].as_str().unwrap()),
        ("pool_b", b_second["job_id"].as_str().unwrap()),
    ] {
        let resp = client
            .post(format!(
                "http://127.0.0.1:{port}/{pool}/complete/reject/{id}"
            ))
            .json(&serde_json::json!({"reason": "done"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    for handle in [&h_a_expensive, &h_a_cheap, &h_b_expensive, &h_b_cheap] {
        assert!(matches!(
            bits.poll(&handle.id, Some(Duration::from_secs(5))).await,
            PollOutcome::Ready(JobResult::Error { .. })
        ));
    }
}

#[tokio::test]
async fn per_pool_heartbeat_timeout() {
    let port = free_port().await;
    let bits = make_two_pool_bits(port, "fifo", "fifo", 0.1, 60.0);
    wait_for_pool(port, "pool_a").await;
    wait_for_pool(port, "pool_b").await;

    let h_a = bits.submit(Job::new(
        serde_json::json!({"class": "a", "job": "timeout"}),
    ));
    let h_b = bits.submit(Job::new(serde_json::json!({"class": "b", "job": "alive"})));
    let client = Client::new();

    let w_a: serde_json::Value = client
        .get(format!(
            "http://127.0.0.1:{port}/pool_a/work?timeout_ms=5000"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let w_b: serde_json::Value = client
        .get(format!(
            "http://127.0.0.1:{port}/pool_b/work?timeout_ms=5000"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    assert!(matches!(
        bits.poll(&h_a.id, Some(Duration::from_secs(1))).await,
        PollOutcome::Ready(JobResult::Failed { .. })
    ));
    assert!(matches!(
        bits.poll(&h_b.id, Some(Duration::from_millis(100))).await,
        PollOutcome::Pending { .. }
    ));

    let id_b = w_b["job_id"].as_str().unwrap();
    let done_b = client
        .post(format!(
            "http://127.0.0.1:{port}/pool_b/complete/reject/{id_b}"
        ))
        .json(&serde_json::json!({"reason": "cleanup"}))
        .send()
        .await
        .unwrap();
    assert_eq!(done_b.status(), 200);

    assert!(matches!(
        bits.poll(&h_b.id, Some(Duration::from_secs(5))).await,
        PollOutcome::Ready(JobResult::Error { .. })
    ));

    let _ = w_a;
}

#[tokio::test]
async fn flat_paths_return_404() {
    let port = free_port().await;
    let _bits = make_two_pool_bits(port, "fifo", "fifo", 60.0, 60.0);
    wait_for_pool(port, "pool_a").await;
    wait_for_pool(port, "pool_b").await;

    let client = Client::new();

    let flat_work = client
        .get(format!("http://127.0.0.1:{port}/work?timeout_ms=50"))
        .send()
        .await
        .unwrap();
    assert_eq!(flat_work.status(), 404);

    let flat_hb = client
        .post(format!("http://127.0.0.1:{port}/heartbeat/x"))
        .send()
        .await
        .unwrap();
    assert_eq!(flat_hb.status(), 404);
}

#[tokio::test]
async fn same_target_referenced_twice_creates_one_pool() {
    let port = free_port().await;
    let bits = make_dedup_pool_bits(port);
    wait_for_pool(port, "pool_a").await;

    let client = Client::new();
    let handle = bits.submit(Job::new(serde_json::json!({"class": "x"})));

    let work = client
        .get(format!(
            "http://127.0.0.1:{port}/pool_a/work?timeout_ms=5000"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(work.status(), 200);
    let work: serde_json::Value = work.json().await.unwrap();
    let id = work["job_id"].as_str().unwrap();

    let done = client
        .post(format!("http://127.0.0.1:{port}/pool_a/complete/data/{id}"))
        .header("content-type", "application/json")
        .body(r#"{"dedup": true}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(done.status(), 200);

    assert!(matches!(
        bits.poll(&handle.id, Some(Duration::from_secs(5))).await,
        PollOutcome::Ready(JobResult::Success { .. })
    ));
}
