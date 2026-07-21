// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

mod common;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bits::actions::{Action, ActionError, TargetAction, TargetResult};
use bits::db::{BrokerLeaseStore, memory::MemoryStore};
use bits::routing::{Route, switch::Switch};
use bits::server::{
    CODE_ACTION_CANCELLED, CODE_ACTION_CLIENT_GONE, CODE_JOB_ERROR, CODE_JOB_FAILED, CODE_JOB_LOST,
    CODE_JOB_NOT_FOUND,
};
use bits::{Bits, Job, JobResult};
use common::recovery::{broker_identity, new_recovery_job_id};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

struct TestServer {
    port: u16,
    bits: Arc<Bits>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TargetClientGoneAfterDelay {
    check_interval_ms: u64,
}

#[async_trait]
impl TargetAction for TargetClientGoneAfterDelay {
    async fn dispatch(&self, job: &Job) -> Result<TargetResult, ActionError> {
        let interval = Duration::from_millis(self.check_interval_ms);
        let timeout = tokio::time::Instant::now() + Duration::from_secs(6);
        loop {
            if !job.client_present() {
                return Err(ActionError::ClientGone);
            }
            if tokio::time::Instant::now() > timeout {
                return Ok(TargetResult::Complete(JobResult::Redirect {
                    location: String::new(),
                    message: "still connected".to_string(),
                    content_type: None,
                    content_length: None,
                }));
            }
            tokio::time::sleep(interval).await;
        }
    }
}

bits::register_action!(
    target,
    "client_gone_after_delay",
    TargetClientGoneAfterDelay
);

async fn start_server(config: &str, poll_timeout: Duration) -> TestServer {
    let config = format!("bits:\n  site: tst\n  env: hsv\n{config}");
    let bits = Arc::new(Bits::from_config(&config).unwrap());
    start_server_with_bits(bits, poll_timeout).await
}

async fn start_server_with_bits(bits: Arc<Bits>, poll_timeout: Duration) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = bits::server::router(
        bits.clone(),
        poll_timeout,
        bits::server::DEFAULT_RETRY_AFTER_SECS,
    );
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    TestServer { port, bits }
}

#[tokio::test]
async fn post_job_returns_result() {
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 10
          concurrency: 1
"#;

    let server = start_server(config, Duration::from_secs(25)).await;
    let port = server.port;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("http://127.0.0.1:{port}/job"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::SEE_OTHER);
}

#[tokio::test]
async fn poll_redirect_resolves_to_final_result() {
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 100
          concurrency: 1
"#;

    let server = start_server(config, Duration::from_millis(50)).await;
    let port = server.port;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("http://127.0.0.1:{port}/job"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::SEE_OTHER);
    let poll_url = resp
        .headers()
        .get("location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        poll_url.starts_with("/job/"),
        "expected poll redirect, got Location: {poll_url}"
    );

    let resp = client
        .get(format!("http://127.0.0.1:{port}{poll_url}"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::SEE_OTHER);
    let final_location = resp.headers().get("location").unwrap().to_str().unwrap();
    assert!(
        !final_location.starts_with("/job/"),
        "expected final result, not another poll redirect"
    );
}

#[tokio::test]
async fn post_malformed_json_returns_error() {
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 0
          concurrency: 1
"#;
    let server = start_server(config, Duration::from_secs(5)).await;
    let port = server.port;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/job"))
        .header("content-type", "application/json")
        .body("not valid json{{{")
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "malformed JSON should return 400"
    );
}

#[tokio::test]
async fn get_nonexistent_job_returns_not_found() {
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 0
          concurrency: 1
"#;
    let server = start_server(config, Duration::from_millis(100)).await;
    let port = server.port;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let resp = client
        .get(format!("http://127.0.0.1:{port}/job/does-not-exist"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn post_empty_object_is_valid() {
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 0
          concurrency: 1
"#;
    let server = start_server(config, Duration::from_secs(5)).await;
    let port = server.port;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/job"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::SEE_OTHER,
        "empty object should produce a redirect result"
    );
}

#[tokio::test]
async fn concurrent_submits_all_resolve() {
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 0
          concurrency: 8
"#;
    let server = start_server(config, Duration::from_secs(5)).await;
    let port = server.port;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let futs: Vec<_> = (0..20)
        .map(|i| {
            let client = client.clone();
            async move {
                client
                    .post(format!("http://127.0.0.1:{port}/job"))
                    .json(&serde_json::json!({"index": i}))
                    .send()
                    .await
                    .unwrap()
                    .status()
            }
        })
        .collect();

    let statuses = futures::future::join_all(futs).await;
    for (i, status) in statuses.iter().enumerate() {
        assert_eq!(
            *status,
            reqwest::StatusCode::SEE_OTHER,
            "request {i} expected 303, got {status}"
        );
    }
}

#[tokio::test]
async fn pending_redirect_includes_location_and_retry_after_headers() {
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 500
          concurrency: 1
"#;
    let server = start_server(config, Duration::from_millis(50)).await;
    let port = server.port;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("http://127.0.0.1:{port}/job"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::SEE_OTHER);

    let location = resp
        .headers()
        .get("location")
        .expect("missing Location header");
    assert!(
        location.to_str().unwrap().starts_with("/job/"),
        "Location should point to /job/{{id}}, got: {location:?}"
    );

    let retry_after = resp
        .headers()
        .get("retry-after")
        .expect("missing Retry-After header");
    assert_eq!(retry_after.to_str().unwrap(), "0");
}

#[tokio::test]
async fn cancelled_job_returns_gone_via_http() {
    let _ = common::CheckDummyDelay::new(500);
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - check::dummy_delay:
          duration_ms: 500
      - target::dummy_dispatch:
          duration_ms: 0
          concurrency: 1
"#;
    let server = start_server(config, Duration::from_secs(2)).await;
    let port = server.port;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let handle = server
        .bits
        .submit(Job::new_with_id(
            new_recovery_job_id("tst", "hsv", 30),
            serde_json::json!({}),
        ))
        .expect_accepted("submit should not be rejected");
    server.bits.cancel(&handle.id);

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let resp = loop {
        let r = client
            .get(format!("http://127.0.0.1:{port}/job/{}", handle.id))
            .send()
            .await
            .unwrap();
        if r.status() == reqwest::StatusCode::GONE {
            break r;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for GONE, last status: {}",
            r.status()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/json"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], CODE_ACTION_CANCELLED);
    assert_eq!(body["retryable"], false);
}

#[tokio::test]
async fn not_found_returns_json_error_body() {
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 0
          concurrency: 1
"#;
    let server = start_server(config, Duration::from_secs(5)).await;
    let port = server.port;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let resp = client
        .get(format!("http://127.0.0.1:{port}/job/does-not-exist"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/json"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], CODE_JOB_NOT_FOUND);
    assert_eq!(body["retryable"], false);
    assert!(body["message"].as_str().unwrap().contains("does not exist"));
}

#[tokio::test]
async fn check_reject_returns_json_error_body() {
    let _ = common::CheckAlwaysReject;
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - check::always_reject:
      - target::dummy_dispatch:
          duration_ms: 0
          concurrency: 1
"#;
    let server = start_server(config, Duration::from_secs(5)).await;
    let port = server.port;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("http://127.0.0.1:{port}/job"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/json"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], CODE_JOB_ERROR);
    assert_eq!(body["retryable"], false);
}

#[tokio::test]
async fn panicking_target_returns_json_error_body() {
    let _ = common::TargetPanicking;

    let config = r#"
bits:
  site: tst
  env: dev
routes:
  - panicking:
      - target::panicking: ~
"#;
    let server = start_server(config, Duration::from_secs(5)).await;
    let port = server.port;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("http://127.0.0.1:{port}/job"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/json"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], CODE_JOB_FAILED);
    assert_eq!(body["message"], "internal server error");
    assert_eq!(body["retryable"], false);
}

#[tokio::test]
async fn user_limit_exceeded_returns_http_429_with_retry_after() {
    let _ = common::TargetAlwaysUserLimitExceeded;

    let config = r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - target::always_user_limit_exceeded: ~
"#;
    let server = start_server(config, Duration::from_secs(5)).await;
    let port = server.port;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("http://127.0.0.1:{port}/job"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        resp.headers().get("retry-after").unwrap().to_str().unwrap(),
        bits::server::DEFAULT_RETRY_AFTER_SECS.to_string()
    );

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], bits::server::CODE_USER_LIMIT_EXCEEDED);
    assert_eq!(
        body["message"],
        "user is at the per-user limit (6) for this route"
    );
    assert_eq!(body["retryable"], true);
}

#[tokio::test]
async fn job_lost_returns_json_error_body() {
    let store = Arc::new(MemoryStore::new());
    let owner_id = broker_identity("tst", "hsv", 1);

    let router = Switch::new(vec![Route::new(
        "default".to_string(),
        vec![Action::Target(
            Arc::new(common::TargetDummyDelay::new(0)),
            None,
            None,
        )],
    )]);
    let bits = Arc::new(Bits::from_router_for_tests(
        router,
        "tst-tst-23".to_string(),
        "http://127.0.0.1:9/job".to_string(),
        Duration::from_millis(30),
        None,
        Some(store.clone()),
        Duration::from_secs(5),
    ));

    let server = start_server_with_bits(bits, Duration::from_millis(50)).await;
    let port = server.port;
    tokio::time::sleep(Duration::from_millis(50)).await;

    store
        .upsert_broker_lease(
            owner_id,
            "http://127.0.0.1:1/job",
            Duration::from_millis(20),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let job_id = new_recovery_job_id("tst", "hsv", 1);

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let resp = client
        .get(format!("http://127.0.0.1:{port}/job/{job_id}"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::GONE);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/json"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], CODE_JOB_LOST);
    assert_eq!(body["retryable"], false);
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("no longer recoverable")
    );
}

#[tokio::test]
async fn client_gone_returns_json_error_body() {
    let _ = TargetClientGoneAfterDelay {
        check_interval_ms: 100,
    };

    let config = r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - target::client_gone_after_delay:
          check_interval_ms: 100
"#;

    let server = start_server(config, Duration::from_millis(50)).await;
    let port = server.port;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let submit = client
        .post(format!("http://127.0.0.1:{port}/job"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(submit.status(), reqwest::StatusCode::SEE_OTHER);
    let poll_url = submit
        .headers()
        .get("location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // This test runs against a real TCP server (`reqwest` + `axum`), so
    // `tokio::time::pause` cannot reliably advance socket I/O timing.
    // We wait for reconnect buffer expiry plus a small margin.
    tokio::time::sleep(Duration::from_millis(5600)).await;

    let resp = client
        .get(format!("http://127.0.0.1:{port}{poll_url}"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::GONE);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/json"
    );
    let resp: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(resp["code"], CODE_ACTION_CLIENT_GONE);
    assert_eq!(resp["retryable"], false);
    assert_eq!(resp["message"], "client disconnected");
}
