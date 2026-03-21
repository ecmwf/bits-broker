mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Json, Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use bits::{Bits, Job, JobResult, PollOutcome};
use serde_json::Value;
use tokio::net::TcpListener;

// ================================
//   Minimal server (mirrors examples/http_server.rs)
// ================================

#[derive(Clone)]
struct AppState {
    bits: Arc<Bits>,
    poll_timeout: Duration,
}

async fn submit_job(State(state): State<AppState>, Json(body): Json<Value>) -> Response {
    let handle = state.bits.submit(Job::new(body));
    poll_by_id(&handle.id, &state).await
}

async fn poll_job(Path(id): Path<String>, State(state): State<AppState>) -> Response {
    poll_by_id(&id, &state).await
}

async fn poll_by_id(id: &str, state: &AppState) -> Response {
    match state.bits.poll(id, Some(state.poll_timeout)).await {
        PollOutcome::Ready(result) => match result {
            JobResult::Success {
                content_type,
                stream,
                ..
            } => (
                [(header::CONTENT_TYPE, content_type)],
                Body::from_stream(stream),
            )
                .into_response(),
            JobResult::Redirect { location, .. } => {
                (StatusCode::SEE_OTHER, [(header::LOCATION, location)]).into_response()
            }
            JobResult::Error { message } => (StatusCode::BAD_REQUEST, message).into_response(),
            JobResult::Failed { reason } => {
                (StatusCode::INTERNAL_SERVER_ERROR, reason).into_response()
            }
            JobResult::Cancelled | JobResult::ClientGone => StatusCode::GONE.into_response(),
        },
        PollOutcome::Pending { id } => (
            StatusCode::SEE_OTHER,
            [
                (header::LOCATION, format!("/job/{id}")),
                (header::RETRY_AFTER, "0".to_string()),
            ],
        )
            .into_response(),
        PollOutcome::NotFound => StatusCode::NOT_FOUND.into_response(),
        PollOutcome::JobLost => StatusCode::GONE.into_response(),
    }
}

struct TestServer {
    port: u16,
    bits: Arc<Bits>,
}

async fn start_server(config: &str, poll_timeout: Duration) -> TestServer {
    let bits = Arc::new(Bits::from_config(config).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let state = AppState {
        bits: bits.clone(),
        poll_timeout,
    };
    let app = Router::new()
        .route("/job", post(submit_job))
        .route("/job/{id}", get(poll_job))
        .with_state(state);
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    TestServer { port, bits }
}

// ================================
//   Tests
// ================================

#[tokio::test]
async fn post_job_returns_result() {
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
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

    // TargetDummyDelay returns JobResult::Redirect → HTTP 303 SEE_OTHER
    assert_eq!(resp.status(), reqwest::StatusCode::SEE_OTHER);
}

#[tokio::test]
async fn poll_redirect_resolves_to_final_result() {
    let _ = common::TargetDummyDelay::new(0);

    // poll_timeout=50ms → first request times out and returns a poll redirect.
    // duration_ms=100 → job finishes 50ms into the second poll window,
    // so the second GET returns the final result.
    let config = r#"
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

    // First request — times out, expect poll redirect.
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

    // Follow the redirect — job finishes during this poll, expect the final result.
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

    let handle = server.bits.submit(Job::new(serde_json::json!({})));
    server.bits.cancel(&handle.id);

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .get(format!("http://127.0.0.1:{port}/job/{}", handle.id))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::GONE,
        "cancelled job should return 410 GONE"
    );
}
