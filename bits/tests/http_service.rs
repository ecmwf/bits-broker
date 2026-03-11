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

async fn start_server(config: &str, poll_timeout: Duration) -> u16 {
    let bits = Arc::new(Bits::from_config(config).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let state = AppState { bits, poll_timeout };
    let app = Router::new()
        .route("/job", post(submit_job))
        .route("/job/{id}", get(poll_job))
        .with_state(state);
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    port
}

// ================================
//   Tests
// ================================

#[tokio::test]
async fn post_job_returns_result() {
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
routes:
  default:
    - target::dummy_dispatch:
        duration_ms: 10
        concurrency: 1
"#;

    let port = start_server(config, Duration::from_secs(25)).await;
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
  default:
    - target::dummy_dispatch:
        duration_ms: 100
        concurrency: 1
"#;

    let port = start_server(config, Duration::from_millis(50)).await;
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
