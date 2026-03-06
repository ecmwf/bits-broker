mod common;

use std::time::Duration;

use bits::Bits;
use tokio::net::TcpListener;

/// Bind to port 0, capture the assigned port, then drop the listener so
/// the server can bind immediately after.
async fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test]
async fn post_job_returns_result() {
    // Reference common types so the register_action! inventory entries are linked in.
    let _ = common::TargetDummyDelay::new(0, 1);

    let port = free_port().await;

    let config = format!(r#"
server:
  type: http
  bind: "127.0.0.1:{port}"
routes:
  default:
    - target::dummy_dispatch:
        duration_ms: 10
        concurrency: 1
"#);

    tokio::spawn(async move {
        Bits::from_config(&config).unwrap().serve().await.unwrap();
    });

    // Give the server time to bind and accept connections.
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
    let _ = common::TargetDummyDelay::new(0, 1);

    let port = free_port().await;

    // poll_timeout_ms=200 → first request times out and returns a poll redirect.
    // duration_ms=300 → job finishes 100ms into the second poll window,
    // so the second GET returns the final result.
    let config = format!(r#"
server:
  type: http
  bind: "127.0.0.1:{port}"
  poll_timeout_ms: 50
routes:
  default:
    - target::dummy_dispatch:
        duration_ms: 100
        concurrency: 1
"#);

    tokio::spawn(async move {
        Bits::from_config(&config).unwrap().serve().await.unwrap();
    });

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
    let poll_url = resp.headers().get("location").unwrap().to_str().unwrap().to_string();
    assert!(poll_url.starts_with("/job/"), "expected poll redirect, got Location: {poll_url}");

    // Follow the redirect — job finishes during this poll, expect the final result.
    let resp = client
        .get(format!("http://127.0.0.1:{port}{poll_url}"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::SEE_OTHER);
    let final_location = resp.headers().get("location").unwrap().to_str().unwrap();
    assert!(!final_location.starts_with("/job/"), "expected final result, not another poll redirect");
}
