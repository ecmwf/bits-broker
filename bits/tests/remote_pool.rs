use std::time::Duration;

use bits::{Bits, Job, JobResult, PollOutcome};
use reqwest::Client;

// ─── helpers ──────────────────────────────────────────────────────────────────

/// Bind a listener on port 0 to obtain a free port, then release it.
/// There is a small TOCTOU window before the remote_pool server binds the same
/// port, but `wait_for_server` compensates by retrying until the server is up.
async fn free_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().port()
    // listener drops here
}

/// Retry until the remote_pool HTTP server is responding, or panic.
async fn wait_for_server(port: u16) {
    let client = Client::new();
    for _ in 0..100 {
        if client
            .get(format!("http://127.0.0.1:{port}/work?timeout_ms=0"))
            .send()
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("remote_pool server on port {port} never became ready");
}

fn make_bits(port: u16, heartbeat_timeout_secs: f64) -> Bits {
    // `target::remote: ~` — null value deserialized into the unit struct RemoteTarget.
    let config = format!(
        r#"
routes:
  default:
    - target::remote: ~
      dispatcher:
        executor:
          remote_pool:
            bind: "127.0.0.1:{port}"
            heartbeat_timeout_secs: {heartbeat_timeout_secs}
"#
    );
    Bits::from_config(&config).expect("config error")
}

// ─── tests ────────────────────────────────────────────────────────────────────

/// Happy path: worker polls, sends a heartbeat, then completes → Success.
#[tokio::test]
async fn worker_completes_job() {
    let port = free_port().await;
    let bits = make_bits(port, 60.0);
    wait_for_server(port).await;

    let handle = bits.submit(Job::new(serde_json::json!({"class": "od"})));
    let client = Client::new();

    // Long-poll until the job is available.
    let resp = client
        .get(format!("http://127.0.0.1:{port}/work?timeout_ms=5000"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let work: serde_json::Value = resp.json().await.unwrap();
    let job_id = work["job_id"].as_str().unwrap();
    assert_eq!(
        work["request"]["class"], "od",
        "request payload should be forwarded"
    );

    // Heartbeat while "working".
    let hb = client
        .post(format!("http://127.0.0.1:{port}/heartbeat/{job_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(hb.status(), 200);

    // Complete the job.
    let done = client
        .post(format!("http://127.0.0.1:{port}/complete/{job_id}"))
        .json(&serde_json::json!({
            "status": "complete",
            "content_type": "application/json",
            "body": r#"{"result": 42}"#
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(done.status(), 200);

    match bits.poll(&handle.id, Some(Duration::from_secs(5))).await {
        PollOutcome::Ready(JobResult::Success { content_type, .. }) => {
            assert_eq!(content_type, "application/json");
        }
        other => panic!("expected Success, got {:?}", other),
    }
}

/// Worker rejects the job → Bits surfaces it as a routing Error.
#[tokio::test]
async fn worker_rejects_job() {
    let port = free_port().await;
    let bits = make_bits(port, 60.0);
    wait_for_server(port).await;

    let handle = bits.submit(Job::new(serde_json::json!({})));
    let client = Client::new();

    let resp = client
        .get(format!("http://127.0.0.1:{port}/work?timeout_ms=5000"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let work: serde_json::Value = resp.json().await.unwrap();
    let job_id = work["job_id"].as_str().unwrap();

    client
        .post(format!("http://127.0.0.1:{port}/complete/{job_id}"))
        .json(&serde_json::json!({
            "status": "reject",
            "reason": "unsupported request"
        }))
        .send()
        .await
        .unwrap();

    // A worker reject causes the route to be abandoned. With no further routes
    // the switch returns "no route matched", which bits surfaces as an Error.
    match bits.poll(&handle.id, Some(Duration::from_secs(5))).await {
        PollOutcome::Ready(JobResult::Error { .. }) => {}
        other => panic!("expected Error, got {:?}", other),
    }
}

/// Worker posts an error → Bits surfaces it as a Failed result.
#[tokio::test]
async fn worker_reports_error() {
    let port = free_port().await;
    let bits = make_bits(port, 60.0);
    wait_for_server(port).await;

    let handle = bits.submit(Job::new(serde_json::json!({})));
    let client = Client::new();

    let resp = client
        .get(format!("http://127.0.0.1:{port}/work?timeout_ms=5000"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let work: serde_json::Value = resp.json().await.unwrap();
    let job_id = work["job_id"].as_str().unwrap();

    client
        .post(format!("http://127.0.0.1:{port}/complete/{job_id}"))
        .json(&serde_json::json!({
            "status": "error",
            "message": "internal worker failure"
        }))
        .send()
        .await
        .unwrap();

    match bits.poll(&handle.id, Some(Duration::from_secs(5))).await {
        PollOutcome::Ready(JobResult::Failed { reason }) => {
            assert!(
                reason.contains("internal worker failure"),
                "unexpected reason: {reason}"
            );
        }
        other => panic!("expected Failed, got {:?}", other),
    }
}

/// Worker requests a redirect → Bits surfaces Redirect to the client.
#[tokio::test]
async fn worker_requests_redirect() {
    let port = free_port().await;
    let bits = make_bits(port, 60.0);
    wait_for_server(port).await;

    let handle = bits.submit(Job::new(serde_json::json!({"dataset": "era5"})));
    let client = Client::new();

    let resp = client
        .get(format!("http://127.0.0.1:{port}/work?timeout_ms=5000"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let work: serde_json::Value = resp.json().await.unwrap();
    let job_id = work["job_id"].as_str().unwrap();

    let done = client
        .post(format!("http://127.0.0.1:{port}/complete/{job_id}"))
        .json(&serde_json::json!({
            "status": "redirect",
            "location": "https://example-bucket.s3.amazonaws.com/object?signature=abc",
            "message": "Download from object storage"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(done.status(), 200);

    match bits.poll(&handle.id, Some(Duration::from_secs(5))).await {
        PollOutcome::Ready(JobResult::Redirect { location, message }) => {
            assert!(location.contains("example-bucket.s3.amazonaws.com/object"));
            assert_eq!(message, "Download from object storage");
        }
        other => panic!("expected Redirect, got {:?}", other),
    }
}

/// Long-polling with no queued work returns 204 No Content within the timeout.
#[tokio::test]
async fn long_poll_returns_204_when_no_work() {
    let port = free_port().await;
    let _bits = make_bits(port, 60.0);
    wait_for_server(port).await;

    let resp = Client::new()
        .get(format!("http://127.0.0.1:{port}/work?timeout_ms=50"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
}

/// Heartbeating an unknown job_id returns 404.
#[tokio::test]
async fn heartbeat_unknown_job_returns_404() {
    let port = free_port().await;
    let _bits = make_bits(port, 60.0);
    wait_for_server(port).await;

    let resp = Client::new()
        .post(format!("http://127.0.0.1:{port}/heartbeat/no-such-job"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

/// Completing an already-completed (or unknown) job_id returns 404.
#[tokio::test]
async fn complete_unknown_job_returns_404() {
    let port = free_port().await;
    let _bits = make_bits(port, 60.0);
    wait_for_server(port).await;

    let resp = Client::new()
        .post(format!("http://127.0.0.1:{port}/complete/no-such-job"))
        .json(&serde_json::json!({"status": "reject", "reason": "nope"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

/// When a worker stops heartbeating the reaper evicts the job, which causes
/// the waiting execute() future to error → Bits returns Failed.
#[tokio::test]
async fn heartbeat_timeout_evicts_job() {
    let port = free_port().await;
    // heartbeat_timeout_secs = 0.1 → reaper ticks every 50 ms
    let bits = make_bits(port, 0.1);
    wait_for_server(port).await;

    let handle = bits.submit(Job::new(serde_json::json!({})));
    let client = Client::new();

    // Pick up the job but never heartbeat or complete it.
    let resp = client
        .get(format!("http://127.0.0.1:{port}/work?timeout_ms=5000"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Wait long enough for the reaper to fire (two tick intervals = ~100 ms,
    // plus the 100 ms timeout = ~200 ms total; 500 ms is a comfortable margin).
    tokio::time::sleep(Duration::from_millis(500)).await;

    match bits.poll(&handle.id, Some(Duration::from_secs(1))).await {
        PollOutcome::Ready(JobResult::Failed { .. }) => {}
        other => panic!("expected Failed after heartbeat timeout, got {:?}", other),
    }
}
