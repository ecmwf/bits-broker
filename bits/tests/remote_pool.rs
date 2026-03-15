use std::time::Duration;

use bits::{Bits, Job, JobResult, PollOutcome};
use futures::TryStreamExt;
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
          type: remote_pool
          bind: "127.0.0.1:{port}"
          heartbeat_timeout_secs: {heartbeat_timeout_secs}
"#
    );
    Bits::from_config(&config).expect("config error")
}

fn make_bits_with_queue(port: u16, heartbeat_timeout_secs: f64, queue: &str) -> Bits {
    let config = format!(
        r#"
routes:
  default:
    - target::remote: ~
      dispatcher:
        queue: {queue}
        executor:
          type: remote_pool
          bind: "127.0.0.1:{port}"
          heartbeat_timeout_secs: {heartbeat_timeout_secs}
"#
    );
    Bits::from_config(&config).expect("config error")
}

// ─── tests ────────────────────────────────────────────────────────────────────

/// Happy path: worker polls, sends a heartbeat, then streams a completion body.
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
        .post(format!("http://127.0.0.1:{port}/complete/data/{job_id}"))
        .header("content-type", "application/json")
        .body(r#"{"result": 42}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(done.status(), 200);

    match bits.poll(&handle.id, Some(Duration::from_secs(5))).await {
        PollOutcome::Ready(JobResult::Success {
            content_type,
            size,
            stream,
        }) => {
            assert_eq!(content_type, "application/json");
            assert_eq!(size, 14);
            let body = stream
                .try_fold(Vec::new(), |mut acc, chunk| async move {
                    acc.extend_from_slice(&chunk);
                    Ok(acc)
                })
                .await
                .unwrap();
            assert_eq!(body, br#"{"result": 42}"#);
        }
        other => panic!("expected Success, got {:?}", other),
    }
}

#[tokio::test]
async fn worker_streams_binary_job_chunks() {
    let port = free_port().await;
    let bits = make_bits(port, 60.0);
    wait_for_server(port).await;

    let handle = bits.submit(Job::new(serde_json::json!({"class": "od"})));
    let client = Client::new();

    let resp = client
        .get(format!("http://127.0.0.1:{port}/work?timeout_ms=5000"))
        .send()
        .await
        .unwrap();
    let work: serde_json::Value = resp.json().await.unwrap();
    let job_id = work["job_id"].as_str().unwrap();

    let upload = reqwest::Body::wrap_stream(futures::stream::iter(vec![
        Ok::<_, std::io::Error>(bytes::Bytes::from_static(&[0u8, 159u8])),
        Ok::<_, std::io::Error>(bytes::Bytes::from_static(&[146u8, 150u8])),
    ]));

    let done = client
        .post(format!("http://127.0.0.1:{port}/complete/data/{job_id}"))
        .header("content-type", "application/x-grib")
        .body(upload)
        .send()
        .await
        .unwrap();
    assert_eq!(done.status(), 200);

    match bits.poll(&handle.id, Some(Duration::from_secs(5))).await {
        PollOutcome::Ready(JobResult::Success {
            content_type,
            size,
            stream,
        }) => {
            assert_eq!(content_type, "application/x-grib");
            assert_eq!(size, -1);
            let body = stream
                .try_fold(Vec::new(), |mut acc, chunk| async move {
                    acc.extend_from_slice(&chunk);
                    Ok(acc)
                })
                .await
                .unwrap();
            assert_eq!(body, vec![0u8, 159u8, 146u8, 150u8]);
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
        .post(format!("http://127.0.0.1:{port}/complete/reject/{job_id}"))
        .json(&serde_json::json!({
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
        .post(format!("http://127.0.0.1:{port}/complete/error/{job_id}"))
        .json(&serde_json::json!({
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
        .post(format!(
            "http://127.0.0.1:{port}/complete/redirect/{job_id}"
        ))
        .json(&serde_json::json!({
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
        .post(format!(
            "http://127.0.0.1:{port}/complete/reject/no-such-job"
        ))
        .json(&serde_json::json!({"reason": "nope"}))
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

#[tokio::test]
async fn multiple_workers_polling_get_distinct_jobs() {
    let port = free_port().await;
    let bits = make_bits(port, 60.0);
    wait_for_server(port).await;

    let h1 = bits.submit(Job::new(serde_json::json!({"n": 1})));
    let h2 = bits.submit(Job::new(serde_json::json!({"n": 2})));
    let client = Client::new();

    let (r1, r2) = tokio::join!(
        client
            .get(format!("http://127.0.0.1:{port}/work?timeout_ms=5000"))
            .send(),
        client
            .get(format!("http://127.0.0.1:{port}/work?timeout_ms=5000"))
            .send(),
    );

    let w1: serde_json::Value = r1.unwrap().json().await.unwrap();
    let w2: serde_json::Value = r2.unwrap().json().await.unwrap();

    let id1 = w1["job_id"].as_str().unwrap().to_string();
    let id2 = w2["job_id"].as_str().unwrap().to_string();
    assert_ne!(id1, id2, "two workers should not receive the same job");

    client
        .post(format!("http://127.0.0.1:{port}/complete/reject/{id1}"))
        .json(&serde_json::json!({"reason": "done"}))
        .send()
        .await
        .unwrap();
    client
        .post(format!("http://127.0.0.1:{port}/complete/reject/{id2}"))
        .json(&serde_json::json!({"reason": "done"}))
        .send()
        .await
        .unwrap();

    let _ = bits.poll(&h1.id, Some(Duration::from_secs(5))).await;
    let _ = bits.poll(&h2.id, Some(Duration::from_secs(5))).await;
}

#[tokio::test]
async fn worker_cannot_complete_same_job_twice() {
    let port = free_port().await;
    let bits = make_bits(port, 60.0);
    wait_for_server(port).await;

    let handle = bits.submit(Job::new(serde_json::json!({"class": "od"})));
    let client = Client::new();

    let resp = client
        .get(format!("http://127.0.0.1:{port}/work?timeout_ms=5000"))
        .send()
        .await
        .unwrap();
    let work: serde_json::Value = resp.json().await.unwrap();
    let job_id = work["job_id"].as_str().unwrap();

    let first = client
        .post(format!("http://127.0.0.1:{port}/complete/reject/{job_id}"))
        .json(&serde_json::json!({"reason": "first"}))
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 200);

    let second = client
        .post(format!("http://127.0.0.1:{port}/complete/reject/{job_id}"))
        .json(&serde_json::json!({"reason": "second"}))
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), 404);

    let _ = bits.poll(&handle.id, Some(Duration::from_secs(5))).await;
}

#[tokio::test]
async fn heartbeat_after_completion_returns_404() {
    let port = free_port().await;
    let bits = make_bits(port, 60.0);
    wait_for_server(port).await;

    let handle = bits.submit(Job::new(serde_json::json!({"class": "od"})));
    let client = Client::new();

    let resp = client
        .get(format!("http://127.0.0.1:{port}/work?timeout_ms=5000"))
        .send()
        .await
        .unwrap();
    let work: serde_json::Value = resp.json().await.unwrap();
    let job_id = work["job_id"].as_str().unwrap();

    client
        .post(format!("http://127.0.0.1:{port}/complete/reject/{job_id}"))
        .json(&serde_json::json!({"reason": "done"}))
        .send()
        .await
        .unwrap();

    let hb = client
        .post(format!("http://127.0.0.1:{port}/heartbeat/{job_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(hb.status(), 404);

    let _ = bits.poll(&handle.id, Some(Duration::from_secs(5))).await;
}

#[tokio::test]
async fn remote_pool_preserves_cost_weighted_ordering() {
    let port = free_port().await;
    let bits = make_bits_with_queue(port, 60.0, "cost_weighted");
    wait_for_server(port).await;

    let blocker = bits.submit(Job::new(serde_json::json!({"kind": "blocker", "cost": 0})));
    let client = Client::new();

    let blocker_resp = client
        .get(format!("http://127.0.0.1:{port}/work?timeout_ms=5000"))
        .send()
        .await
        .unwrap();
    let blocker_work: serde_json::Value = blocker_resp.json().await.unwrap();
    let blocker_id = blocker_work["job_id"].as_str().unwrap().to_string();

    let mut expensive = Job::new(serde_json::json!({"label": "expensive"}));
    expensive.metadata["cost"] = serde_json::json!(100u64);
    let h_expensive = bits.submit(expensive);

    let mut cheap = Job::new(serde_json::json!({"label": "cheap"}));
    cheap.metadata["cost"] = serde_json::json!(1u64);
    let h_cheap = bits.submit(cheap);

    tokio::time::sleep(Duration::from_millis(20)).await;

    client
        .post(format!(
            "http://127.0.0.1:{port}/complete/reject/{blocker_id}"
        ))
        .json(&serde_json::json!({"reason": "release blocker"}))
        .send()
        .await
        .unwrap();

    let first = client
        .get(format!("http://127.0.0.1:{port}/work?timeout_ms=5000"))
        .send()
        .await
        .unwrap();
    let second = client
        .get(format!("http://127.0.0.1:{port}/work?timeout_ms=5000"))
        .send()
        .await
        .unwrap();

    let first_work: serde_json::Value = first.json().await.unwrap();
    let second_work: serde_json::Value = second.json().await.unwrap();

    assert_eq!(first_work["request"]["label"], "cheap");
    assert_eq!(second_work["request"]["label"], "expensive");

    client
        .post(format!(
            "http://127.0.0.1:{port}/complete/reject/{}",
            first_work["job_id"].as_str().unwrap()
        ))
        .json(&serde_json::json!({"reason": "done"}))
        .send()
        .await
        .unwrap();
    client
        .post(format!(
            "http://127.0.0.1:{port}/complete/reject/{}",
            second_work["job_id"].as_str().unwrap()
        ))
        .json(&serde_json::json!({"reason": "done"}))
        .send()
        .await
        .unwrap();

    let _ = bits.poll(&blocker.id, Some(Duration::from_secs(5))).await;
    let _ = bits
        .poll(&h_expensive.id, Some(Duration::from_secs(5)))
        .await;
    let _ = bits.poll(&h_cheap.id, Some(Duration::from_secs(5))).await;
}

#[tokio::test]
async fn remote_pool_preserves_age_priority_ordering() {
    let port = free_port().await;
    let bits = make_bits_with_queue(port, 60.0, "age_priority");
    wait_for_server(port).await;

    let blocker = bits.submit(Job::new(serde_json::json!({"kind": "blocker"})));
    let client = Client::new();

    let blocker_resp = client
        .get(format!("http://127.0.0.1:{port}/work?timeout_ms=5000"))
        .send()
        .await
        .unwrap();
    let blocker_work: serde_json::Value = blocker_resp.json().await.unwrap();
    let blocker_id = blocker_work["job_id"].as_str().unwrap().to_string();

    let mut expensive = Job::new(serde_json::json!({"label": "expensive"}));
    expensive.metadata["cost"] = serde_json::json!(100u64);
    let h_expensive = bits.submit(expensive);

    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut cheap = Job::new(serde_json::json!({"label": "cheap"}));
    cheap.metadata["cost"] = serde_json::json!(1u64);
    let h_cheap = bits.submit(cheap);

    tokio::time::sleep(Duration::from_millis(20)).await;

    client
        .post(format!(
            "http://127.0.0.1:{port}/complete/reject/{blocker_id}"
        ))
        .json(&serde_json::json!({"reason": "release blocker"}))
        .send()
        .await
        .unwrap();

    let first = client
        .get(format!("http://127.0.0.1:{port}/work?timeout_ms=5000"))
        .send()
        .await
        .unwrap();
    let second = client
        .get(format!("http://127.0.0.1:{port}/work?timeout_ms=5000"))
        .send()
        .await
        .unwrap();

    let first_work: serde_json::Value = first.json().await.unwrap();
    let second_work: serde_json::Value = second.json().await.unwrap();

    assert_eq!(first_work["request"]["label"], "expensive");
    assert_eq!(second_work["request"]["label"], "cheap");

    client
        .post(format!(
            "http://127.0.0.1:{port}/complete/reject/{}",
            first_work["job_id"].as_str().unwrap()
        ))
        .json(&serde_json::json!({"reason": "done"}))
        .send()
        .await
        .unwrap();
    client
        .post(format!(
            "http://127.0.0.1:{port}/complete/reject/{}",
            second_work["job_id"].as_str().unwrap()
        ))
        .json(&serde_json::json!({"reason": "done"}))
        .send()
        .await
        .unwrap();

    let _ = bits.poll(&blocker.id, Some(Duration::from_secs(5))).await;
    let _ = bits
        .poll(&h_expensive.id, Some(Duration::from_secs(5)))
        .await;
    let _ = bits.poll(&h_cheap.id, Some(Duration::from_secs(5))).await;
}

#[tokio::test]
async fn remote_pool_skips_claimed_job_if_caller_dropped() {
    let port = free_port().await;
    let bits = make_bits(port, 60.0);
    wait_for_server(port).await;

    let client = Client::new();

    let handle = bits.submit(Job::new(serde_json::json!({"n": 1})));
    bits.cancel(&handle.id);
    let _ = bits.poll(&handle.id, Some(Duration::from_secs(5))).await;

    let resp = client
        .get(format!("http://127.0.0.1:{port}/work?timeout_ms=50"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
}
