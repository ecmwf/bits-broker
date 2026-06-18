mod common;

use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use bits::actions::{ActionError, CheckResult};
use bits::dispatcher::{
    DEFAULT_QUEUE_CAPACITY, DispatchGuard, Dispatcher, ExecutorKind, QueueKind,
};
use bits::server::{CODE_QUEUE_FULL, DEFAULT_RETRY_AFTER_SECS};
use bits::{Job, PollOutcome, SubmitOutcome, parse_bootstrap};
use futures::future::BoxFuture;
use futures::poll;
use tokio::net::TcpListener;

#[tokio::test]
async fn queue_capacity_rejects_when_full() {
    let dispatcher = Dispatcher::<CheckResult>::from_config(
        Some(&QueueKind::Fifo),
        Some(&ExecutorKind::AsyncPool {
            concurrency: Some(1),
        }),
        None,
        None,
        2,
    )
    .expect("dispatcher config should not error")
    .expect("dispatcher should be created");

    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

    let job1 = Job::new(serde_json::json!({}));
    let work1: BoxFuture<'static, Result<CheckResult, ActionError>> = Box::pin(async move {
        let _ = release_rx.await;
        Ok(CheckResult::Pass)
    });

    let job2 = Job::new(serde_json::json!({}));
    let work2: BoxFuture<'static, Result<CheckResult, ActionError>> =
        Box::pin(async move { Ok(CheckResult::Pass) });

    let first = dispatcher.dispatch(&job1, DispatchGuard::None, work1);
    let second = dispatcher.dispatch(&job2, DispatchGuard::None, work2);
    tokio::pin!(first);
    tokio::pin!(second);

    assert!(matches!(poll!(first.as_mut()), Poll::Pending));
    assert!(matches!(poll!(second.as_mut()), Poll::Pending));

    let job3 = Job::new(serde_json::json!({}));
    let work3: BoxFuture<'static, Result<CheckResult, ActionError>> =
        Box::pin(async move { Ok(CheckResult::Pass) });

    let third = dispatcher.dispatch(&job3, DispatchGuard::None, work3).await;
    assert!(matches!(third, Err(ActionError::QueueFull(_))));

    let _ = release_tx.send(());
    tokio::task::yield_now().await;

    assert!(first.await.is_ok());
    assert!(second.await.is_ok());
}

#[tokio::test]
async fn queue_capacity_frees_slot_on_dequeue() {
    let dispatcher = Dispatcher::<CheckResult>::from_config(
        Some(&QueueKind::Fifo),
        Some(&ExecutorKind::AsyncPool {
            concurrency: Some(1),
        }),
        None,
        None,
        1,
    )
    .expect("dispatcher config should not error")
    .expect("dispatcher should be created");

    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

    let first_job = Job::new(serde_json::json!({}));
    let first_work: BoxFuture<'static, Result<CheckResult, ActionError>> = Box::pin(async move {
        let _ = release_rx.await;
        Ok(CheckResult::Pass)
    });

    let first = dispatcher.dispatch(&first_job, DispatchGuard::None, first_work);
    tokio::pin!(first);
    assert!(matches!(poll!(first.as_mut()), Poll::Pending));

    let rejected_job = Job::new(serde_json::json!({}));
    let rejected_work: BoxFuture<'static, Result<CheckResult, ActionError>> =
        Box::pin(async move { Ok(CheckResult::Pass) });
    let rejected = dispatcher
        .dispatch(&rejected_job, DispatchGuard::None, rejected_work)
        .await;
    assert!(matches!(rejected, Err(ActionError::QueueFull(_))));

    let _ = release_tx.send(());
    tokio::task::yield_now().await;
    assert!(first.await.is_ok());

    let next_job = Job::new(serde_json::json!({}));
    let next_work: BoxFuture<'static, Result<CheckResult, ActionError>> =
        Box::pin(async move { Ok(CheckResult::Pass) });
    let next = dispatcher
        .dispatch(&next_job, DispatchGuard::None, next_work)
        .await;
    assert!(next.is_ok());
}

#[test]
fn default_queue_capacity_is_500k() {
    assert_eq!(DEFAULT_QUEUE_CAPACITY, 500_000);
}

#[test]
fn config_accepts_custom_queue_capacity() {
    let yaml = r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - target::http:
          url: "http://localhost:9999"
        dispatcher:
          queue_capacity: 100
"#;

    assert!(parse_bootstrap(yaml).is_ok());
}

#[test]
fn config_rejects_zero_queue_capacity() {
    let yaml = r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - target::http:
          url: "http://localhost:9999"
        dispatcher:
          queue_capacity: 0
"#;

    let err = match parse_bootstrap(yaml) {
        Ok(_) => panic!("zero queue_capacity must be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("dispatcher.queue_capacity"),
        "unexpected error: {err}"
    );
}

#[test]
fn config_accepts_custom_max_jobs() {
    let yaml = r#"
bits:
  site: tst
  env: dev
  max_jobs: 42
routes:
  - default:
      - target::http:
          url: "http://localhost:9999"
"#;

    assert!(parse_bootstrap(yaml).is_ok());
}

#[test]
fn config_rejects_zero_max_jobs() {
    let yaml = r#"
bits:
  site: tst
  env: dev
  max_jobs: 0
routes:
  - default:
      - target::http:
          url: "http://localhost:9999"
"#;

    let err = match parse_bootstrap(yaml) {
        Ok(_) => panic!("zero max_jobs must be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("bits.max_jobs"),
        "unexpected error: {err}"
    );
}

#[test]
fn config_accepts_custom_retry_after() {
    let yaml = r#"
bits:
  site: tst
  env: dev
server:
  retry_after_secs: 9
routes:
  - default:
      - target::http:
          url: "http://localhost:9999"
"#;

    assert!(parse_bootstrap(yaml).is_ok());
}

#[test]
fn config_rejects_zero_retry_after() {
    let yaml = r#"
bits:
  site: tst
  env: dev
server:
  retry_after_secs: 0
routes:
  - default:
      - target::http:
          url: "http://localhost:9999"
"#;

    let err = match parse_bootstrap(yaml) {
        Ok(_) => panic!("zero retry_after_secs must be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("server.retry_after_secs"),
        "unexpected error: {err}"
    );
}

#[test]
fn defaults_are_sensible() {
    assert_eq!(DEFAULT_QUEUE_CAPACITY, 500_000);
    assert_eq!(bits::DEFAULT_MAX_JOBS, 500_000);
    assert_eq!(DEFAULT_RETRY_AFTER_SECS, 5);
}

#[tokio::test]
async fn submit_returns_overloaded_at_max_jobs() {
    let _ = common::TargetDummyDelay::new(500);
    let config = r#"
bits:
  site: tst
  env: dev
  max_jobs: 1
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 500
          concurrency: 1
"#;
    let bits = parse_bootstrap(config)
        .expect("parse")
        .into_bits()
        .expect("build");

    let first = bits.submit(Job::new(serde_json::json!({})));
    assert!(matches!(first, SubmitOutcome::Accepted(_)));

    let second = bits.submit(Job::new(serde_json::json!({})));
    assert!(matches!(second, SubmitOutcome::Overloaded));
}

#[tokio::test]
async fn route_handle_submit_returns_overloaded_at_max_jobs() {
    let _ = common::TargetDummyDelay::new(500);
    let config = r#"
bits:
  site: tst
  env: dev
  max_jobs: 1
targets:
  slow:
    type: dummy_dispatch
    duration_ms: 500
"#;
    let bits = parse_bootstrap(config)
        .expect("parse")
        .into_bits()
        .expect("build");
    let route = serde_json::json!([{"added": ["target::slow"]}]);
    let handle = bits.add_route("added", &route).expect("add route");

    let first = handle.submit(Job::new(serde_json::json!({})));
    assert!(matches!(first, SubmitOutcome::Accepted(_)));

    let second = handle.submit(Job::new(serde_json::json!({})));
    assert!(matches!(second, SubmitOutcome::Overloaded));
}

#[tokio::test]
async fn bits_and_route_handle_share_max_jobs_counter() {
    let _ = common::TargetDummyDelay::new(500);
    let config = r#"
bits:
  site: tst
  env: dev
  max_jobs: 1
targets:
  slow:
    type: dummy_dispatch
    duration_ms: 500
routes:
  - default:
      - target::slow
"#;
    let bits = parse_bootstrap(config)
        .expect("parse")
        .into_bits()
        .expect("build");
    let route = serde_json::json!([{"added": ["target::slow"]}]);
    let handle = bits.add_route("added", &route).expect("add route");

    let first = bits.submit(Job::new(serde_json::json!({})));
    assert!(matches!(first, SubmitOutcome::Accepted(_)));

    let second = handle.submit(Job::new(serde_json::json!({})));
    assert!(matches!(second, SubmitOutcome::Overloaded));
}

#[tokio::test]
async fn max_jobs_counter_recovers_after_completion() {
    let _ = common::TargetDummyDelay::new(0);
    let config = r#"
bits:
  site: tst
  env: dev
  max_jobs: 1
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 0
          concurrency: 256
"#;
    let bits = parse_bootstrap(config)
        .expect("parse")
        .into_bits()
        .expect("build");

    let first = bits
        .submit(Job::new(serde_json::json!({})))
        .expect_accepted("first submit");

    let outcome = bits.poll(&first.id, Some(Duration::from_secs(5))).await;
    assert!(matches!(outcome, PollOutcome::Ready(_)));

    let second = bits.submit(Job::new(serde_json::json!({})));
    assert!(
        matches!(second, SubmitOutcome::Accepted(_)),
        "counter should have recovered after poll consumed the first job"
    );
}

#[tokio::test]
async fn permit_freed_on_dequeue_not_completion() {
    let dispatcher = Dispatcher::<CheckResult>::from_config(
        Some(&QueueKind::Fifo),
        Some(&ExecutorKind::AsyncPool {
            concurrency: Some(1),
        }),
        None,
        None,
        1,
    )
    .expect("dispatcher config should not error")
    .expect("dispatcher should be created");

    let (block_tx, block_rx) = tokio::sync::oneshot::channel::<()>();

    let blocking_job = Job::new(serde_json::json!({}));
    let blocking_work: BoxFuture<'static, Result<CheckResult, ActionError>> =
        Box::pin(async move {
            let _ = block_rx.await;
            Ok(CheckResult::Pass)
        });

    let blocking = dispatcher.dispatch(&blocking_job, DispatchGuard::None, blocking_work);
    tokio::pin!(blocking);
    assert!(matches!(poll!(blocking.as_mut()), Poll::Pending));

    // Give the executor time to dequeue the blocking job. The permit is
    // released on dequeue (not completion), freeing the single slot.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let probe_job = Job::new(serde_json::json!({}));
    let probe_work: BoxFuture<'static, Result<CheckResult, ActionError>> =
        Box::pin(async move { Ok(CheckResult::Pass) });

    // Poll once: if the permit was freed, this enters Pending (admitted,
    // waiting for executor). If still held, it returns Err(QueueFull).
    let probe = dispatcher.dispatch(&probe_job, DispatchGuard::None, probe_work);
    tokio::pin!(probe);
    let poll_result = poll!(probe.as_mut());
    assert!(
        matches!(poll_result, Poll::Pending),
        "permit should be freed on dequeue, allowing a new dispatch while the first job is still executing"
    );

    let _ = block_tx.send(());
    assert!(blocking.await.is_ok());
    assert!(probe.await.is_ok());
}

#[tokio::test]
async fn duplicate_job_id_returns_overloaded() {
    let _ = common::TargetDummyDelay::new(500);
    let config = r#"
bits:
  site: tst
  env: dev
  max_jobs: 10
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 500
          concurrency: 1
"#;
    let bits = parse_bootstrap(config)
        .expect("parse")
        .into_bits()
        .expect("build");

    let valid_id = bits::request_id::encode("tst", "dev", 0, chrono::Utc::now())
        .expect("encode valid BITS ID");

    let first = bits.submit(Job::new_with_id(valid_id.clone(), serde_json::json!({})));
    assert!(
        matches!(first, SubmitOutcome::Accepted(_)),
        "first submit with a valid BITS ID should be accepted"
    );

    let second = bits.submit(Job::new_with_id(valid_id.clone(), serde_json::json!({})));
    assert!(
        matches!(second, SubmitOutcome::Overloaded),
        "second submit with the same job ID must be rejected as Overloaded"
    );
}

#[tokio::test]
async fn overloaded_result_maps_to_http_529_with_retry_after() {
    let _ = common::TargetDummyDelay::new(250);

    let config = r#"
bits:
  site: tst
  env: dev
  max_jobs: 1
server:
  retry_after_secs: 7
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 250
          concurrency: 1
"#;

    let bootstrap = parse_bootstrap(config).expect("config should parse");
    let (bits, server_config) = bootstrap.into_parts().expect("bootstrap should build bits");

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let port = listener.local_addr().expect("get listener addr").port();
    let app = bits::server::router(
        Arc::new(bits),
        Duration::from_millis(10),
        server_config.retry_after_secs,
    );
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve test app");
    });

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build http client");

    let first = client
        .post(format!("http://127.0.0.1:{port}/job"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("first submit");
    assert_eq!(first.status(), reqwest::StatusCode::SEE_OTHER);

    let second = client
        .post(format!("http://127.0.0.1:{port}/job"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("second submit");

    assert_eq!(second.status().as_u16(), 529);
    assert_eq!(
        second
            .headers()
            .get("retry-after")
            .expect("retry-after header")
            .to_str()
            .expect("retry-after header value"),
        "7"
    );

    let body: serde_json::Value = second.json().await.expect("json body for overload");
    assert_eq!(body["code"], CODE_QUEUE_FULL);
}
