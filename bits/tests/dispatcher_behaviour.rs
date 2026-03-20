mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;

use bits::actions::{ActionError, CheckResult, TargetResult};
use bits::dispatcher::{DispatchGuard, Dispatcher, ExecutorKind, QueueKind};
use bits::job::Job;
use bits::result::JobResult;
use bits::{Bits, PollOutcome};

// ================================
//   ThreadPoolExecutor via Dispatcher
// ================================

/// Work futures run on OS threads when using the thread_pool executor.
#[tokio::test]
async fn thread_pool_runs_on_os_threads() {
    let dispatcher = Dispatcher::<CheckResult>::from_config(
        Some(&QueueKind::Fifo),
        Some(&ExecutorKind::ThreadPool {
            concurrency: Some(2),
        }),
        None,
        None,
    )
    .expect("dispatcher config should not error")
    .expect("dispatcher should be created");

    let test_thread = std::thread::current().id();
    let thread_ids: Arc<Mutex<Vec<std::thread::ThreadId>>> = Arc::new(Mutex::new(Vec::new()));

    let ids1 = Arc::clone(&thread_ids);
    let job1 = Job::new(serde_json::json!({}));
    let work1: BoxFuture<'static, Result<CheckResult, ActionError>> = Box::pin(async move {
        ids1.lock().unwrap().push(std::thread::current().id());
        Ok(CheckResult::Pass)
    });

    let ids2 = Arc::clone(&thread_ids);
    let job2 = Job::new(serde_json::json!({}));
    let work2: BoxFuture<'static, Result<CheckResult, ActionError>> = Box::pin(async move {
        ids2.lock().unwrap().push(std::thread::current().id());
        Ok(CheckResult::Pass)
    });

    let (r1, r2) = tokio::join!(
        dispatcher.dispatch(&job1, DispatchGuard::None, work1),
        dispatcher.dispatch(&job2, DispatchGuard::None, work2),
    );
    assert!(r1.is_ok() && r2.is_ok());

    let ids = thread_ids.lock().unwrap();
    assert_eq!(ids.len(), 2);
    assert!(
        ids.iter().all(|id| *id != test_thread),
        "work should run on pool threads, not the test thread"
    );
}

/// Two futures can reach a barrier simultaneously, proving the pool runs
/// them concurrently. Would deadlock if only one thread ran at a time.
#[tokio::test]
async fn thread_pool_executes_concurrently() {
    let dispatcher = Dispatcher::<CheckResult>::from_config(
        Some(&QueueKind::Fifo),
        Some(&ExecutorKind::ThreadPool {
            concurrency: Some(2),
        }),
        None,
        None,
    )
    .expect("dispatcher config should not error")
    .expect("dispatcher should be created");

    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let b1 = Arc::clone(&barrier);
    let job1 = Job::new(serde_json::json!({}));
    let work1: BoxFuture<'static, Result<CheckResult, ActionError>> = Box::pin(async move {
        b1.wait().await;
        Ok(CheckResult::Pass)
    });

    let b2 = Arc::clone(&barrier);
    let job2 = Job::new(serde_json::json!({}));
    let work2: BoxFuture<'static, Result<CheckResult, ActionError>> = Box::pin(async move {
        b2.wait().await;
        Ok(CheckResult::Pass)
    });

    let (r1, r2) = tokio::join!(
        dispatcher.dispatch(&job1, DispatchGuard::None, work1),
        dispatcher.dispatch(&job2, DispatchGuard::None, work2),
    );
    assert!(r1.is_ok() && r2.is_ok());
}

// ================================
//   Full pipeline via Bits config
// ================================

/// A job routed through check → transform → target completes successfully.
#[tokio::test]
async fn pipeline_check_transform_target() {
    let _ = common::CheckDummyDelay::new(0);
    let _ = common::TransformDummyCost { cost: 0 };
    let _ = common::TargetDummyDelay::new(0);

    let config = r#"
routes:
  - default:
      - check::dummy_delay:
          duration_ms: 10
      - transform::dummy_cost:
          cost: 1
      - target::dummy_dispatch:
          duration_ms: 10
"#;

    let bits = Bits::from_config(config).unwrap();
    let handle = bits.submit(Job::new(serde_json::json!({})));
    let outcome = bits.poll(&handle.id, Some(Duration::from_secs(2))).await;

    assert!(
        matches!(outcome, PollOutcome::Ready(JobResult::Redirect { .. })),
        "expected Redirect from target, got {:?}",
        outcome
    );
}

// ================================
//   Dispatcher<CheckResult>
// ================================

/// Cheaper check is dispatched before expensive when both are waiting.
#[tokio::test]
async fn check_dispatcher_cost_weighted_ordering() {
    let dispatcher = Arc::new(
        Dispatcher::<CheckResult>::from_config(
            Some(&QueueKind::CostWeighted),
            Some(&ExecutorKind::AsyncPool {
                concurrency: Some(1),
            }),
            None,
            None,
        )
        .expect("dispatcher config should not error")
        .expect("dispatcher should be created"),
    );

    let order: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    // Blocker holds the single slot while expensive + cheap enter the heap.
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let mut blocker = Job::new(serde_json::json!({}));
    blocker.metadata["cost"] = serde_json::json!(0u64);
    let blocker_work: BoxFuture<'static, Result<CheckResult, ActionError>> = Box::pin(async move {
        let _ = release_rx.await;
        Ok(CheckResult::Pass)
    });

    let mut expensive = Job::new(serde_json::json!({}));
    expensive.metadata["cost"] = serde_json::json!(100u64);
    let order_e = Arc::clone(&order);
    let expensive_work: BoxFuture<'static, Result<CheckResult, ActionError>> =
        Box::pin(async move {
            order_e.lock().unwrap().push(100);
            Ok(CheckResult::Pass)
        });

    let mut cheap = Job::new(serde_json::json!({}));
    cheap.metadata["cost"] = serde_json::json!(1u64);
    let order_c = Arc::clone(&order);
    let cheap_work: BoxFuture<'static, Result<CheckResult, ActionError>> = Box::pin(async move {
        order_c.lock().unwrap().push(1);
        Ok(CheckResult::Pass)
    });

    let blocker_h = tokio::spawn(dispatcher.dispatch(&blocker, DispatchGuard::None, blocker_work));
    let expensive_h =
        tokio::spawn(dispatcher.dispatch(&expensive, DispatchGuard::None, expensive_work));
    let cheap_h = tokio::spawn(dispatcher.dispatch(&cheap, DispatchGuard::None, cheap_work));

    // Give expensive + cheap time to enter the cost-weighted heap.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let _ = release_tx.send(());

    blocker_h.await.unwrap().unwrap();
    expensive_h.await.unwrap().unwrap();
    cheap_h.await.unwrap().unwrap();

    assert_eq!(
        *order.lock().unwrap(),
        vec![1, 100],
        "cheap check should run before expensive"
    );
}

// ================================
//   Dispatcher<TargetResult>
// ================================

/// Cheaper target is dispatched before expensive when both are waiting.
#[tokio::test]
async fn target_dispatcher_cost_weighted_ordering() {
    let dispatcher = Arc::new(
        Dispatcher::<TargetResult>::from_config(
            Some(&QueueKind::CostWeighted),
            Some(&ExecutorKind::AsyncPool {
                concurrency: Some(1),
            }),
            None,
            None,
        )
        .expect("dispatcher config should not error")
        .expect("dispatcher should be created"),
    );

    let order: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let mut blocker = Job::new(serde_json::json!({}));
    blocker.metadata["cost"] = serde_json::json!(0u64);
    let blocker_work: BoxFuture<'static, Result<TargetResult, ActionError>> =
        Box::pin(async move {
            let _ = release_rx.await;
            Ok(TargetResult::Complete(JobResult::Error {
                message: "blocker".into(),
            }))
        });

    let mut expensive = Job::new(serde_json::json!({}));
    expensive.metadata["cost"] = serde_json::json!(100u64);
    let order_e = Arc::clone(&order);
    let expensive_work: BoxFuture<'static, Result<TargetResult, ActionError>> =
        Box::pin(async move {
            order_e.lock().unwrap().push(100);
            Ok(TargetResult::Complete(JobResult::Error {
                message: "expensive".into(),
            }))
        });

    let mut cheap = Job::new(serde_json::json!({}));
    cheap.metadata["cost"] = serde_json::json!(1u64);
    let order_c = Arc::clone(&order);
    let cheap_work: BoxFuture<'static, Result<TargetResult, ActionError>> = Box::pin(async move {
        order_c.lock().unwrap().push(1);
        Ok(TargetResult::Complete(JobResult::Error {
            message: "cheap".into(),
        }))
    });

    let blocker_h = tokio::spawn(dispatcher.dispatch(&blocker, DispatchGuard::None, blocker_work));
    let expensive_h =
        tokio::spawn(dispatcher.dispatch(&expensive, DispatchGuard::None, expensive_work));
    let cheap_h = tokio::spawn(dispatcher.dispatch(&cheap, DispatchGuard::None, cheap_work));

    tokio::time::sleep(Duration::from_millis(20)).await;
    let _ = release_tx.send(());

    blocker_h.await.unwrap().unwrap();
    expensive_h.await.unwrap().unwrap();
    cheap_h.await.unwrap().unwrap();

    assert_eq!(
        *order.lock().unwrap(),
        vec![1, 100],
        "cheap target should run before expensive"
    );
}

#[tokio::test]
async fn target_dispatcher_age_priority_promotes_waiting_expensive_job() {
    let dispatcher = Arc::new(
        Dispatcher::<TargetResult>::from_config(
            Some(&QueueKind::AgePriority),
            Some(&ExecutorKind::AsyncPool {
                concurrency: Some(1),
            }),
            None,
            None,
        )
        .expect("dispatcher config should not error")
        .expect("dispatcher should be created"),
    );

    let order: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let mut blocker = Job::new(serde_json::json!({}));
    blocker.metadata["cost"] = serde_json::json!(0u64);
    let blocker_work: BoxFuture<'static, Result<TargetResult, ActionError>> =
        Box::pin(async move {
            let _ = release_rx.await;
            Ok(TargetResult::Complete(JobResult::Error {
                message: "blocker".into(),
            }))
        });

    let blocker_h = tokio::spawn(dispatcher.dispatch(&blocker, DispatchGuard::None, blocker_work));
    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut expensive = Job::new(serde_json::json!({}));
    expensive.metadata["cost"] = serde_json::json!(100u64);
    let order_e = Arc::clone(&order);
    let expensive_work: BoxFuture<'static, Result<TargetResult, ActionError>> =
        Box::pin(async move {
            order_e.lock().unwrap().push(100);
            Ok(TargetResult::Complete(JobResult::Error {
                message: "expensive".into(),
            }))
        });
    let expensive_h =
        tokio::spawn(dispatcher.dispatch(&expensive, DispatchGuard::None, expensive_work));

    // Give expensive enough age to outrank cheap despite higher cost.
    // score = wait_time * other_cost / sqrt(own_cost).
    // expensive needs wait_time * 1 > cheap_wait_time * 10.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut cheap = Job::new(serde_json::json!({}));
    cheap.metadata["cost"] = serde_json::json!(1u64);
    let order_c = Arc::clone(&order);
    let cheap_work: BoxFuture<'static, Result<TargetResult, ActionError>> = Box::pin(async move {
        order_c.lock().unwrap().push(1);
        Ok(TargetResult::Complete(JobResult::Error {
            message: "cheap".into(),
        }))
    });
    let cheap_h = tokio::spawn(dispatcher.dispatch(&cheap, DispatchGuard::None, cheap_work));

    tokio::time::sleep(Duration::from_millis(20)).await;
    let _ = release_tx.send(());

    blocker_h.await.unwrap().unwrap();
    expensive_h.await.unwrap().unwrap();
    cheap_h.await.unwrap().unwrap();

    assert_eq!(
        *order.lock().unwrap(),
        vec![100, 1],
        "older expensive target should outrank newly-arrived cheap target once it has aged"
    );
}
