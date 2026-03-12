//! Edge-case tests for dispatcher/executor scheduling behaviour.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;

use bits::actions::{ActionError, CheckResult};
use bits::dispatcher::{Dispatcher, ExecutorKind, QueueKind};
use bits::job::Job;

// ================================
//   Concurrency limits
// ================================

/// With concurrency 2 and 4 jobs, at most 2 should be running at any instant.
#[tokio::test]
async fn async_pool_respects_concurrency_limit() {
    let dispatcher = Arc::new(
        Dispatcher::<CheckResult>::from_config(
            Some(&QueueKind::Fifo),
            Some(&ExecutorKind::AsyncPool),
            Some(2),
        )
        .unwrap(),
    );

    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..4 {
        let running = Arc::clone(&running);
        let peak = Arc::clone(&peak);
        let job = Job::new(serde_json::json!({}));
        let work: BoxFuture<'static, Result<CheckResult, ActionError>> = Box::pin(async move {
            let prev = running.fetch_add(1, Ordering::SeqCst);
            peak.fetch_max(prev + 1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            running.fetch_sub(1, Ordering::SeqCst);
            Ok(CheckResult::Pass)
        });
        handles.push(tokio::spawn(dispatcher.dispatch(&job, work)));
    }

    for h in handles {
        h.await.unwrap().unwrap();
    }

    assert!(
        peak.load(Ordering::SeqCst) <= 2,
        "peak concurrency {} exceeded limit of 2",
        peak.load(Ordering::SeqCst)
    );
}

/// Same shape for thread_pool executor.
#[tokio::test]
async fn thread_pool_respects_concurrency_limit() {
    let dispatcher = Arc::new(
        Dispatcher::<CheckResult>::from_config(
            Some(&QueueKind::Fifo),
            Some(&ExecutorKind::ThreadPool),
            Some(2),
        )
        .unwrap(),
    );

    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..4 {
        let running = Arc::clone(&running);
        let peak = Arc::clone(&peak);
        let job = Job::new(serde_json::json!({}));
        let work: BoxFuture<'static, Result<CheckResult, ActionError>> = Box::pin(async move {
            let prev = running.fetch_add(1, Ordering::SeqCst);
            peak.fetch_max(prev + 1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            running.fetch_sub(1, Ordering::SeqCst);
            Ok(CheckResult::Pass)
        });
        handles.push(tokio::spawn(dispatcher.dispatch(&job, work)));
    }

    for h in handles {
        h.await.unwrap().unwrap();
    }

    assert!(
        peak.load(Ordering::SeqCst) <= 2,
        "peak concurrency {} exceeded limit of 2",
        peak.load(Ordering::SeqCst)
    );
}

// ================================
//   FIFO ordering
// ================================

/// async_pool + fifo preserves enqueue order when concurrency is 1.
#[tokio::test]
async fn async_pool_fifo_preserves_order() {
    let dispatcher = Arc::new(
        Dispatcher::<CheckResult>::from_config(
            Some(&QueueKind::Fifo),
            Some(&ExecutorKind::AsyncPool),
            Some(1),
        )
        .unwrap(),
    );

    let order: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));

    let mut handles = Vec::new();
    for i in 0..5u32 {
        let order = Arc::clone(&order);
        let job = Job::new(serde_json::json!({"n": i}));
        let work: BoxFuture<'static, Result<CheckResult, ActionError>> = Box::pin(async move {
            order.lock().unwrap().push(i);
            Ok(CheckResult::Pass)
        });
        handles.push(tokio::spawn(dispatcher.dispatch(&job, work)));
        // Small delay to ensure enqueue order is deterministic.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    for h in handles {
        h.await.unwrap().unwrap();
    }

    assert_eq!(
        *order.lock().unwrap(),
        vec![0, 1, 2, 3, 4],
        "FIFO order not preserved"
    );
}

/// thread_pool + fifo preserves enqueue order when concurrency is 1.
#[tokio::test]
async fn thread_pool_fifo_preserves_order() {
    let dispatcher = Arc::new(
        Dispatcher::<CheckResult>::from_config(
            Some(&QueueKind::Fifo),
            Some(&ExecutorKind::ThreadPool),
            Some(1),
        )
        .unwrap(),
    );

    let order: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));

    let mut handles = Vec::new();
    for i in 0..5u32 {
        let order = Arc::clone(&order);
        let job = Job::new(serde_json::json!({"n": i}));
        let work: BoxFuture<'static, Result<CheckResult, ActionError>> = Box::pin(async move {
            order.lock().unwrap().push(i);
            Ok(CheckResult::Pass)
        });
        handles.push(tokio::spawn(dispatcher.dispatch(&job, work)));
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    for h in handles {
        h.await.unwrap().unwrap();
    }

    assert_eq!(
        *order.lock().unwrap(),
        vec![0, 1, 2, 3, 4],
        "FIFO order not preserved"
    );
}

// ================================
//   Cross-product: queue × executor
// ================================

/// cost_weighted + thread_pool: cheap job runs before expensive.
#[tokio::test]
async fn thread_pool_cost_weighted_ordering() {
    let dispatcher = Arc::new(
        Dispatcher::<CheckResult>::from_config(
            Some(&QueueKind::CostWeighted),
            Some(&ExecutorKind::ThreadPool),
            Some(1),
        )
        .unwrap(),
    );

    let order: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

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

    let blocker_h = tokio::spawn(dispatcher.dispatch(&blocker, blocker_work));
    let expensive_h = tokio::spawn(dispatcher.dispatch(&expensive, expensive_work));
    let cheap_h = tokio::spawn(dispatcher.dispatch(&cheap, cheap_work));

    tokio::time::sleep(Duration::from_millis(20)).await;
    let _ = release_tx.send(());

    blocker_h.await.unwrap().unwrap();
    expensive_h.await.unwrap().unwrap();
    cheap_h.await.unwrap().unwrap();

    assert_eq!(
        *order.lock().unwrap(),
        vec![1, 100],
        "cheap should run before expensive with thread_pool + cost_weighted"
    );
}

/// age_priority + thread_pool: older expensive job eventually outranks newer cheap job.
#[tokio::test]
async fn thread_pool_age_priority_ordering() {
    let dispatcher = Arc::new(
        Dispatcher::<CheckResult>::from_config(
            Some(&QueueKind::AgePriority),
            Some(&ExecutorKind::ThreadPool),
            Some(1),
        )
        .unwrap(),
    );

    let order: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let mut blocker = Job::new(serde_json::json!({}));
    blocker.metadata["cost"] = serde_json::json!(0u64);
    let blocker_work: BoxFuture<'static, Result<CheckResult, ActionError>> = Box::pin(async move {
        let _ = release_rx.await;
        Ok(CheckResult::Pass)
    });

    let blocker_h = tokio::spawn(dispatcher.dispatch(&blocker, blocker_work));
    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut expensive = Job::new(serde_json::json!({}));
    expensive.metadata["cost"] = serde_json::json!(100u64);
    let order_e = Arc::clone(&order);
    let expensive_work: BoxFuture<'static, Result<CheckResult, ActionError>> =
        Box::pin(async move {
            order_e.lock().unwrap().push(100);
            Ok(CheckResult::Pass)
        });
    let expensive_h = tokio::spawn(dispatcher.dispatch(&expensive, expensive_work));

    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut cheap = Job::new(serde_json::json!({}));
    cheap.metadata["cost"] = serde_json::json!(1u64);
    let order_c = Arc::clone(&order);
    let cheap_work: BoxFuture<'static, Result<CheckResult, ActionError>> = Box::pin(async move {
        order_c.lock().unwrap().push(1);
        Ok(CheckResult::Pass)
    });
    let cheap_h = tokio::spawn(dispatcher.dispatch(&cheap, cheap_work));

    tokio::time::sleep(Duration::from_millis(20)).await;
    let _ = release_tx.send(());

    blocker_h.await.unwrap().unwrap();
    expensive_h.await.unwrap().unwrap();
    cheap_h.await.unwrap().unwrap();

    assert_eq!(
        *order.lock().unwrap(),
        vec![100, 1],
        "older expensive job should outrank newer cheap with thread_pool + age_priority"
    );
}

// ================================
//   Config defaults
// ================================

/// from_config returns None when no settings are provided.
#[tokio::test]
async fn dispatcher_from_config_none_when_no_settings() {
    let result = Dispatcher::<CheckResult>::from_config(None, None, None);
    assert!(result.is_none(), "expected None when all settings are None");
}

/// Default executor (None) behaves like async_pool — work runs on Tokio tasks,
/// not the caller's thread.
#[tokio::test]
async fn dispatcher_default_executor_is_async_pool() {
    let dispatcher = Dispatcher::<CheckResult>::from_config(
        Some(&QueueKind::Fifo),
        None, // default executor
        Some(1),
    )
    .unwrap();

    let job = Job::new(serde_json::json!({}));
    let work: BoxFuture<'static, Result<CheckResult, ActionError>> =
        Box::pin(async { Ok(CheckResult::Pass) });

    let result = dispatcher.dispatch(&job, work).await;
    assert!(result.is_ok(), "default executor should run work successfully");
}

/// When only concurrency is set (no queue), FIFO is used by default.
#[tokio::test]
async fn dispatcher_default_queue_is_fifo() {
    let dispatcher = Arc::new(
        Dispatcher::<CheckResult>::from_config(
            None,                            // default queue
            Some(&ExecutorKind::AsyncPool),
            Some(1),
        )
        .unwrap(),
    );

    let order: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));

    let mut handles = Vec::new();
    for i in 0..3u32 {
        let order = Arc::clone(&order);
        let job = Job::new(serde_json::json!({"n": i}));
        let work: BoxFuture<'static, Result<CheckResult, ActionError>> = Box::pin(async move {
            order.lock().unwrap().push(i);
            Ok(CheckResult::Pass)
        });
        handles.push(tokio::spawn(dispatcher.dispatch(&job, work)));
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    for h in handles {
        h.await.unwrap().unwrap();
    }

    assert_eq!(
        *order.lock().unwrap(),
        vec![0, 1, 2],
        "default queue should be FIFO"
    );
}

// ================================
//   Cancellation / race paths
// ================================

/// If the caller drops the dispatch future before the work is dequeued,
/// the work should never run.
#[tokio::test]
async fn async_pool_skips_work_if_caller_drops_before_dequeue() {
    let dispatcher = Arc::new(
        Dispatcher::<CheckResult>::from_config(
            Some(&QueueKind::Fifo),
            Some(&ExecutorKind::AsyncPool),
            Some(1),
        )
        .unwrap(),
    );

    let ran = Arc::new(AtomicUsize::new(0));

    // Fill the single slot with a blocker so subsequent jobs queue up.
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let blocker = Job::new(serde_json::json!({}));
    let blocker_work: BoxFuture<'static, Result<CheckResult, ActionError>> = Box::pin(async move {
        let _ = release_rx.await;
        Ok(CheckResult::Pass)
    });
    let blocker_h = tokio::spawn(dispatcher.dispatch(&blocker, blocker_work));

    // Give blocker time to be dequeued and start running.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Enqueue a job and immediately drop (cancel) the dispatch future.
    let ran_cancelled = Arc::clone(&ran);
    let cancelled_job = Job::new(serde_json::json!({}));
    let cancelled_work: BoxFuture<'static, Result<CheckResult, ActionError>> =
        Box::pin(async move {
            ran_cancelled.fetch_add(1, Ordering::SeqCst);
            Ok(CheckResult::Pass)
        });
    {
        let fut = dispatcher.dispatch(&cancelled_job, cancelled_work);
        // Pin and poll once so the pending entry + enqueue actually happen.
        let mut pinned = Box::pin(fut);
        let _ = futures::poll!(&mut pinned);
        // Drop `pinned` here — the reply_rx is dropped, closing reply_tx.
    }

    // Now enqueue a normal job that should actually run.
    let ran_normal = Arc::clone(&ran);
    let normal_job = Job::new(serde_json::json!({}));
    let normal_work: BoxFuture<'static, Result<CheckResult, ActionError>> = Box::pin(async move {
        ran_normal.fetch_add(10, Ordering::SeqCst);
        Ok(CheckResult::Pass)
    });
    let normal_h = tokio::spawn(dispatcher.dispatch(&normal_job, normal_work));

    // Release blocker.
    let _ = release_tx.send(());
    blocker_h.await.unwrap().unwrap();
    normal_h.await.unwrap().unwrap();

    // The cancelled work adds 1; the normal work adds 10.
    // We expect only 10 (normal ran, cancelled didn't).
    assert_eq!(
        ran.load(Ordering::SeqCst),
        10,
        "cancelled work should not have run"
    );
}

/// Same shape for thread_pool executor.
#[tokio::test]
async fn thread_pool_skips_work_if_caller_drops_before_dequeue() {
    let dispatcher = Arc::new(
        Dispatcher::<CheckResult>::from_config(
            Some(&QueueKind::Fifo),
            Some(&ExecutorKind::ThreadPool),
            Some(1),
        )
        .unwrap(),
    );

    let ran = Arc::new(AtomicUsize::new(0));

    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let blocker = Job::new(serde_json::json!({}));
    let blocker_work: BoxFuture<'static, Result<CheckResult, ActionError>> = Box::pin(async move {
        let _ = release_rx.await;
        Ok(CheckResult::Pass)
    });
    let blocker_h = tokio::spawn(dispatcher.dispatch(&blocker, blocker_work));

    tokio::time::sleep(Duration::from_millis(10)).await;

    let ran_cancelled = Arc::clone(&ran);
    let cancelled_job = Job::new(serde_json::json!({}));
    let cancelled_work: BoxFuture<'static, Result<CheckResult, ActionError>> =
        Box::pin(async move {
            ran_cancelled.fetch_add(1, Ordering::SeqCst);
            Ok(CheckResult::Pass)
        });
    {
        let fut = dispatcher.dispatch(&cancelled_job, cancelled_work);
        let mut pinned = Box::pin(fut);
        let _ = futures::poll!(&mut pinned);
    }

    let ran_normal = Arc::clone(&ran);
    let normal_job = Job::new(serde_json::json!({}));
    let normal_work: BoxFuture<'static, Result<CheckResult, ActionError>> = Box::pin(async move {
        ran_normal.fetch_add(10, Ordering::SeqCst);
        Ok(CheckResult::Pass)
    });
    let normal_h = tokio::spawn(dispatcher.dispatch(&normal_job, normal_work));

    let _ = release_tx.send(());
    blocker_h.await.unwrap().unwrap();
    normal_h.await.unwrap().unwrap();

    assert_eq!(
        ran.load(Ordering::SeqCst),
        10,
        "cancelled work should not have run"
    );
}
