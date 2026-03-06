mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bits::job::Job;
use bits::actions::{CheckAction, TargetAction};
use tokio::task::JoinSet;

// ================================
//   CheckDummyDelay
// ================================

#[tokio::test]
async fn check_dummy_passes_after_delay() {
    let action = common::CheckDummyDelay::new(10);
    let job = Job::new(serde_json::json!({}));
    let result = action.evaluate(&job).await.unwrap();
    assert!(matches!(result, bits::actions::CheckResult::Pass));
}

#[tokio::test]
async fn check_dummy_serialises_concurrent_callers() {
    let action = Arc::new(common::CheckDummyDelay::new(50));
    let start = Instant::now();

    let mut set = JoinSet::new();
    for _ in 0..2 {
        let a = Arc::clone(&action);
        set.spawn(async move {
            let job = Job::new(serde_json::json!({}));
            a.evaluate(&job).await
        });
    }
    while let Some(res) = set.join_next().await {
        res.unwrap().unwrap();
    }

    assert!(start.elapsed().as_millis() >= 100, "jobs should have run sequentially");
}

// ================================
//   TargetDummyDelay
// ================================

#[tokio::test]
async fn target_dummy_dispatches_successfully() {
    let action = common::TargetDummyDelay::new(10, 1);
    let job = Job::new(serde_json::json!({}));
    let result = action.dispatch(&job).await.unwrap();
    assert!(matches!(result, bits::actions::TargetResult::Complete(_)));
}

#[tokio::test]
async fn target_dummy_serialises_when_concurrency_is_one() {
    let action = Arc::new(common::TargetDummyDelay::new(50, 1));
    let start = Instant::now();

    let mut set = JoinSet::new();
    for _ in 0..2 {
        let a = Arc::clone(&action);
        set.spawn(async move {
            let job = Job::new(serde_json::json!({}));
            a.dispatch(&job).await
        });
    }
    while let Some(res) = set.join_next().await {
        res.unwrap().unwrap();
    }

    assert!(start.elapsed().as_millis() >= 100, "jobs should have run sequentially");
}

#[tokio::test]
async fn target_dummy_cheap_job_queue_jumps_expensive() {
    // Job 1 runs immediately and holds the slot.
    // While job 1 runs, job 2 (expensive) and job 3 (cheap) both queue up.
    // When job 1 finishes the scheduler should admit job 3 before job 2.
    let action = Arc::new(common::TargetDummyDelay::new(80, 1));
    let order: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    let mut set = JoinSet::new();

    // Job 1 — runs immediately, holds the slot for 80ms.
    {
        let a = Arc::clone(&action);
        let order = Arc::clone(&order);
        set.spawn(async move {
            let mut job = Job::new(serde_json::json!({}));
            job.metadata["cost"] = serde_json::json!(50u64);
            a.dispatch(&job).await.unwrap();
            order.lock().unwrap().push(50);
        });
    }

    // Let job 1 acquire the slot before submitting the others.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Job 2 — expensive, enters heap while slot is taken.
    {
        let a = Arc::clone(&action);
        let order = Arc::clone(&order);
        set.spawn(async move {
            let mut job = Job::new(serde_json::json!({}));
            job.metadata["cost"] = serde_json::json!(100u64);
            a.dispatch(&job).await.unwrap();
            order.lock().unwrap().push(100);
        });
    }

    // Brief stagger so job 2 enters the heap before job 3.
    tokio::time::sleep(Duration::from_millis(5)).await;

    // Job 3 — cheap, enters heap after job 2 but should be admitted first.
    {
        let a = Arc::clone(&action);
        let order = Arc::clone(&order);
        set.spawn(async move {
            let mut job = Job::new(serde_json::json!({}));
            job.metadata["cost"] = serde_json::json!(1u64);
            a.dispatch(&job).await.unwrap();
            order.lock().unwrap().push(1);
        });
    }

    while set.join_next().await.is_some() {}

    assert_eq!(*order.lock().unwrap(), vec![50, 1, 100]);
}
