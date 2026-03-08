mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

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

// ================================
//   TargetDummyDelay
// ================================

#[tokio::test]
async fn target_dummy_dispatches_successfully() {
    let action = common::TargetDummyDelay::new(10);
    let job = Job::new(serde_json::json!({}));
    let result = action.dispatch(&job).await.unwrap();
    assert!(matches!(result, bits::actions::TargetResult::Complete(_)));
}

#[tokio::test]
async fn target_dummy_cheap_job_dequeued_before_expensive() {
    // Enqueue an expensive job and a cheap job while a blocker holds the
    // queue "busy". The cheap job should be dequeued first.
    //
    // We drive this directly against the queue rather than through dispatch
    // so we can control exactly when items enter and are consumed.
    use bits::dispatcher::queue::{CostWeightedQueue, Queue};

    let q = Arc::new(CostWeightedQueue::new());
    let order: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    // Prime the queue with an expensive job and a cheap job.
    let mut expensive = Job::new(serde_json::json!({}));
    expensive.metadata["cost"] = serde_json::json!(100u64);
    let mut cheap = Job::new(serde_json::json!({}));
    cheap.metadata["cost"] = serde_json::json!(1u64);

    q.enqueue(expensive);
    // Brief pause so the expensive job arrives in the heap first.
    tokio::time::sleep(Duration::from_millis(5)).await;
    q.enqueue(cheap);

    // Give the worker time to ingest both before we start dequeuing.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Drain the queue; items should come out cheapest-first.
    let mut set = JoinSet::new();
    for _ in 0..2 {
        let q = Arc::clone(&q);
        let order = Arc::clone(&order);
        set.spawn(async move {
            let job = q.dequeue().await.unwrap();
            order.lock().unwrap().push(job.metadata["cost"].as_u64().unwrap());
        });
    }
    while set.join_next().await.is_some() {}

    assert_eq!(*order.lock().unwrap(), vec![1, 100], "cheap job should be dequeued first");
}
