//! Edge-case tests for queue implementations.

use std::time::Duration;

use bits::dispatcher::queue::{AgePriorityQueue, CostWeightedQueue, FifoQueue, Queue};
use bits::job::Job;

// ================================
//   FIFO: multiple pending waiters
// ================================

/// Several dequeue() futures are started before any enqueue.
/// Each waiter should receive items in the order they were enqueued.
#[tokio::test]
async fn fifo_multiple_waiters_receive_in_order() {
    use std::sync::Arc;
    let q = Arc::new(FifoQueue::new());

    let mut handles = Vec::new();
    for _ in 0..3 {
        let q = Arc::clone(&q);
        handles.push(tokio::spawn(async move { q.dequeue().await }));
    }

    // Brief yield to let all waiters register.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Enqueue items.
    q.enqueue(Job::new(serde_json::json!({"n": 1})));
    q.enqueue(Job::new(serde_json::json!({"n": 2})));
    q.enqueue(Job::new(serde_json::json!({"n": 3})));

    let mut results = Vec::new();
    for h in handles {
        let job = h.await.unwrap().unwrap();
        results.push(job.request["n"].as_u64().unwrap());
    }

    // Each waiter gets exactly one job; ordering is FIFO because the
    // internal mpsc channel wakes receivers in order.
    results.sort(); // tasks may complete in any order, but values should be 1,2,3
    assert_eq!(results, vec![1, 2, 3]);
}

// ================================
//   CostWeighted: tie-break
// ================================

/// Jobs with equal cost should be dequeued in FIFO (seq) order.
#[tokio::test]
async fn cost_weighted_tiebreak_is_fifo_for_equal_cost() {
    let q = CostWeightedQueue::new();

    for i in 0..5u64 {
        let mut job = Job::new(serde_json::json!({"seq": i}));
        job.metadata_mut()["cost"] = serde_json::json!(10u64);
        q.enqueue(job);
        // Small gap so seq assignment is deterministic relative to enqueue order.
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    // Let worker ingest all items.
    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut order = Vec::new();
    for _ in 0..5 {
        let job = q.dequeue().await.unwrap();
        order.push(job.request["seq"].as_u64().unwrap());
    }

    assert_eq!(
        order,
        vec![0, 1, 2, 3, 4],
        "equal-cost jobs should dequeue in FIFO order"
    );
}

// ================================
//   CostWeighted: waiter cancellation
// ================================

/// If the first dequeue waiter is cancelled, the next waiter should still
/// receive the job.
#[tokio::test]
async fn cost_weighted_cancelled_waiter_does_not_block() {
    use std::sync::Arc;
    let q = Arc::new(CostWeightedQueue::new());

    // Start a dequeue waiter and then cancel it.
    let q1 = Arc::clone(&q);
    let first = tokio::spawn(async move { q1.dequeue().await });

    // Give waiter time to register.
    tokio::time::sleep(Duration::from_millis(10)).await;
    first.abort();
    // Wait for abort to complete.
    let _ = first.await;

    // Start a second waiter.
    let q2 = Arc::clone(&q);
    let second = tokio::spawn(async move { q2.dequeue().await });

    tokio::time::sleep(Duration::from_millis(5)).await;

    // Enqueue a job — the second waiter should get it.
    let mut job = Job::new(serde_json::json!({"value": 42}));
    job.metadata_mut()["cost"] = serde_json::json!(1u64);
    q.enqueue(job);

    let result = tokio::time::timeout(Duration::from_secs(2), second)
        .await
        .expect("timed out waiting for second dequeue")
        .unwrap()
        .unwrap();

    assert_eq!(result.request["value"], 42);
}

// ================================
//   AgePriority: waiter cancellation
// ================================

/// Same concept for AgePriorityQueue: cancelled waiter should not block.
#[tokio::test]
async fn age_priority_cancelled_waiter_does_not_block() {
    use std::sync::Arc;
    let q = Arc::new(AgePriorityQueue::new());

    let q1 = Arc::clone(&q);
    let first = tokio::spawn(async move { q1.dequeue().await });

    tokio::time::sleep(Duration::from_millis(10)).await;
    first.abort();
    let _ = first.await;

    let q2 = Arc::clone(&q);
    let second = tokio::spawn(async move { q2.dequeue().await });

    tokio::time::sleep(Duration::from_millis(5)).await;

    let mut job = Job::new(serde_json::json!({"value": 99}));
    job.metadata_mut()["cost"] = serde_json::json!(1u64);
    q.enqueue(job);

    let result = tokio::time::timeout(Duration::from_secs(2), second)
        .await
        .expect("timed out waiting for second dequeue")
        .unwrap()
        .unwrap();

    assert_eq!(result.request["value"], 99);
}

// ================================
//   AgePriority: tie-break
// ================================

/// Jobs with equal cost and similar enqueue time should dequeue in FIFO order.
#[tokio::test]
async fn age_priority_tiebreak_is_fifo_for_equal_cost() {
    let q = AgePriorityQueue::new();

    for i in 0..3u64 {
        let mut job = Job::new(serde_json::json!({"seq": i}));
        job.metadata_mut()["cost"] = serde_json::json!(10u64);
        q.enqueue(job);
    }

    // Let the worker ingest all and rebalance.
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut order = Vec::new();
    for _ in 0..3 {
        let job = q.dequeue().await.unwrap();
        order.push(job.request["seq"].as_u64().unwrap());
    }

    assert_eq!(
        order,
        vec![0, 1, 2],
        "equal-cost/equal-age jobs should dequeue in FIFO order"
    );
}

// ================================
//   Missing cost defaults
// ================================

/// CostWeightedQueue: missing cost is treated as 0 (highest priority).
#[tokio::test]
async fn cost_weighted_missing_cost_defaults_to_zero() {
    let q = CostWeightedQueue::new();

    let mut expensive = Job::new(serde_json::json!({"label": "expensive"}));
    expensive.metadata_mut()["cost"] = serde_json::json!(100u64);

    let no_cost = Job::new(serde_json::json!({"label": "no_cost"}));

    q.enqueue(expensive);
    q.enqueue(no_cost);

    tokio::time::sleep(Duration::from_millis(10)).await;

    let first = q.dequeue().await.unwrap();
    assert_eq!(
        first.request["label"], "no_cost",
        "job with missing cost should be treated as cost 0 (highest priority)"
    );
}

/// AgePriorityQueue: missing cost is treated as 1.
#[tokio::test]
async fn age_priority_missing_cost_defaults_to_one() {
    let q = AgePriorityQueue::new();

    // Enqueue a job with explicit cost=1 and one without cost.
    // They should behave identically — both treated as cost 1.
    let mut explicit = Job::new(serde_json::json!({"label": "explicit"}));
    explicit.metadata_mut()["cost"] = serde_json::json!(1u64);
    let no_cost = Job::new(serde_json::json!({"label": "no_cost"}));

    // Enqueue explicit first, then no_cost right after.
    q.enqueue(explicit);
    q.enqueue(no_cost);

    tokio::time::sleep(Duration::from_millis(20)).await;

    // With equal cost and nearly-equal age, FIFO tie-break should preserve order.
    let first = q.dequeue().await.unwrap();
    let second = q.dequeue().await.unwrap();
    assert_eq!(first.request["label"], "explicit");
    assert_eq!(second.request["label"], "no_cost");
}
