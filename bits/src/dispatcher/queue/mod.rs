// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

pub mod age_priority;
pub mod cost_weighted;
pub mod fifo;

pub use age_priority::AgePriorityQueue;
pub use cost_weighted::CostWeightedQueue;
pub use fifo::FifoQueue;

use async_trait::async_trait;

use crate::job::Job;

/// Selects the queue implementation to construct from config.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueKind {
    Fifo,
    CostWeighted,
    AgePriority,
}

/// A queue is a pure data structure: items go in via `enqueue` and come out
/// via `dequeue` in an implementation-defined order (FIFO, priority, etc.).
///
/// Weighted implementations spawn internal threads to maintain their storage
/// ordering. Those threads never pop items — only `dequeue` does that.
///
/// Calling `close` requests that the queue shut down: future `enqueue`
/// calls are silently dropped, and `dequeue` will eventually return
/// `None`. Implementations may drain or drop already-buffered items;
/// callers must not rely on all enqueued items being dequeued.
#[async_trait]
pub trait Queue: Send + Sync {
    fn enqueue(&self, job: Job);
    async fn dequeue(&self) -> Option<Job>;

    /// Shut the queue down. Future `enqueue` calls are dropped.
    /// Already-buffered items may be drained or dropped depending on
    /// the implementation. The default implementation is a no-op.
    fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fifo_queue_basic_usage() {
        let q = FifoQueue::new();

        q.enqueue(Job::new(serde_json::json!({"n": 1})));
        q.enqueue(Job::new(serde_json::json!({"n": 2})));

        let first = q.dequeue().await.unwrap();
        let second = q.dequeue().await.unwrap();

        assert_eq!(first.request["n"], 1);
        assert_eq!(second.request["n"], 2);
    }

    #[tokio::test]
    async fn cost_weighted_queue_basic_usage() {
        let q = CostWeightedQueue::new();

        let mut cheap = Job::new(serde_json::json!({}));
        cheap.metadata_mut()["cost"] = serde_json::json!(1u64);
        let mut expensive = Job::new(serde_json::json!({}));
        expensive.metadata_mut()["cost"] = serde_json::json!(99u64);

        q.enqueue(expensive);
        q.enqueue(cheap);

        // Give the worker time to ingest both before dequeuing.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let first = q.dequeue().await.unwrap();
        assert_eq!(
            first.metadata["cost"].as_u64().unwrap(),
            1,
            "cheaper job should come out first"
        );
    }

    #[tokio::test]
    async fn age_priority_queue_eventually_promotes_old_expensive_job() {
        let q = AgePriorityQueue::new();

        let mut expensive = Job::new(serde_json::json!({}));
        expensive.metadata_mut()["cost"] = serde_json::json!(100u64);
        q.enqueue(expensive);

        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        let mut cheap = Job::new(serde_json::json!({}));
        cheap.metadata_mut()["cost"] = serde_json::json!(1u64);
        q.enqueue(cheap);

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let first = q.dequeue().await.unwrap();
        assert_eq!(
            first.metadata["cost"].as_u64().unwrap(),
            100,
            "older expensive job should eventually outrank a newly-arrived cheap job"
        );
    }

    #[tokio::test]
    async fn fifo_close_returns_none_on_empty() {
        let q = FifoQueue::new();
        q.close();
        assert!(q.dequeue().await.is_none());
    }

    #[tokio::test]
    async fn fifo_close_drains_buffered_then_none() {
        let q = FifoQueue::new();
        q.enqueue(Job::new(serde_json::json!({"n": 1})));
        q.close();
        assert!(q.dequeue().await.is_some(), "buffered item should drain");
        assert!(
            q.dequeue().await.is_none(),
            "should return None after drain"
        );
    }

    #[tokio::test]
    async fn fifo_enqueue_after_close_is_dropped() {
        let q = FifoQueue::new();
        q.close();
        q.enqueue(Job::new(serde_json::json!({})));
        assert!(q.dequeue().await.is_none());
    }

    #[tokio::test]
    async fn cost_weighted_close_returns_none() {
        let q = CostWeightedQueue::new();
        q.close();
        assert!(q.dequeue().await.is_none());
    }

    #[tokio::test]
    async fn age_priority_close_returns_none() {
        let q = AgePriorityQueue::new();
        q.close();
        assert!(q.dequeue().await.is_none());
    }

    #[tokio::test]
    async fn fifo_close_unblocks_waiting_dequeue() {
        let q = std::sync::Arc::new(FifoQueue::new());
        let q2 = std::sync::Arc::clone(&q);
        let handle = tokio::spawn(async move { q2.dequeue().await });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        q.close();
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("should not timeout")
            .expect("task should not panic");
        assert!(result.is_none());
    }
}
