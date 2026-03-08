pub mod cost_weighted;
pub mod fifo;

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
}

/// A queue is a pure data structure: items go in via `enqueue` and come out
/// via `dequeue` in an implementation-defined order (FIFO, priority, etc.).
///
/// Weighted implementations spawn internal threads to maintain their storage
/// ordering. Those threads never pop items — only `dequeue` does that.
#[async_trait]
pub trait Queue: Send + Sync {
    fn enqueue(&self, job: Job);
    async fn dequeue(&self) -> Option<Job>;
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
        cheap.metadata["cost"] = serde_json::json!(1u64);
        let mut expensive = Job::new(serde_json::json!({}));
        expensive.metadata["cost"] = serde_json::json!(99u64);

        q.enqueue(expensive);
        q.enqueue(cheap);

        // Give the worker time to ingest both before dequeuing.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let first = q.dequeue().await.unwrap();
        assert_eq!(first.metadata["cost"].as_u64().unwrap(), 1, "cheaper job should come out first");
    }
}
