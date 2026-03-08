use std::collections::{BinaryHeap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

use crate::job::Job;
use crate::queue::Queue;

/// A queue that yields the cheapest waiting job first.
///
/// Cost is read from `job.metadata["cost"]` as a u64. Jobs with no cost
/// value are treated as cost 0 (highest priority).
///
/// A background task maintains the priority heap and re-orders it as new
/// items arrive. It never pops items on its own — only `dequeue` does that.
pub struct CostWeightedQueue {
    tx: mpsc::UnboundedSender<WorkerCmd>,
    seq: Arc<AtomicU64>,
}

impl std::fmt::Debug for CostWeightedQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CostWeightedQueue").finish_non_exhaustive()
    }
}

enum WorkerCmd {
    Enqueue { cost: u64, seq: u64, job: Job },
    Dequeue(oneshot::Sender<Job>),
}

// Entry in the min-heap. Ordered by cost ascending, then seq ascending (FIFO tiebreak).
struct Entry {
    cost: u64,
    seq: u64,
    job: Job,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.cost == other.cost && self.seq == other.seq
    }
}
impl Eq for Entry {}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap is a max-heap; reverse flips to min-heap by cost, then FIFO by seq.
        other.cost.cmp(&self.cost).then(other.seq.cmp(&self.seq))
    }
}

impl CostWeightedQueue {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(worker(rx));
        Self { tx, seq: Arc::new(AtomicU64::new(0)) }
    }
}

impl Default for CostWeightedQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// The background worker shuffles the priority heap as items arrive and
/// satisfies pending `dequeue` requests. It never pops from the heap
/// spontaneously — a `Dequeue` command is required.
async fn worker(mut rx: mpsc::UnboundedReceiver<WorkerCmd>) {
    let mut heap: BinaryHeap<Entry> = BinaryHeap::new();
    let mut waiters: VecDeque<oneshot::Sender<Job>> = VecDeque::new();

    while let Some(cmd) = rx.recv().await {
        match cmd {
            WorkerCmd::Enqueue { cost, seq, job } => {
                heap.push(Entry { cost, seq, job });
            }
            WorkerCmd::Dequeue(reply) => {
                waiters.push_back(reply);
            }
        }

        // Match pending dequeue requests against the heap in priority order.
        // The heap is the authoritative ordering; only this path pops from it.
        while !waiters.is_empty() && !heap.is_empty() {
            let reply = waiters.pop_front().unwrap();
            let entry = heap.pop().unwrap();
            if reply.send(entry.job).is_err() {
                // Caller cancelled — skip and try next waiter.
            }
        }
    }
}

#[async_trait]
impl Queue for CostWeightedQueue {
    fn enqueue(&self, job: Job) {
        let cost = job.metadata["cost"].as_u64().unwrap_or(0);
        let seq = self.seq.fetch_add(1, AtomicOrdering::Relaxed);
        let _ = self.tx.send(WorkerCmd::Enqueue { cost, seq, job });
    }

    async fn dequeue(&self) -> Option<Job> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(WorkerCmd::Dequeue(reply_tx)).ok()?;
        reply_rx.await.ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::task::JoinSet;

    fn job_with_cost(cost: u64) -> Job {
        let mut job = Job::new(serde_json::json!({}));
        job.metadata["cost"] = serde_json::json!(cost);
        job
    }

    #[tokio::test]
    async fn enqueue_then_dequeue_roundtrip() {
        let q = CostWeightedQueue::new();
        q.enqueue(job_with_cost(10));
        let job = q.dequeue().await.unwrap();
        assert_eq!(job.metadata["cost"].as_u64().unwrap(), 10);
    }

    #[tokio::test]
    async fn cheap_dequeued_before_expensive() {
        let q = CostWeightedQueue::new();
        // Enqueue both before any dequeue so the worker can sort them.
        q.enqueue(job_with_cost(100));
        q.enqueue(job_with_cost(1));

        // Give the worker a moment to insert both into the heap.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let first = q.dequeue().await.unwrap();
        let second = q.dequeue().await.unwrap();

        assert_eq!(first.metadata["cost"].as_u64().unwrap(), 1, "cheap should come first");
        assert_eq!(second.metadata["cost"].as_u64().unwrap(), 100);
    }

    #[tokio::test]
    async fn fifo_tiebreak_for_equal_cost() {
        let q = CostWeightedQueue::new();
        let order: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

        // Enqueue three jobs with the same cost in staggered order.
        let mut set = JoinSet::new();
        for seq in [1u64, 2, 3] {
            let q_tx = q.tx.clone();
            let q_seq = Arc::clone(&q.seq);
            set.spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(seq * 5)).await;
                let cost = 10u64;
                let s = q_seq.fetch_add(1, AtomicOrdering::Relaxed);
                let mut job = Job::new(serde_json::json!({}));
                job.metadata["cost"] = serde_json::json!(cost);
                job.metadata["seq_label"] = serde_json::json!(seq);
                let _ = q_tx.send(WorkerCmd::Enqueue { cost, seq: s, job });
            });
        }
        while set.join_next().await.is_some() {}

        // Give worker time to insert all three.
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        for _ in 0..3 {
            let job = q.dequeue().await.unwrap();
            order.lock().unwrap().push(job.metadata["seq_label"].as_u64().unwrap());
        }

        assert_eq!(*order.lock().unwrap(), vec![1, 2, 3], "equal cost jobs should be FIFO");
    }
}
