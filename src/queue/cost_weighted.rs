use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

use crate::actions::ActionError;
use crate::job::Job;
use crate::queue::Queue;

/// A queue that admits the cheapest waiting job first.
///
/// Cost is read from `job.metadata["cost"]` as a u64. Jobs with no cost
/// value are treated as cost 0 (highest priority).
///
/// Up to `concurrency` jobs run simultaneously. A background task maintains
/// the priority heap and signals waiting callers when a slot opens.
pub struct CostWeightedQueue {
    tx: mpsc::UnboundedSender<WorkerCmd>,
    seq: Arc<AtomicU64>,
}

impl std::fmt::Debug for CostWeightedQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CostWeightedQueue").finish_non_exhaustive()
    }
}

pub struct Permit {
    done_tx: mpsc::UnboundedSender<WorkerCmd>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let _ = self.done_tx.send(WorkerCmd::Done);
    }
}

enum WorkerCmd {
    Enqueue { cost: u64, seq: u64, signal: oneshot::Sender<()> },
    Done,
}

// Entry in the min-heap. Ordered by cost ascending, then seq ascending (FIFO tiebreak).
struct Entry {
    cost: u64,
    seq: u64,
    signal: oneshot::Sender<()>,
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
        // BinaryHeap is a max-heap; Reverse flips to min-heap by cost, then FIFO by seq.
        other.cost.cmp(&self.cost).then(other.seq.cmp(&self.seq))
    }
}

impl CostWeightedQueue {
    pub fn new(concurrency: usize) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let tx_clone = tx.clone();
        tokio::spawn(worker(rx, tx_clone, concurrency));
        Self { tx, seq: Arc::new(AtomicU64::new(0)) }
    }
}

async fn worker(
    mut rx: mpsc::UnboundedReceiver<WorkerCmd>,
    done_tx: mpsc::UnboundedSender<WorkerCmd>,
    concurrency: usize,
) {
    let mut in_flight: usize = 0;
    let mut heap: BinaryHeap<Entry> = BinaryHeap::new();

    while let Some(cmd) = rx.recv().await {
        match cmd {
            WorkerCmd::Enqueue { cost, seq, signal } => {
                heap.push(Entry { cost, seq, signal });
            }
            WorkerCmd::Done => {
                in_flight = in_flight.saturating_sub(1);
            }
        }

        // Dispatch as many waiting jobs as capacity allows.
        while in_flight < concurrency {
            match heap.pop() {
                None => break,
                Some(entry) => {
                    if entry.signal.send(()).is_ok() {
                        in_flight += 1;
                    }
                    // If send failed the caller cancelled — skip and try next.
                }
            }
        }
    }

    drop(done_tx);
}

#[async_trait]

impl Queue for CostWeightedQueue {
    type Permit = Permit;

    async fn acquire(&self, job: &Job) -> Result<Self::Permit, ActionError> {
        let cost = job.metadata["cost"].as_u64().unwrap_or(0);
        let seq = self.seq.fetch_add(1, AtomicOrdering::Relaxed);

        let (signal_tx, signal_rx) = oneshot::channel();
        self.tx
            .send(WorkerCmd::Enqueue { cost, seq, signal: signal_tx })
            .map_err(|_| ActionError::ResourceError("scheduler closed".into()))?;

        signal_rx
            .await
            .map_err(|_| ActionError::ResourceError("scheduler closed".into()))?;

        Ok(Permit { done_tx: self.tx.clone() })
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
    async fn admits_single_job_immediately() {
        let q = Arc::new(CostWeightedQueue::new(1));
        let job = job_with_cost(10);
        let _permit = q.acquire(&job).await.unwrap();
    }

    #[tokio::test]
    async fn cheap_admitted_before_expensive_when_both_waiting() {
        // Fill the slot with a blocker, enqueue expensive then cheap, release blocker.
        // The scheduler should pick cheap (cost=1) before expensive (cost=100).
        let q = Arc::new(CostWeightedQueue::new(1));
        let order: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

        // Occupy the sole slot.
        let blocker = job_with_cost(0);
        let hold = q.acquire(&blocker).await.unwrap();

        // Enqueue expensive first, then cheap, both will block on the held slot.
        let mut set = JoinSet::new();
        for cost in [100u64, 1u64] {
            let q = Arc::clone(&q);
            let order = Arc::clone(&order);
            set.spawn(async move {
                let permit = q.acquire(&job_with_cost(cost)).await.unwrap();
                order.lock().unwrap().push(cost);
                drop(permit);
            });
        }

        // Give both tasks time to enter the heap before releasing.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        drop(hold);

        while set.join_next().await.is_some() {}

        assert_eq!(*order.lock().unwrap(), vec![1, 100], "cheap should be admitted first");
    }

    #[tokio::test]
    async fn fifo_tiebreak_for_equal_cost() {
        let q = Arc::new(CostWeightedQueue::new(1));
        let order: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

        let hold = q.acquire(&job_with_cost(0)).await.unwrap();

        // Submit three jobs with the same cost in sequence.
        let mut set = JoinSet::new();
        for seq in [1u64, 2, 3] {
            let q = Arc::clone(&q);
            let order = Arc::clone(&order);
            set.spawn(async move {
                // Stagger slightly to guarantee submission order.
                tokio::time::sleep(std::time::Duration::from_millis(seq * 5)).await;
                let permit = q.acquire(&job_with_cost(10)).await.unwrap();
                order.lock().unwrap().push(seq);
                drop(permit);
            });
        }

        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        drop(hold);

        while set.join_next().await.is_some() {}

        assert_eq!(*order.lock().unwrap(), vec![1, 2, 3], "equal cost jobs should be FIFO");
    }
}
