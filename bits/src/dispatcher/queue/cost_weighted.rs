use std::collections::{BinaryHeap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

use super::Queue;
use crate::job::Job;

/// A queue that yields the cheapest waiting job first.
///
/// Cost is read from `job.metadata["cost"]` as a u64. Jobs with no cost
/// value are treated as cost 0 (highest priority).
///
/// A background task maintains the priority heap and re-orders it as new
/// items arrive. It never pops items on its own — only `dequeue` does that.
pub struct CostWeightedQueue {
    tx: std::sync::Mutex<Option<mpsc::UnboundedSender<WorkerCmd>>>,
    seq: Arc<AtomicU64>,
    dead: AtomicBool,
}

impl std::fmt::Debug for CostWeightedQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CostWeightedQueue").finish_non_exhaustive()
    }
}

enum WorkerCmd {
    Enqueue { cost: u64, seq: u64, job: Box<Job> },
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
        Self {
            tx: std::sync::Mutex::new(Some(tx)),
            seq: Arc::new(AtomicU64::new(0)),
            dead: AtomicBool::new(false),
        }
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
                heap.push(Entry {
                    cost,
                    seq,
                    job: *job,
                });
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
            if let Err(job) = reply.send(entry.job) {
                // Caller cancelled — re-enqueue the job so it isn't lost.
                let cost = job.metadata["cost"].as_u64().unwrap_or(0);
                heap.push(Entry {
                    cost,
                    seq: entry.seq,
                    job,
                });
            }
        }
    }
}

#[async_trait]
impl Queue for CostWeightedQueue {
    fn enqueue(&self, job: Job) {
        let cost = job.metadata["cost"].as_u64().unwrap_or(0);
        let seq = self.seq.fetch_add(1, AtomicOrdering::Relaxed);
        let send_failed = {
            let guard = self.tx.lock().unwrap_or_else(|p| p.into_inner());
            match guard.as_ref() {
                Some(tx) => tx
                    .send(WorkerCmd::Enqueue {
                        cost,
                        seq,
                        job: Box::new(job),
                    })
                    .is_err(),
                None => true,
            }
        };
        if send_failed
            && self
                .dead
                .compare_exchange(
                    false,
                    true,
                    AtomicOrdering::Relaxed,
                    AtomicOrdering::Relaxed,
                )
                .is_ok()
        {
            tracing::warn!("cost_weighted queue worker has exited; new jobs will not be processed");
        }
    }

    async fn dequeue(&self) -> Option<Job> {
        if self.dead.load(AtomicOrdering::Relaxed) {
            return None;
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        let send_failed = {
            let guard = self.tx.lock().unwrap_or_else(|p| p.into_inner());
            match guard.as_ref() {
                Some(tx) => tx.send(WorkerCmd::Dequeue(reply_tx)).is_err(),
                None => true,
            }
        };
        if send_failed {
            if self
                .dead
                .compare_exchange(
                    false,
                    true,
                    AtomicOrdering::Relaxed,
                    AtomicOrdering::Relaxed,
                )
                .is_ok()
            {
                tracing::warn!("cost_weighted queue worker has exited; dequeue disabled");
            }
            return None;
        }
        match reply_rx.await {
            Ok(job) => Some(job),
            Err(_) => {
                if self
                    .dead
                    .compare_exchange(
                        false,
                        true,
                        AtomicOrdering::Relaxed,
                        AtomicOrdering::Relaxed,
                    )
                    .is_ok()
                {
                    tracing::warn!("cost_weighted queue worker has exited; dropped dequeue reply");
                }
                None
            }
        }
    }

    fn close(&self) {
        self.dead.store(true, AtomicOrdering::Relaxed);
        self.tx.lock().unwrap_or_else(|p| p.into_inner()).take();
    }
}
