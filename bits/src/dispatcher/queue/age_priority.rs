use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::oneshot;

use super::Queue;
use crate::job::Job;

pub struct AgePriorityQueue {
    tx: mpsc::Sender<WorkerCmd>,
    seq: Arc<AtomicU64>,
}

impl std::fmt::Debug for AgePriorityQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgePriorityQueue").finish_non_exhaustive()
    }
}

enum WorkerCmd {
    Enqueue(Entry),
    Dequeue(oneshot::Sender<Job>),
}

struct Entry {
    cost: u64,
    seq: u64,
    enqueued_at: Instant,
    job: Job,
}

const REBALANCE_INTERVAL: Duration = Duration::from_millis(5);

impl AgePriorityQueue {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("bits-age-priority-queue".into())
            .spawn(move || worker(rx))
            .expect("failed to spawn age-priority queue thread");
        Self {
            tx,
            seq: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Default for AgePriorityQueue {
    fn default() -> Self {
        Self::new()
    }
}

fn compare_entries(left: &Entry, right: &Entry, now: Instant) -> Ordering {
    let left_cost = integer_sqrt(left.cost.max(1) as u128);
    let right_cost = integer_sqrt(right.cost.max(1) as u128);
    let left_wait = now.duration_since(left.enqueued_at).as_nanos();
    let right_wait = now.duration_since(right.enqueued_at).as_nanos();

    let left_score = left_wait.saturating_mul(right_cost);
    let right_score = right_wait.saturating_mul(left_cost);

    match left_score.cmp(&right_score) {
        Ordering::Equal => right.seq.cmp(&left.seq),
        other => other,
    }
}

fn integer_sqrt(value: u128) -> u128 {
    let mut x = value;
    let mut y = x.div_ceil(2);

    while y < x {
        x = y;
        y = (x + value / x) / 2;
    }

    x.max(1)
}

fn rebalance(entries: &mut Vec<Entry>) {
    if entries.len() < 2 {
        return;
    }

    let now = Instant::now();
    entries.sort_by(|left, right| compare_entries(left, right, now));
}

fn service_waiters(entries: &mut Vec<Entry>, waiters: &mut VecDeque<oneshot::Sender<Job>>) {
    while !waiters.is_empty() && !entries.is_empty() {
        let reply = waiters.pop_front().unwrap();
        if reply.is_closed() {
            // Caller cancelled — skip this waiter, don't pop an entry.
            continue;
        }
        let entry = entries.pop().unwrap();
        if let Err(job) = reply.send(entry.job) {
            // Race: caller cancelled between the check and the send.
            // Re-insert the entry so the job isn't lost.
            entries.push(Entry {
                cost: entry.cost,
                seq: entry.seq,
                enqueued_at: entry.enqueued_at,
                job,
            });
        }
    }
}

fn handle_cmd(cmd: WorkerCmd, entries: &mut Vec<Entry>, waiters: &mut VecDeque<oneshot::Sender<Job>>) {
    match cmd {
        WorkerCmd::Enqueue(entry) => entries.push(entry),
        WorkerCmd::Dequeue(reply) => waiters.push_back(reply),
    }
}

fn worker(rx: mpsc::Receiver<WorkerCmd>) {
    let mut entries: Vec<Entry> = Vec::new();
    let mut waiters: VecDeque<oneshot::Sender<Job>> = VecDeque::new();
    let mut needs_rebalance = false;

    loop {
        match rx.recv_timeout(REBALANCE_INTERVAL) {
            Ok(cmd) => {
                let rebalance_requested = matches!(cmd, WorkerCmd::Enqueue(_));
                handle_cmd(cmd, &mut entries, &mut waiters);
                while let Ok(cmd) = rx.try_recv() {
                    needs_rebalance |= matches!(cmd, WorkerCmd::Enqueue(_));
                    handle_cmd(cmd, &mut entries, &mut waiters);
                }
                needs_rebalance |= rebalance_requested;
            }
            Err(RecvTimeoutError::Timeout) => {
                needs_rebalance = entries.len() > 1;
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }

        if needs_rebalance {
            rebalance(&mut entries);
            needs_rebalance = false;
        }
        service_waiters(&mut entries, &mut waiters);
    }
}

#[async_trait]
impl Queue for AgePriorityQueue {
    fn enqueue(&self, job: Job) {
        let cost = job.metadata["cost"].as_u64().unwrap_or(1);
        let seq = self.seq.fetch_add(1, AtomicOrdering::Relaxed);
        let _ = self.tx.send(WorkerCmd::Enqueue(Entry {
            cost,
            seq,
            enqueued_at: Instant::now(),
            job,
        }));
    }

    async fn dequeue(&self) -> Option<Job> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(WorkerCmd::Dequeue(reply_tx)).ok()?;
        reply_rx.await.ok()
    }
}
