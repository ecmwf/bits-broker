// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::db::{DbError, PersistenceStore};
use crate::job::{self, Job};

/// Shared shutdown signal that can wake blocked threads immediately.
pub(crate) struct ShutdownSignal {
    stopped: Mutex<bool>,
    condvar: Condvar,
}

impl ShutdownSignal {
    pub(crate) fn new() -> Self {
        Self {
            stopped: Mutex::new(false),
            condvar: Condvar::new(),
        }
    }

    pub(crate) fn stop(&self) {
        let mut guard = self.stopped.lock().unwrap_or_else(|p| p.into_inner());
        *guard = true;
        self.condvar.notify_all();
    }

    pub(crate) fn is_stopped(&self) -> bool {
        *self.stopped.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub(crate) fn wait_timeout(&self, duration: Duration) {
        let guard = self.stopped.lock().unwrap_or_else(|p| p.into_inner());
        if *guard {
            return;
        }
        let _ = self.condvar.wait_timeout(guard, duration);
    }
}

pub(crate) struct ConnectedGuard {
    pollers: Arc<AtomicUsize>,
    deadline_nanos: Arc<AtomicU64>,
    notify: Arc<tokio::sync::Notify>,
    reconnect_buffer: Duration,
}

impl ConnectedGuard {
    pub(crate) fn new(
        pollers: Arc<AtomicUsize>,
        deadline_nanos: Arc<AtomicU64>,
        notify: Arc<tokio::sync::Notify>,
        reconnect_buffer: Duration,
    ) -> Self {
        pollers.fetch_add(1, Ordering::Release);
        notify.notify_waiters();
        Self {
            pollers,
            deadline_nanos,
            notify,
            reconnect_buffer,
        }
    }
}

impl Drop for ConnectedGuard {
    fn drop(&mut self) {
        let deadline = Instant::now() + self.reconnect_buffer;
        self.deadline_nanos
            .store(job::instant_to_nanos(deadline), Ordering::Release);
        self.pollers.fetch_sub(1, Ordering::Release);
        self.notify.notify_waiters();
    }
}

pub(crate) fn start_sweeper(
    jobs: Arc<DashMap<String, Arc<Job>>>,
    sweep_interval: Duration,
    job_store: Option<Arc<dyn PersistenceStore>>,
    shutdown: Arc<ShutdownSignal>,
    job_count: Arc<AtomicUsize>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let runtime = job_store.as_ref().and_then(|_| {
            match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => Some(rt),
                Err(err) => {
                    tracing::error!(
                        error = %err,
                        "sweeper: failed to build tokio runtime, \
                         durable cleanup will be skipped"
                    );
                    None
                }
            }
        });

        loop {
            shutdown.wait_timeout(sweep_interval);

            if shutdown.is_stopped() {
                return;
            }

            let mut expired = Vec::new();

            for entry in jobs.iter() {
                let job = entry.value();
                let has_result = job
                    .result
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .is_some();

                if has_result && !job.client_present() {
                    expired.push(entry.key().clone());
                }
            }

            for id in expired {
                let removed = jobs.remove_if(&id, |_, job| {
                    job.result
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .is_some()
                        && !job.client_present()
                });
                if removed.is_some() {
                    let _ = job_count
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1));
                }
                if let Some((_, job)) = removed
                    && job.persisted.load(Ordering::Acquire)
                    && let (Some(store), Some(rt)) = (&job_store, &runtime)
                    && let Err(err) = rt.block_on(store.delete_job(&id))
                {
                    tracing::warn!(request.id = %id, error = %err, "sweeper durable cleanup failed");
                }
            }

            // Reclaim per-user admission slots left behind by dead brokers (a
            // broker that crashed without releasing its guard). Owner liveness
            // is derived from broker leases inside the store.
            //
            // Bounded: under extreme leaked-entry bloat a full-bucket scan
            // can degrade by orders of magnitude (a NATS-backed store's
            // `keys()`-style scan slows sharply once the bucket far exceeds
            // the client's subscription buffer). A single slow cycle must not
            // block this sweeper thread (and the durable job cleanup above,
            // which shares this loop): give up and retry on the next sweep.
            if let (Some(store), Some(rt)) = (&job_store, &runtime) {
                match rt.block_on(reclaim_within(store.reclaim_user_slots(), RECLAIM_TIMEOUT)) {
                    ReclaimOutcome::Reclaimed(n) if n > 0 => {
                        tracing::debug!(reclaimed = n, "sweeper reclaimed user-limit slots")
                    }
                    ReclaimOutcome::Reclaimed(_) => {}
                    ReclaimOutcome::Failed(err) => {
                        tracing::debug!(error = %err, "sweeper user-limit reclaim failed")
                    }
                    ReclaimOutcome::TimedOut => {
                        tracing::warn!(
                            timeout = ?RECLAIM_TIMEOUT,
                            "sweeper user-limit reclaim timed out; will retry next sweep"
                        )
                    }
                }
            }
        }
    })
}

/// Bound on a single `reclaim_user_slots` sweep cycle (see the call site in
/// [`start_sweeper`] for why this exists). Comfortably below the minimum
/// realistic `sweep_interval` so a timed-out cycle never overlaps the next
/// one.
const RECLAIM_TIMEOUT: Duration = Duration::from_secs(15);

/// Outcome of a single bounded reclaim attempt.
#[derive(Debug)]
pub(crate) enum ReclaimOutcome {
    Reclaimed(u64),
    Failed(DbError),
    TimedOut,
}

/// Runs `fut` (a `reclaim_user_slots` call) but gives up after `timeout`
/// rather than blocking indefinitely. Generic over the future (rather than
/// taking a `&dyn PersistenceStore` directly) so it is independently
/// testable with a synthetic future instead of a real backend.
pub(crate) async fn reclaim_within<F>(fut: F, timeout: Duration) -> ReclaimOutcome
where
    F: std::future::Future<Output = Result<u64, DbError>>,
{
    match tokio::time::timeout(timeout, fut).await {
        Ok(Ok(n)) => ReclaimOutcome::Reclaimed(n),
        Ok(Err(e)) => ReclaimOutcome::Failed(e),
        Err(_) => ReclaimOutcome::TimedOut,
    }
}

#[cfg(test)]
mod reclaim_within_tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn returns_reclaimed_count_on_success() {
        let outcome = reclaim_within(async { Ok(3) }, Duration::from_secs(1)).await;
        assert!(matches!(outcome, ReclaimOutcome::Reclaimed(3)));
    }

    #[tokio::test(start_paused = true)]
    async fn propagates_backend_errors() {
        let outcome = reclaim_within(
            async { Err(DbError::Backend("boom".to_string())) },
            Duration::from_secs(1),
        )
        .await;
        assert!(matches!(outcome, ReclaimOutcome::Failed(DbError::Backend(msg)) if msg == "boom"));
    }

    #[tokio::test(start_paused = true)]
    async fn times_out_instead_of_blocking_forever_on_a_stuck_scan() {
        let start = tokio::time::Instant::now();
        // Stands in for a NATS scan pathologically slowed by bucket bloat:
        // a future that never resolves on its own.
        let never = std::future::pending::<Result<u64, DbError>>();
        let outcome = reclaim_within(never, Duration::from_secs(1)).await;
        assert!(matches!(outcome, ReclaimOutcome::TimedOut));
        // Virtual time (paused runtime): must return at ~the timeout, not
        // hang indefinitely.
        assert_eq!(start.elapsed(), Duration::from_secs(1));
    }
}
