// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use futures::FutureExt;
use tracing::Instrument;

use crate::actions::{TargetAction, TargetResult};
use crate::db::{PersistenceStore, PersistentJobRecord};
use crate::job::Job;
use crate::metrics;
use crate::result::JobResult;
use crate::routing::switch::Switch;

struct InFlightGuard(Arc<AtomicUsize>);

impl InFlightGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::Release);
        Self(counter)
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

pub(crate) fn spawn_job(
    router: Arc<Switch>,
    job: Arc<Job>,
    store: Option<Arc<dyn PersistenceStore>>,
    persist_after: Option<std::time::Duration>,
    broker_id: String,
    already_persisted: bool,
    in_flight: Arc<AtomicUsize>,
    route_handle: Option<String>,
) {
    let span = tracing::info_span!("job", request.id = %job.id);
    tracing::info!(parent: &span, "job received");

    let _guard = InFlightGuard::new(in_flight);
    tokio::spawn(
        async move {
            let _in_flight = _guard;
            let started = Instant::now();
            let router_for_dispatch = Arc::clone(&router);
            let job_for_dispatch = (*job).clone();
            let dispatch_fut = AssertUnwindSafe(async move {
                dispatch(&router_for_dispatch, job_for_dispatch).await
            })
            .catch_unwind();
            tokio::pin!(dispatch_fut);

            if already_persisted {
                job.persisted.store(true, Ordering::Release);
            }

            let result = if let Some(delay) = persist_after {
                tokio::select! {
                    result = &mut dispatch_fut => result.unwrap_or_else(|p| handle_action_panic(&job.id, p)),
                    _ = tokio::time::sleep(delay) => {
                        if let Some(store) = &store
                            && !job.persisted.load(Ordering::Acquire)
                            && job.result.lock().unwrap_or_else(|p| p.into_inner()).is_none()
                        {
                            let record = PersistentJobRecord {
                                job_id: job.id.clone(),
                                broker_id: broker_id.clone(),
                                original_request: (*job.original_request).clone(),
                                user: (*job.user).clone(),
                                metadata: (*job.metadata).clone(),
                                created_at: job.created_at,
                            };
                            match store.upsert_job(record).await {
                                Ok(_) => job.persisted.store(true, Ordering::Release),
                                Err(err) => tracing::warn!(request.id = %job.id, error = %err, "delayed persist failed"),
                            }
                        }
                        (&mut dispatch_fut).await.unwrap_or_else(|p| handle_action_panic(&job.id, p))
                    }
                }
            } else {
                (&mut dispatch_fut).await.unwrap_or_else(|p| handle_action_panic(&job.id, p))
            };

            let ms = started.elapsed().as_millis();

            let outcome = metrics::job_result_outcome(&result);
            let duration_secs = (chrono::Utc::now() - job.created_at)
                .num_milliseconds()
                .max(0) as f64
                / 1000.0;
            metrics::record_job_finished(outcome);
            metrics::record_job_duration(outcome, duration_secs);
            if let Some(ref rh) = route_handle {
                metrics::record_route_handle_job_finished(rh, outcome);
                metrics::record_route_handle_job_duration(rh, outcome, duration_secs);
            }

            match &result {
                JobResult::Success { .. } => tracing::info!(duration_ms = ms, "job completed"),
                JobResult::Redirect { .. } => tracing::info!(duration_ms = ms, "job redirected"),
                JobResult::Error { message } => tracing::warn!(duration_ms = ms, error = %message, "job error"),
                JobResult::Failed { reason } => tracing::error!(duration_ms = ms, reason = %reason, "job failed"),
                JobResult::Overloaded { reason } => tracing::warn!(duration_ms = ms, reason = %reason, "job rejected: overloaded"),
                JobResult::RateLimited { reason } => tracing::warn!(duration_ms = ms, reason = %reason, "job rejected: rate limited"),
                JobResult::Cancelled => tracing::info!(duration_ms = ms, "job cancelled"),
                JobResult::ClientGone => tracing::info!(duration_ms = ms, "job abandoned: client gone"),
            }

            *job.result.lock().unwrap_or_else(|p| p.into_inner()) = Some(result);
            job.notify.notify_waiters();
        }
        .instrument(span),
    );
}

fn handle_action_panic(job_id: &str, panic_payload: Box<dyn std::any::Any + Send>) -> JobResult {
    let detail = if let Some(s) = panic_payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = panic_payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    };
    tracing::error!(request.id = %job_id, reason = %detail, "action panicked");
    JobResult::Failed {
        reason: "internal server error".to_string(),
    }
}

async fn dispatch(router: &Switch, job: Job) -> JobResult {
    match router.dispatch(&job).await {
        Ok(TargetResult::Complete(result)) => result,
        Ok(TargetResult::Reject { reason, .. }) => JobResult::Error { message: reason },
        Err(crate::actions::ActionError::Cancelled) => JobResult::Cancelled,
        Err(crate::actions::ActionError::ClientGone) => JobResult::ClientGone,
        Err(crate::actions::ActionError::QueueFull(reason)) => {
            tracing::warn!(error = %reason, "dispatch rejected: queue full");
            JobResult::Overloaded { reason }
        }
        Err(crate::actions::ActionError::UserLimitExceeded(reason)) => {
            tracing::warn!(error = %reason, "dispatch rejected: user limit exceeded");
            JobResult::RateLimited { reason }
        }
        Err(err) => {
            tracing::error!(error = %err, "dispatch failed");
            JobResult::Failed {
                reason: "internal server error".to_string(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::dispatch;
    use crate::actions::{Action, ActionError, TargetAction, TargetResult};
    use crate::job::Job;
    use crate::result::JobResult;
    use crate::routing::Route;
    use crate::routing::switch::Switch;

    struct AlwaysUserLimitExceeded;

    #[async_trait]
    impl TargetAction for AlwaysUserLimitExceeded {
        async fn dispatch(&self, _job: &Job) -> Result<TargetResult, ActionError> {
            Err(ActionError::UserLimitExceeded(
                "user is at the per-user limit (6) for this route".to_string(),
            ))
        }
    }

    /// A user-limit rejection is a "try again later" signal, not a system
    /// failure: it must surface as `JobResult::RateLimited` (retryable, 429 +
    /// Retry-After at the HTTP layer) rather than falling through to the
    /// generic `JobResult::Failed` ("internal server error", non-retryable),
    /// and distinct from `JobResult::Overloaded` (system-wide 529 backpressure).
    #[tokio::test]
    async fn user_limit_exceeded_maps_to_rate_limited_not_failed() {
        let switch = Switch::new(vec![Route::new(
            "default".to_string(),
            vec![Action::Target(
                Arc::new(AlwaysUserLimitExceeded),
                None,
                None,
            )],
        )]);

        let job = Job::new(serde_json::json!({}));
        job.set_reconnect_deadline_for_test(
            std::time::Instant::now() + std::time::Duration::from_secs(5),
        );

        let result = dispatch(&switch, job).await;

        match result {
            JobResult::RateLimited { reason } => {
                assert!(reason.contains("per-user limit"));
            }
            other => panic!("expected JobResult::RateLimited, got {other:?}"),
        }
    }
}
