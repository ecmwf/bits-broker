use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use tracing::Instrument;

use crate::actions::{TargetAction, TargetResult};
use crate::db::{PersistenceStore, PersistentJobRecord};
use crate::job::Job;
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
) {
    let span = tracing::info_span!("job", job.id = %job.id);
    tracing::info!(parent: &span, "job received");

    let _guard = InFlightGuard::new(in_flight);
    tokio::spawn(
        async move {
            let _in_flight = _guard;
            let started = Instant::now();
            let router_for_dispatch = Arc::clone(&router);
            let job_for_dispatch = (*job).clone();
            let dispatch_fut = async move { dispatch(&router_for_dispatch, job_for_dispatch).await };
            tokio::pin!(dispatch_fut);

            if already_persisted {
                job.persisted.store(true, Ordering::Release);
            }

            let result = if let Some(delay) = persist_after {
                tokio::select! {
                    result = &mut dispatch_fut => result,
                    _ = tokio::time::sleep(delay) => {
                        if let Some(store) = &store
                            && !job.persisted.load(Ordering::Acquire)
                            && job.result.lock().unwrap_or_else(|p| p.into_inner()).is_none()
                        {
                            let record = PersistentJobRecord {
                                job_id: job.id.clone(),
                                broker_id: broker_id.clone(),
                                original_request: job.original_request.clone(),
                                user: job.user.clone(),
                                metadata: job.metadata.clone(),
                                created_at: job.created_at,
                            };
                            match store.upsert_job(record).await {
                                Ok(_) => job.persisted.store(true, Ordering::Release),
                                Err(err) => tracing::warn!(job.id = %job.id, error = %err, "delayed persist failed"),
                            }
                        }
                        (&mut dispatch_fut).await
                    }
                }
            } else {
                (&mut dispatch_fut).await
            };

            let ms = started.elapsed().as_millis();
            match &result {
                JobResult::Success { .. } => tracing::info!(duration_ms = ms, "job completed"),
                JobResult::Redirect { .. } => tracing::info!(duration_ms = ms, "job redirected"),
                JobResult::Error { message } => tracing::warn!(duration_ms = ms, error = %message, "job error"),
                JobResult::Failed { reason } => tracing::error!(duration_ms = ms, reason = %reason, "job failed"),
                JobResult::Cancelled => tracing::info!(duration_ms = ms, "job cancelled"),
                JobResult::ClientGone => tracing::info!(duration_ms = ms, "job abandoned: client gone"),
            }

            *job.result.lock().unwrap_or_else(|p| p.into_inner()) = Some(result);
            job.notify.notify_waiters();
        }
        .instrument(span),
    );
}

async fn dispatch(router: &Switch, job: Job) -> JobResult {
    match router.dispatch(&job).await {
        Ok(TargetResult::Complete(result)) => result,
        Ok(TargetResult::Reject { reason, .. }) => JobResult::Error { message: reason },
        Err(crate::actions::ActionError::Cancelled) => JobResult::Cancelled,
        Err(crate::actions::ActionError::ClientGone) => JobResult::ClientGone,
        Err(err) => {
            tracing::error!(error = %err, "dispatch failed");
            JobResult::Failed {
                reason: "internal server error".to_string(),
            }
        }
    }
}
