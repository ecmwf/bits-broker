use std::sync::Arc;
use std::time::Instant;

use tracing::Instrument;

use crate::actions::{TargetAction, TargetResult};
use crate::db::{PersistenceStore, PersistentJobRecord};
use crate::job::Job;
use crate::result::JobResult;
use crate::routing::switch::Switch;

pub(crate) fn spawn_job(
    router: Arc<Switch>,
    job: Arc<Job>,
    store: Option<Arc<dyn PersistenceStore>>,
    persist_after: Option<std::time::Duration>,
    broker_id: String,
    already_persisted: bool,
) {
    let span = tracing::info_span!("job", job.id = %job.id);
    tracing::info!(parent: &span, "job received");

    tokio::spawn(
        async move {
            let started = Instant::now();
            let router_for_dispatch = Arc::clone(&router);
            let job_for_dispatch = (*job).clone();
            let dispatch_fut = async move { dispatch(&router_for_dispatch, job_for_dispatch).await };
            tokio::pin!(dispatch_fut);

            let mut persisted = already_persisted;

            let result = if let Some(delay) = persist_after {
                tokio::select! {
                    result = &mut dispatch_fut => result,
                    _ = tokio::time::sleep(delay) => {
                        if let Some(store) = &store
                            && job.result.lock().unwrap().is_none()
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
                                Ok(_) => persisted = true,
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

            *job.result.lock().unwrap() = Some(result);
            job.notify.notify_waiters();

            if persisted
                && let Some(store) = &store
            {
                let _ = store.delete_job(&job.id).await;
            }
        }
        .instrument(span),
    );
}

async fn dispatch(router: &Switch, job: Job) -> JobResult {
    match router.dispatch(&job).await {
        Ok(TargetResult::Complete(result)) => result,
        Ok(TargetResult::Reject { reason }) => JobResult::Error {
            message: format!("All pipelines rejected: {}", reason),
        },
        Err(crate::actions::ActionError::Cancelled) => JobResult::Cancelled,
        Err(crate::actions::ActionError::ClientGone) => JobResult::ClientGone,
        Err(err) => JobResult::Failed {
            reason: format!("Dispatch failed: {}", err),
        },
    }
}
