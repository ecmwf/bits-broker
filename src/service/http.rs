use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use utoipa::OpenApi;

use crate::result::JobResult;
use crate::job::Job;
use crate::Bits;
use super::Service;

const POLL_TIMEOUT: Duration = Duration::from_secs(25);

// ================================
//   OpenAPI spec
// ================================

#[derive(OpenApi)]
#[openapi(
    info(
        title = "BITS",
        description = "Broker for Intelligent Task Scheduling — policy-aware job routing across distributed infrastructure.",
        version = "0.1.0",
    ),
    paths(submit_job, poll_job),
)]
struct ApiDoc;

// ================================
//   InFlightJob
// ================================

struct InFlightJob {
    result: Mutex<Option<JobResult>>,
    notify: Notify,
}

// ================================
//   AppState
// ================================

#[derive(Clone)]
struct AppState {
    bits: Arc<Bits>,
    jobs: Arc<Mutex<HashMap<String, Arc<InFlightJob>>>>,
}

// ================================
//   HttpService
// ================================

pub struct HttpService {
    bind: String,
    bits: Arc<Bits>,
}

impl HttpService {
    pub fn new(bind: impl Into<String>, bits: Arc<Bits>) -> Self {
        Self { bind: bind.into(), bits }
    }
}

#[async_trait]
impl Service for HttpService {
    async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let state = AppState {
            bits: self.bits.clone(),
            jobs: Arc::new(Mutex::new(HashMap::new())),
        };

        let spec = ApiDoc::openapi();
        let app = Router::new()
            .route("/job", post(submit_job))
            .route("/job/{id}", get(poll_job))
            .route("/openapi.json", get(move || async move { Json(spec) }))
            .with_state(state);

        let listener = TcpListener::bind(&self.bind).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }
}

// ================================
//   Handlers
// ================================

#[utoipa::path(
    post,
    path = "/job",
    tag = "jobs",
    request_body(
        content = Object,
        description = "Arbitrary JSON request payload routed through the BITS pipeline.",
        content_type = "application/json",
    ),
    responses(
        (status = 200, description = "Job completed. Body is the result stream; Content-Type reflects the data format."),
        (status = 303, description = "Job in progress. Reconnect to the URL in the Location header to continue polling.",
            headers(
                ("Location" = String, description = "URL to reconnect to: GET /job/{id}"),
                ("Retry-After" = String, description = "Suggested reconnect delay in seconds (0 = immediately)"),
            )
        ),
        (status = 303, description = "Job result is a redirect to an external data location.",
            headers(("Location" = String, description = "Data URL"))
        ),
        (status = 400, description = "Job was rejected by the pipeline (e.g. no matching route, failed check)."),
        (status = 500, description = "System-level failure (routing error, internal fault)."),
    ),
)]
async fn submit_job(State(state): State<AppState>, Json(body): Json<Value>) -> Response {
    let job = Job::new(body);
    let job_id = job.id.clone();

    let in_flight = Arc::new(InFlightJob {
        result: Mutex::new(None),
        notify: Notify::new(),
    });

    state.jobs.lock().unwrap().insert(job_id.clone(), in_flight.clone());

    let bits = state.bits.clone();
    tokio::spawn(async move {
        let result = bits.process(job).await;
        *in_flight.result.lock().unwrap() = Some(result);
        in_flight.notify.notify_waiters();
    });

    wait_for_result(&job_id, &state).await
}

#[utoipa::path(
    get,
    path = "/job/{id}",
    tag = "jobs",
    params(
        ("id" = String, Path, description = "Job ID returned in the Location header of a 303 response."),
    ),
    responses(
        (status = 200, description = "Job completed. Body is the result stream; Content-Type reflects the data format."),
        (status = 303, description = "Job still in progress. Reconnect to the Location header URL.",
            headers(
                ("Location" = String, description = "URL to reconnect to: GET /job/{id}"),
                ("Retry-After" = String, description = "Suggested reconnect delay in seconds (0 = immediately)"),
            )
        ),
        (status = 303, description = "Job result is a redirect to an external data location.",
            headers(("Location" = String, description = "Data URL"))
        ),
        (status = 400, description = "Job was rejected by the pipeline."),
        (status = 404, description = "Job not found (expired or never existed)."),
        (status = 500, description = "System-level failure."),
    ),
)]
async fn poll_job(Path(id): Path<String>, State(state): State<AppState>) -> Response {
    wait_for_result(&id, &state).await
}

// ================================
//   Long-poll logic
// ================================

async fn wait_for_result(job_id: &str, state: &AppState) -> Response {
    let in_flight = {
        state.jobs.lock().unwrap().get(job_id).cloned()
    };

    let Some(in_flight) = in_flight else {
        return StatusCode::NOT_FOUND.into_response();
    };

    // Register interest BEFORE checking result to close the race window:
    // if the worker notifies between our check and our await, enable() ensures
    // the Notified future is already marked ready and won't block.
    let notified = in_flight.notify.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();

    // Fast path: result already available
    if let Some(result) = in_flight.result.lock().unwrap().take() {
        state.jobs.lock().unwrap().remove(job_id);
        return job_result_to_response(result);
    }

    // Wait up to 25s, then redirect the client back to reconnect
    match tokio::time::timeout(POLL_TIMEOUT, notified).await {
        Ok(()) => {
            if let Some(result) = in_flight.result.lock().unwrap().take() {
                state.jobs.lock().unwrap().remove(job_id);
                job_result_to_response(result)
            } else {
                // Spurious wakeup; redirect to retry
                redirect_to_poll(job_id)
            }
        }
        Err(_timeout) => redirect_to_poll(job_id),
    }
}

fn redirect_to_poll(job_id: &str) -> Response {
    (
        StatusCode::SEE_OTHER,
        [
            (header::LOCATION, format!("/job/{}", job_id)),
            (header::RETRY_AFTER, "0".to_string()),
        ],
    )
        .into_response()
}

fn job_result_to_response(result: JobResult) -> Response {
    match result {
        JobResult::Success { content_type, stream, .. } => {
            ([(header::CONTENT_TYPE, content_type)], Body::from_stream(stream)).into_response()
        }
        JobResult::Redirect { location, .. } => {
            (StatusCode::SEE_OTHER, [(header::LOCATION, location)]).into_response()
        }
        JobResult::Error { message } => (StatusCode::BAD_REQUEST, message).into_response(),
        JobResult::Failed { reason } => {
            (StatusCode::INTERNAL_SERVER_ERROR, reason).into_response()
        }
    }
}
