    use std::sync::OnceLock;

use async_trait::async_trait;
use futures::{future::BoxFuture, TryStreamExt};
use serde::{Deserialize, Serialize};

use crate::actions::{ActionError, TargetAction, TargetResult};
use crate::dispatcher::{Dispatcher, ExecutorKind};
use crate::job::Job;
use crate::queue::QueueKind;
use crate::result::JobResult;

// ================================
//   Runtime (not serialised)
// ================================

struct HttpRuntime {
    client: reqwest::Client,
    dispatcher: Option<Dispatcher>,
}

// ================================
//   HttpTarget
// ================================

/// POST the job request as JSON to an HTTP endpoint and stream the response back.
///
/// Optional scheduling:
///   `queue`       — "fifo" | "cost_weighted"  — orders waiting jobs before dispatch
///   `concurrency` — integer                   — max simultaneous in-flight requests
///   `executor`    — "semaphore" (default) | "thread_pool"  — execution policy
///
/// Response code mapping:
///   2xx → Complete(Success)  — streams body with content-type and size from headers
///   4xx → Reject             — remote considers the request invalid; reason from body
///   5xx / other → ActionError::NetworkError
#[derive(Serialize, Deserialize)]
pub struct HttpTarget {
    pub url: String,
    #[serde(default)]
    pub concurrency: Option<usize>,
    #[serde(default)]
    pub queue: Option<QueueKind>,
    #[serde(default)]
    pub executor: Option<ExecutorKind>,
    #[serde(skip)]
    runtime: OnceLock<HttpRuntime>,
}

impl HttpTarget {
    pub fn new(url: String) -> Self {
        Self {
            url,
            concurrency: None,
            queue: None,
            executor: None,
            runtime: OnceLock::new(),
        }
    }

    fn runtime(&self) -> &HttpRuntime {
        self.runtime.get_or_init(|| {
            let dispatcher = Dispatcher::from_config(
                self.queue.as_ref(),
                self.executor.as_ref(),
                self.concurrency,
            );
            HttpRuntime { client: reqwest::Client::new(), dispatcher }
        })
    }
}

impl std::fmt::Debug for HttpTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpTarget")
            .field("url", &self.url)
            .field("concurrency", &self.concurrency)
            .field("queue", &self.queue)
            .field("executor", &self.executor)
            .finish_non_exhaustive()
    }
}

// ================================
//   HTTP call (owned args → 'static future)
// ================================

async fn execute(
    client: reqwest::Client,
    url: String,
    request: serde_json::Value,
) -> Result<TargetResult, ActionError> {
    let response = client
        .post(&url)
        .json(&request)
        .send()
        .await
        .map_err(|e| ActionError::NetworkError(e.to_string()))?;

    let status = response.status();

    if status.is_success() {
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();

        let size = response.content_length().map(|n| n as i64).unwrap_or(-1);

        let stream = Box::new(
            response
                .bytes_stream()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
        );

        Ok(TargetResult::Complete(JobResult::Success { content_type, size, stream }))
    } else if status.is_client_error() {
        let reason = response.text().await.unwrap_or_else(|_| status.to_string());
        Ok(TargetResult::Reject { reason })
    } else {
        Err(ActionError::NetworkError(format!("HTTP {} from {}", status, url)))
    }
}

// ================================
//   TargetAction impl
// ================================

#[async_trait]
impl TargetAction for HttpTarget {
    async fn dispatch(&self, job: &Job) -> Result<TargetResult, ActionError> {
        let rt = self.runtime();

        let work: BoxFuture<'static, Result<TargetResult, ActionError>> =
            Box::pin(execute(rt.client.clone(), self.url.clone(), job.request.clone()));

        match &rt.dispatcher {
            Some(d) => d.dispatch(job, work).await,
            None => work.await,
        }
    }
}

crate::register_action!(target, "http", HttpTarget);

// ================================
//   Tests
// ================================

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{header, Response, StatusCode},
        routing::post,
        Router,
    };

    async fn spawn_server(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{}/", addr)
    }

    #[tokio::test]
    async fn test_2xx_maps_to_complete() {
        let app = Router::new().route("/", post(|| async {
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::CONTENT_LENGTH, "12")
                .body(Body::from(r#"{"ok": true}"#))
                .unwrap()
        }));

        let target = HttpTarget::new(spawn_server(app).await);
        let job = Job::new(serde_json::json!({"class": "od"}));
        let result = target.dispatch(&job).await.unwrap();

        match result {
            TargetResult::Complete(JobResult::Success { content_type, size, .. }) => {
                assert_eq!(content_type, "application/json");
                assert_eq!(size, 12);
            }
            other => panic!("expected Complete(Success), got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_4xx_maps_to_reject() {
        let app = Router::new().route("/", post(|| async {
            Response::builder()
                .status(StatusCode::UNPROCESSABLE_ENTITY)
                .body(Body::from("unknown class"))
                .unwrap()
        }));

        let target = HttpTarget::new(spawn_server(app).await);
        let job = Job::new(serde_json::json!({"class": "??"}));
        let result = target.dispatch(&job).await.unwrap();

        match result {
            TargetResult::Reject { reason } => assert_eq!(reason, "unknown class"),
            other => panic!("expected Reject, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_5xx_maps_to_network_error() {
        let app = Router::new().route("/", post(|| async {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())
                .unwrap()
        }));

        let target = HttpTarget::new(spawn_server(app).await);
        let job = Job::new(serde_json::json!({}));
        let result = target.dispatch(&job).await;

        assert!(matches!(result, Err(ActionError::NetworkError(_))));
    }
}
