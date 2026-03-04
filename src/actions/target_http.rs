use crate::actions::{ActionError, TargetAction, TargetResult};
use crate::job::Job;
use crate::result::JobResult;
use async_trait::async_trait;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

/// POST the job request as JSON to an HTTP endpoint and stream the response back.
///
/// Response code mapping:
///   2xx → Complete(Success)  — streams body with content-type and size from headers
///   4xx → Reject             — remote considers the request invalid; reason from body
///   5xx / other → ActionError::NetworkError
#[derive(Debug, Serialize, Deserialize)]
pub struct HttpTarget {
    pub url: String,
}

#[async_trait]
impl TargetAction for HttpTarget {
    async fn dispatch(&self, job: &Job) -> Result<TargetResult, ActionError> {
        let response = reqwest::Client::new()
            .post(&self.url)
            .json(&job.request)
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

            let size = response
                .content_length()
                .map(|n| n as i64)
                .unwrap_or(-1);

            let stream = Box::new(
                response
                    .bytes_stream()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
            );

            Ok(TargetResult::Complete(JobResult::Success {
                content_type,
                size,
                stream,
            }))
        } else if status.is_client_error() {
            let reason = response
                .text()
                .await
                .unwrap_or_else(|_| status.to_string());
            Ok(TargetResult::Reject { reason })
        } else {
            Err(ActionError::NetworkError(format!(
                "HTTP {} from {}",
                status, self.url
            )))
        }
    }
}

crate::register_action!(target, "http", HttpTarget);

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

        let target = HttpTarget { url: spawn_server(app).await };
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

        let target = HttpTarget { url: spawn_server(app).await };
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

        let target = HttpTarget { url: spawn_server(app).await };
        let job = Job::new(serde_json::json!({}));
        let result = target.dispatch(&job).await;

        assert!(matches!(result, Err(ActionError::NetworkError(_))));
    }
}

