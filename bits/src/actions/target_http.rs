use std::sync::OnceLock;
use std::time::Duration;

use async_trait::async_trait;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

use crate::actions::{ActionError, TargetAction, TargetResult};
use crate::job::Job;
use crate::result::JobResult;

const DEFAULT_TARGET_TIMEOUT_SECS_RAW: u64 = 30;
const DEFAULT_TARGET_TIMEOUT_SECS: f64 = DEFAULT_TARGET_TIMEOUT_SECS_RAW as f64;

// ================================
//   HttpTarget
// ================================

/// POST the job request as JSON to an HTTP endpoint and stream the response back.
///
/// Scheduling (queue ordering and concurrency limits) is configured at the
/// route step level, not inside this action.
///
/// Response code mapping:
///   2xx → Complete(Success)  — streams body with content-type and size from headers
///   4xx → Reject             — remote considers the request invalid; reason from body
///   5xx / other → ActionError::NetworkError
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpTarget {
    pub url: String,
    #[serde(
        default = "default_target_timeout_secs",
        deserialize_with = "deserialize_positive_secs"
    )]
    pub connect_timeout_secs: f64,
    #[serde(
        default = "default_target_timeout_secs",
        deserialize_with = "deserialize_positive_secs"
    )]
    pub read_timeout_secs: f64,
    #[serde(skip)]
    client: OnceLock<reqwest::Client>,
}

fn deserialize_positive_secs<'de, D: serde::Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
    let v = f64::deserialize(d)?;
    if !v.is_finite() || v <= 0.0 {
        return Err(serde::de::Error::custom(
            "must be a positive finite number of seconds",
        ));
    }
    Duration::try_from_secs_f64(v).map_err(|_| {
        serde::de::Error::custom("value overflows Duration; use a smaller number of seconds")
    })?;
    Ok(v)
}

fn default_target_timeout_secs() -> f64 {
    DEFAULT_TARGET_TIMEOUT_SECS
}

impl HttpTarget {
    pub fn new(url: String) -> Self {
        Self {
            url,
            connect_timeout_secs: DEFAULT_TARGET_TIMEOUT_SECS,
            read_timeout_secs: DEFAULT_TARGET_TIMEOUT_SECS,
            client: OnceLock::new(),
        }
    }

    fn client(&self) -> &reqwest::Client {
        self.client.get_or_init(|| {
            let connect = Duration::from_secs_f64(self.connect_timeout_secs);
            let read = Duration::from_secs_f64(self.read_timeout_secs);
            reqwest::Client::builder()
                .connect_timeout(connect)
                .read_timeout(read)
                .build()
                .expect("reqwest client with timeout")
        })
    }
}

impl std::fmt::Debug for HttpTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpTarget")
            .field("url", &self.url)
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

        let stream = Box::new(response.bytes_stream().map_err(std::io::Error::other));

        Ok(TargetResult::Complete(JobResult::Success {
            content_type,
            size,
            stream,
        }))
    } else if status.is_client_error() {
        let reason = response.text().await.unwrap_or_else(|_| status.to_string());
        Ok(TargetResult::Reject {
            reason,
            silent: true,
        })
    } else {
        Err(ActionError::NetworkError(format!(
            "HTTP {} from {}",
            status, url
        )))
    }
}

// ================================
//   TargetAction impl
// ================================

#[async_trait]
impl TargetAction for HttpTarget {
    async fn dispatch(&self, job: &Job) -> Result<TargetResult, ActionError> {
        execute(self.client().clone(), self.url.clone(), job.request.clone()).await
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
        Router,
        body::Body,
        http::{Response, StatusCode, header},
        routing::post,
    };

    async fn spawn_server(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{}/", addr)
    }

    #[tokio::test]
    async fn test_2xx_maps_to_complete() {
        let app = Router::new().route(
            "/",
            post(|| async {
                Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::CONTENT_LENGTH, "12")
                    .body(Body::from(r#"{"ok": true}"#))
                    .unwrap()
            }),
        );

        let target = HttpTarget::new(spawn_server(app).await);
        let job = Job::new(serde_json::json!({"class": "od"}));
        let result = target.dispatch(&job).await.unwrap();

        match result {
            TargetResult::Complete(JobResult::Success {
                content_type, size, ..
            }) => {
                assert_eq!(content_type, "application/json");
                assert_eq!(size, 12);
            }
            other => panic!("expected Complete(Success), got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_4xx_maps_to_reject() {
        let app = Router::new().route(
            "/",
            post(|| async {
                Response::builder()
                    .status(StatusCode::UNPROCESSABLE_ENTITY)
                    .body(Body::from("unknown class"))
                    .unwrap()
            }),
        );

        let target = HttpTarget::new(spawn_server(app).await);
        let job = Job::new(serde_json::json!({"class": "??"}));
        let result = target.dispatch(&job).await.unwrap();

        match result {
            TargetResult::Reject { reason, .. } => assert_eq!(reason, "unknown class"),
            other => panic!("expected Reject, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_5xx_maps_to_network_error() {
        let app = Router::new().route(
            "/",
            post(|| async {
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::empty())
                    .unwrap()
            }),
        );

        let target = HttpTarget::new(spawn_server(app).await);
        let job = Job::new(serde_json::json!({}));
        let result = target.dispatch(&job).await;

        assert!(matches!(result, Err(ActionError::NetworkError(_))));
    }

    #[test]
    fn defaults_applied_when_timeout_fields_omitted() {
        let target: HttpTarget =
            serde_json::from_value(serde_json::json!({"url": "http://x"})).unwrap();
        assert_eq!(target.connect_timeout_secs, DEFAULT_TARGET_TIMEOUT_SECS);
        assert_eq!(target.read_timeout_secs, DEFAULT_TARGET_TIMEOUT_SECS);
    }

    #[test]
    fn custom_timeout_values_accepted() {
        let target: HttpTarget = serde_json::from_value(serde_json::json!({
            "url": "http://x",
            "connect_timeout_secs": 5.0,
            "read_timeout_secs": 120.0
        }))
        .unwrap();
        assert_eq!(target.connect_timeout_secs, 5.0);
        assert_eq!(target.read_timeout_secs, 120.0);
    }

    #[test]
    fn negative_timeout_rejected() {
        let result = serde_json::from_value::<HttpTarget>(
            serde_json::json!({"url": "http://x", "connect_timeout_secs": -1.0}),
        );
        assert!(result.is_err());
    }

    #[test]
    fn zero_timeout_rejected() {
        let result = serde_json::from_value::<HttpTarget>(
            serde_json::json!({"url": "http://x", "read_timeout_secs": 0.0}),
        );
        assert!(result.is_err());
    }

    #[test]
    fn unknown_field_rejected() {
        let result = serde_json::from_value::<HttpTarget>(
            serde_json::json!({"url": "http://x", "read_timeout_sec": 10.0}),
        );
        assert!(result.is_err());
    }
}
