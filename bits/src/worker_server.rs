//! Shared HTTP server for all remote worker pools.
//!
//! Multiple [`RemotePoolExecutor`] instances register their per-pool routers
//! via [`WorkerServer::register_pool`]. The server binds its TCP listener
//! lazily on the first registration and dispatches requests dynamically,
//! so pools can be added at any time — including after the server is already
//! serving traffic.
//!
//! Each pool's endpoints are nested under `/{pool_name}/`:
//! - `GET  /{pool_name}/work?timeout_ms=N`
//! - `POST /{pool_name}/heartbeat/{job_id}`
//! - `POST /{pool_name}/complete/data/{job_id}`
//! - `POST /{pool_name}/complete/redirect/{job_id}`
//! - `POST /{pool_name}/complete/reject/{job_id}`
//! - `POST /{pool_name}/complete/error/{job_id}`
//!
//! This server is separate from the client-facing server in [`crate::server`].
//! It is intended to run on a trusted internal network; no authentication is
//! provided.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use axum::Router;
use axum::http::{StatusCode, Uri};
use axum::response::{IntoResponse, Response};

fn validate_pool_name(name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if name.is_empty() {
        return Err("pool name must not be empty".into());
    }
    for ch in name.chars() {
        if !ch.is_ascii_alphanumeric() && ch != '_' && ch != '-' {
            return Err(format!(
                "pool name '{}' contains invalid character '{}'; \
                 only alphanumeric characters, underscores, and hyphens are allowed",
                name, ch
            )
            .into());
        }
    }
    Ok(())
}

/// Shared HTTP server that routes worker requests across multiple remote pools.
///
/// Pools can be registered at any time via [`register_pool`]. The TCP listener
/// is bound lazily on the first registration, and incoming requests are
/// dispatched dynamically — so pools added after the server is already running
/// become reachable immediately without a restart.
///
/// [`register_pool`]: WorkerServer::register_pool
pub struct WorkerServer {
    host: String,
    port: u16,
    pools: Arc<RwLock<HashMap<String, Router>>>,
    started: OnceLock<()>,
}

impl WorkerServer {
    pub fn new(host: &str, port: u16) -> Self {
        Self {
            host: host.to_string(),
            port,
            pools: Arc::new(RwLock::new(HashMap::new())),
            started: OnceLock::new(),
        }
    }

    pub fn register_pool(
        &self,
        pool_name: &str,
        router: Router,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        validate_pool_name(pool_name)?;
        self.pools
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(pool_name.to_string(), router);
        tracing::info!(pool = %pool_name, "registered worker pool");
        self.ensure_started()
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
        Ok(())
    }

    fn ensure_started(&self) -> Result<(), String> {
        if self.started.get().is_some() {
            return Ok(());
        }
        let _ = self.started.set(());

        let pools = self.pools.clone();
        let bind_addr = format!("{}:{}", self.host, self.port);
        let std_listener = std::net::TcpListener::bind(&bind_addr)
            .map_err(|e| format!("worker server failed to bind to {bind_addr}: {e}"))?;
        std_listener
            .set_nonblocking(true)
            .map_err(|e| format!("worker server: failed to set non-blocking on listener: {e}"))?;
        let listener = tokio::net::TcpListener::from_std(std_listener)
            .map_err(|e| format!("worker server: failed to convert listener to async: {e}"))?;
        let local_addr = listener.local_addr();

        let app = Router::new().fallback(move |req: axum::extract::Request| {
            let pools = pools.clone();
            async move { dispatch_to_pool(pools, req).await }
        });

        tokio::spawn(async move {
            match local_addr {
                Ok(addr) => tracing::info!(address = %addr, "worker server listening"),
                Err(_) => tracing::info!(address = %bind_addr, "worker server listening"),
            }
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!(error = %e, "worker server exited with error");
            }
        });
        Ok(())
    }

    pub fn start(&self) -> Result<(), String> {
        if self
            .pools
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .is_empty()
        {
            return Ok(());
        }
        self.ensure_started()
    }
}

fn extract_pool_and_rest(path: &str) -> (&str, &str) {
    let after_slash = path.strip_prefix('/').unwrap_or(path);
    match after_slash.find('/') {
        Some(pos) => (&after_slash[..pos], &after_slash[pos..]),
        None if !after_slash.is_empty() => (after_slash, "/"),
        _ => ("", "/"),
    }
}

async fn dispatch_to_pool(
    pools: Arc<RwLock<HashMap<String, Router>>>,
    req: axum::extract::Request,
) -> Response {
    use tower_service::Service;

    let path = req.uri().path().to_string();
    let (pool_name, rest_path) = extract_pool_and_rest(&path);

    if pool_name.is_empty() {
        let available = available_pools_string(&pools);
        return (
            StatusCode::NOT_FOUND,
            format!("no pool specified; available pools: [{available}]"),
        )
            .into_response();
    }

    let mut router = {
        let map = pools.read().unwrap_or_else(|p| p.into_inner());
        match map.get(pool_name) {
            Some(r) => r.clone(),
            None => {
                let available = map.keys().cloned().collect::<Vec<_>>().join(", ");
                return (
                    StatusCode::NOT_FOUND,
                    format!(
                        "pool '{}' not found; available pools: [{}]",
                        pool_name, available
                    ),
                )
                    .into_response();
            }
        }
    };

    let (mut parts, body) = req.into_parts();
    let new_pq = match parts.uri.query() {
        Some(q) => format!("{rest_path}?{q}"),
        None => rest_path.to_string(),
    };
    let mut uri_parts = parts.uri.into_parts();
    let pq = match new_pq.parse() {
        Ok(pq) => pq,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid request path").into_response(),
    };
    uri_parts.path_and_query = Some(pq);
    let Ok(uri) = Uri::from_parts(uri_parts) else {
        return (StatusCode::BAD_REQUEST, "invalid request URI").into_response();
    };
    parts.uri = uri;
    let req = axum::http::Request::from_parts(parts, body);

    match router.call(req).await {
        Ok(response) => response,
        Err(err) => match err {},
    }
}

fn available_pools_string(pools: &Arc<RwLock<HashMap<String, Router>>>) -> String {
    pools
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .keys()
        .cloned()
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum::{Router, routing::get};
    use std::sync::Arc;
    use std::time::Duration;

    async fn free_port() -> u16 {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    }

    #[test]
    fn new_creates_without_panic() {
        let _ws = WorkerServer::new("127.0.0.1", 0);
    }

    #[tokio::test]
    async fn register_pool_valid_names_succeed() {
        let ws = WorkerServer::new("127.0.0.1", 0);
        ws.register_pool("mars", Router::new()).unwrap();
        ws.register_pool("fdb_slow", Router::new()).unwrap();
        ws.register_pool("pool-1", Router::new()).unwrap();
        ws.register_pool("FDB", Router::new()).unwrap();
    }

    #[test]
    fn register_pool_rejects_slash() {
        let ws = WorkerServer::new("127.0.0.1", 0);
        let err = ws.register_pool("invalid/name", Router::new()).unwrap_err();
        assert!(
            err.to_string().contains("invalid character '/'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn register_pool_rejects_space() {
        let ws = WorkerServer::new("127.0.0.1", 0);
        let err = ws.register_pool("invalid name", Router::new()).unwrap_err();
        assert!(
            err.to_string().contains("invalid character ' '"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn register_pool_rejects_empty_name() {
        let ws = WorkerServer::new("127.0.0.1", 0);
        let err = ws.register_pool("", Router::new()).unwrap_err();
        assert!(
            err.to_string().contains("must not be empty"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn register_pool_rejects_dot() {
        let ws = WorkerServer::new("127.0.0.1", 0);
        let err = ws.register_pool("a..b", Router::new()).unwrap_err();
        assert!(
            err.to_string().contains("invalid character '.'"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn start_with_no_pools_does_not_bind() {
        let port = free_port().await;
        let ws = WorkerServer::new("127.0.0.1", port);
        ws.start().expect("start with no pools should succeed");
        tokio::time::sleep(Duration::from_millis(20)).await;
        tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
            .await
            .expect("port should be available — worker server should not have bound it");
    }

    #[tokio::test]
    async fn start_is_idempotent() {
        let ws = Arc::new(WorkerServer::new("127.0.0.1", 0));
        ws.start().expect("first start should succeed");
        ws.start().expect("second start should also succeed");
    }

    #[tokio::test]
    async fn start_returns_error_when_port_occupied() {
        let blocker = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = blocker.local_addr().unwrap().port();

        let ws = WorkerServer::new("127.0.0.1", port);
        ws.register_pool("pool", Router::new()).unwrap_err();
    }

    #[tokio::test]
    async fn start_serves_nested_routes() {
        let port = free_port().await;
        let ws = WorkerServer::new("127.0.0.1", port);

        let pool_router = Router::new().route("/ping", get(|| async { StatusCode::OK }));
        ws.register_pool("mypool", pool_router).unwrap();

        let client = reqwest::Client::new();
        for _ in 0..100 {
            if client
                .get(format!("http://127.0.0.1:{port}/mypool/ping"))
                .send()
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let resp = client
            .get(format!("http://127.0.0.1:{port}/mypool/ping"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        let resp = client
            .get(format!("http://127.0.0.1:{port}/ping"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
    }

    #[tokio::test]
    async fn two_pools_isolated_on_same_server() {
        let port = free_port().await;
        let ws = WorkerServer::new("127.0.0.1", port);

        ws.register_pool(
            "alpha",
            Router::new().route("/ping", get(|| async { "alpha" })),
        )
        .unwrap();
        ws.register_pool(
            "beta",
            Router::new().route("/ping", get(|| async { "beta" })),
        )
        .unwrap();

        let client = reqwest::Client::new();
        for _ in 0..100 {
            if client
                .get(format!("http://127.0.0.1:{port}/alpha/ping"))
                .send()
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let alpha = client
            .get(format!("http://127.0.0.1:{port}/alpha/ping"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(alpha, "alpha");

        let beta = client
            .get(format!("http://127.0.0.1:{port}/beta/ping"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(beta, "beta");

        let resp = client
            .get(format!("http://127.0.0.1:{port}/gamma/ping"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
        let body = resp.text().await.unwrap();
        assert!(
            body.contains("pool 'gamma' not found"),
            "expected pool-not-found message, got: {body}"
        );
        assert!(
            body.contains("alpha") && body.contains("beta"),
            "expected available pools in message, got: {body}"
        );
    }

    #[tokio::test]
    async fn pool_added_after_start_is_reachable() {
        let port = free_port().await;
        let ws = Arc::new(WorkerServer::new("127.0.0.1", port));

        ws.register_pool(
            "first",
            Router::new().route("/ping", get(|| async { "first" })),
        )
        .unwrap();

        let client = reqwest::Client::new();
        for _ in 0..100 {
            if client
                .get(format!("http://127.0.0.1:{port}/first/ping"))
                .send()
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        ws.register_pool(
            "second",
            Router::new().route("/ping", get(|| async { "second" })),
        )
        .unwrap();

        let resp = client
            .get(format!("http://127.0.0.1:{port}/second/ping"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.text().await.unwrap(), "second");
    }

    #[tokio::test]
    async fn pool_added_after_start_appears_in_404_message() {
        let port = free_port().await;
        let ws = Arc::new(WorkerServer::new("127.0.0.1", port));

        ws.register_pool("initial", Router::new()).unwrap();

        let client = reqwest::Client::new();
        for _ in 0..100 {
            if client
                .get(format!("http://127.0.0.1:{port}/initial/work"))
                .send()
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        ws.register_pool("late_pool", Router::new()).unwrap();

        let resp = client
            .get(format!("http://127.0.0.1:{port}/unknown/work"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
        let body = resp.text().await.unwrap();
        assert!(
            body.contains("initial") && body.contains("late_pool"),
            "404 should list all pools including late additions, got: {body}"
        );
    }

    #[tokio::test]
    async fn pool_replacement_serves_new_router() {
        let port = free_port().await;
        let ws = Arc::new(WorkerServer::new("127.0.0.1", port));

        ws.register_pool(
            "mypool",
            Router::new().route("/ping", get(|| async { "v1" })),
        )
        .unwrap();

        let client = reqwest::Client::new();
        for _ in 0..100 {
            if client
                .get(format!("http://127.0.0.1:{port}/mypool/ping"))
                .send()
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let resp = client
            .get(format!("http://127.0.0.1:{port}/mypool/ping"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.text().await.unwrap(), "v1");

        ws.register_pool(
            "mypool",
            Router::new().route("/ping", get(|| async { "v2" })),
        )
        .unwrap();

        let resp = client
            .get(format!("http://127.0.0.1:{port}/mypool/ping"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.text().await.unwrap(), "v2");
    }

    #[tokio::test]
    async fn query_string_preserved_through_dispatch() {
        let port = free_port().await;
        let ws = WorkerServer::new("127.0.0.1", port);

        ws.register_pool(
            "pool",
            Router::new().route(
                "/work",
                get(|req: axum::extract::Request| async move {
                    req.uri().query().unwrap_or("none").to_string()
                }),
            ),
        )
        .unwrap();

        let client = reqwest::Client::new();
        for _ in 0..100 {
            if client
                .get(format!("http://127.0.0.1:{port}/pool/work"))
                .send()
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let resp = client
            .get(format!("http://127.0.0.1:{port}/pool/work?timeout_ms=5000"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.text().await.unwrap(), "timeout_ms=5000");
    }

    #[tokio::test]
    async fn auto_starts_on_first_registration() {
        let port = free_port().await;
        let ws = WorkerServer::new("127.0.0.1", port);

        let client = reqwest::Client::new();
        assert!(
            client
                .get(format!("http://127.0.0.1:{port}/anything"))
                .send()
                .await
                .is_err(),
            "server should not be listening before any pool is registered"
        );

        ws.register_pool("pool", Router::new().route("/ping", get(|| async { "ok" })))
            .unwrap();

        for _ in 0..100 {
            if client
                .get(format!("http://127.0.0.1:{port}/pool/ping"))
                .send()
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let resp = client
            .get(format!("http://127.0.0.1:{port}/pool/ping"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }
}
