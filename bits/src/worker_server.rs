//! Shared HTTP server for all remote worker pools.
//!
//! Multiple [`RemotePoolExecutor`] instances register their per-pool routers
//! here via [`WorkerServer::register_pool`]. Once all pools have registered,
//! [`WorkerServer::start`] assembles a combined router using Axum's
//! `Router::nest`, binds a single TCP listener, and spawns the server task.
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

use std::sync::{Mutex, OnceLock};

use axum::Router;
use axum::http::{StatusCode, Uri};

/// Validates that a pool name is URL-safe.
///
/// Valid: alphanumeric characters, underscores, and hyphens.
/// Returns an error if the name contains any other character.
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
/// Call [`register_pool`] once per remote pool to register its sub-router,
/// then call [`start`] to bind the listener and begin serving.
///
/// [`register_pool`]: WorkerServer::register_pool
/// [`start`]: WorkerServer::start
pub struct WorkerServer {
    host: String,
    port: u16,
    /// Accumulated per-pool (name, router) pairs, guarded by a mutex so that
    /// `RemotePoolExecutor::start_scheduler` (called from multiple sites during
    /// config parsing) can register in any order.
    pools: Mutex<Vec<(String, Router)>>,
    /// Ensures `start` is only called once.
    started: OnceLock<()>,
}

impl WorkerServer {
    /// Creates a new `WorkerServer` that will bind to `host:port` when started.
    pub fn new(host: &str, port: u16) -> Self {
        Self {
            host: host.to_string(),
            port,
            pools: Mutex::new(Vec::new()),
            started: OnceLock::new(),
        }
    }

    /// Registers a pool sub-router under `/{pool_name}/`.
    ///
    /// Must be called before [`start`]. Returns an error if the pool name
    /// contains invalid characters (only `[a-zA-Z0-9_-]` are allowed).
    ///
    /// [`start`]: Self::start
    pub fn register_pool(
        &self,
        pool_name: &str,
        router: Router,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        validate_pool_name(pool_name)?;
        self.pools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push((pool_name.to_string(), router));
        Ok(())
    }

    /// Starts the shared HTTP server.
    ///
    /// If no pools have been registered, this is a no-op — no listener is
    /// bound and no task is spawned. Can be called again later once pools
    /// have been registered.
    ///
    /// Returns an error if the TCP listener cannot bind (e.g. port in use).
    /// Once the server is actually listening, subsequent calls are no-ops.
    pub fn start(&self) -> Result<(), String> {
        if self.started.get().is_some() {
            return Ok(());
        }
        let pools = self.pools.lock().unwrap_or_else(|p| p.into_inner());
        if pools.is_empty() {
            return Ok(());
        }

        let pool_names: Vec<String> = pools.iter().map(|(name, _)| name.clone()).collect();

        let mut app = Router::new();
        for (pool_name, pool_router) in pools.iter() {
            app = app.nest(&format!("/{pool_name}"), pool_router.clone());
        }

        let fallback_pools = pool_names.clone();
        app = app.fallback(move |uri: Uri| {
            let pools = fallback_pools.clone();
            async move {
                let requested = uri.path().split('/').nth(1).unwrap_or(uri.path());
                let available = pools.join(", ");
                (
                    StatusCode::NOT_FOUND,
                    format!(
                        "pool '{}' not found; available pools: [{}]",
                        requested, available
                    ),
                )
            }
        });

        let bind_addr = format!("{}:{}", self.host, self.port);
        let std_listener = std::net::TcpListener::bind(&bind_addr)
            .map_err(|e| format!("worker server failed to bind to {bind_addr}: {e}"))?;
        std_listener
            .set_nonblocking(true)
            .map_err(|e| format!("worker server: failed to set non-blocking on listener: {e}"))?;
        let listener = tokio::net::TcpListener::from_std(std_listener)
            .map_err(|e| format!("worker server: failed to convert listener to async: {e}"))?;
        let local_addr = listener.local_addr();

        let _ = self.started.set(());

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
        // listener drops here, releasing the port
    }

    #[test]
    fn new_creates_without_panic() {
        let _ws = WorkerServer::new("127.0.0.1", 0);
    }

    #[test]
    fn register_pool_valid_names_succeed() {
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
        // `start` with zero registered pools must not bind a TCP listener.
        // We verify indirectly: bind port 0 to get a free port, then start a
        // WorkerServer on that port. If it bound a listener, a subsequent bind
        // to the same port would fail; if it didn't, the subsequent bind succeeds.
        let port = free_port().await;
        let ws = WorkerServer::new("127.0.0.1", port);
        ws.start().expect("start with no pools should succeed");
        // Give any async tasks a chance to run (should be none).
        tokio::time::sleep(Duration::from_millis(20)).await;
        // The port should be free because no server was started.
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
        ws.register_pool("pool", Router::new()).unwrap();
        let err = ws.start().unwrap_err();
        assert!(
            err.contains("failed to bind"),
            "expected bind error, got: {err}"
        );
    }

    #[tokio::test]
    async fn start_serves_nested_routes() {
        // Register a trivial pool router and verify that its routes are
        // reachable under the pool name prefix.
        let port = free_port().await;
        let ws = WorkerServer::new("127.0.0.1", port);

        let pool_router = Router::new().route("/ping", get(|| async { StatusCode::OK }));
        ws.register_pool("mypool", pool_router).unwrap();
        ws.start().expect("worker server should bind successfully");

        // Wait for server to be ready.
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

        // Prefixed path should work.
        let resp = client
            .get(format!("http://127.0.0.1:{port}/mypool/ping"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        // Flat path (no prefix) should return 404.
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
        ws.start().expect("worker server should bind successfully");

        let client = reqwest::Client::new();
        // Wait for ready.
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

        // Wrong prefix → 404 with helpful message.
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
}
