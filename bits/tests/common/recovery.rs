#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::Router;
use axum::extract::{Json, Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bits::actions::{Action, ActionError, TargetAction, TargetResult};
use bits::db::{
    BrokerLeaseRecord, BrokerLeaseStore, ClaimResult, DbError, JobStore, PersistenceStore,
    PersistentJobRecord,
};
use bits::routing::{Route, switch::Switch};
use bits::server::{CODE_ACTION_CANCELLED, CODE_JOB_ERROR, CODE_JOB_NOT_FOUND};
use bits::{Bits, Job, JobResult, PollOutcome};
use bytes::Bytes;
use futures::TryStreamExt;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, OnceLock};

pub fn success_stream(
    body: impl Into<Vec<u8>>,
) -> Box<dyn futures::Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin> {
    Box::new(futures::stream::iter(vec![Ok(Bytes::from(body.into()))]))
}

#[derive(Clone)]
pub enum TargetBehavior {
    Success {
        delay: Duration,
        content_type: String,
        body: Vec<u8>,
    },
    Redirect {
        delay: Duration,
        location: String,
        message: String,
    },
    Error {
        delay: Duration,
        message: String,
    },
    Never,
}

pub struct ScriptedTarget {
    behavior: TargetBehavior,
}

impl ScriptedTarget {
    pub fn new(behavior: TargetBehavior) -> Self {
        Self { behavior }
    }
}

#[async_trait]
impl TargetAction for ScriptedTarget {
    async fn dispatch(&self, _job: &Job) -> Result<TargetResult, ActionError> {
        match &self.behavior {
            TargetBehavior::Success {
                delay,
                content_type,
                body,
            } => {
                tokio::time::sleep(*delay).await;
                Ok(TargetResult::Complete(JobResult::Success {
                    content_type: content_type.clone(),
                    size: body.len() as i64,
                    stream: success_stream(body.clone()),
                }))
            }
            TargetBehavior::Redirect {
                delay,
                location,
                message,
            } => {
                tokio::time::sleep(*delay).await;
                Ok(TargetResult::Complete(JobResult::Redirect {
                    location: location.clone(),
                    message: message.clone(),
                    content_type: None,
                    content_length: None,
                }))
            }
            TargetBehavior::Error { delay, message } => {
                tokio::time::sleep(*delay).await;
                Ok(TargetResult::Complete(JobResult::Error {
                    message: message.clone(),
                }))
            }
            TargetBehavior::Never => {
                futures::future::pending::<()>().await;
                unreachable!()
            }
        }
    }
}

pub fn single_target_switch(behavior: TargetBehavior) -> Switch {
    Switch::new(vec![Route::new(
        "default".into(),
        vec![Action::Target(
            Arc::new(ScriptedTarget::new(behavior)),
            None,
            None,
        )],
    )])
}

pub struct BrokerServer {
    pub bits: Arc<Bits>,
    pub port: u16,
    _task: JoinHandle<()>,
}

impl BrokerServer {
    pub fn job_url(&self, id: &str) -> String {
        format!("http://127.0.0.1:{}/job/{}", self.port, id)
    }

    pub fn submit_url(&self) -> String {
        format!("http://127.0.0.1:{}/job", self.port)
    }
}

pub async fn start_broker_server(
    broker_id: &str,
    router: Switch,
    persist_after: Option<Duration>,
    store: Option<Arc<dyn PersistenceStore>>,
    poll_timeout: Duration,
    broker_lease_ttl: Duration,
) -> BrokerServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}/job");
    let bits = Arc::new(Bits::from_router_for_tests(
        router,
        broker_id.to_string(),
        base_url.clone(),
        Duration::from_millis(250),
        persist_after,
        store,
        broker_lease_ttl,
    ));
    let app = bits::server::router(
        Arc::clone(&bits),
        poll_timeout,
        bits::server::DEFAULT_RETRY_AFTER_SECS,
    );
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    BrokerServer {
        bits,
        port: addr.port(),
        _task: task,
    }
}

pub async fn poll_until_terminal(bits: &Bits, job_id: &str, total_timeout: Duration) -> JobResult {
    let deadline = Instant::now() + total_timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            panic!("job '{job_id}' did not become terminal within {total_timeout:?}");
        }

        match bits
            .poll(job_id, Some(remaining.min(Duration::from_millis(250))))
            .await
        {
            PollOutcome::Ready(result) => return result,
            PollOutcome::Pending { .. } => tokio::time::sleep(Duration::from_millis(25)).await,
            other => panic!("expected pending/ready while waiting for {job_id}, got {other:?}"),
        }
    }
}

struct TiupGuard {
    child: Child,
    pd_endpoint: String,
}

impl Drop for TiupGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn tiup_guard_slot() -> &'static Mutex<Option<TiupGuard>> {
    static SLOT: OnceLock<Mutex<Option<TiupGuard>>> = OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(None))
}

fn wait_for_pd_ready(pd_endpoint: &str, timeout: Duration) {
    let mut parts = pd_endpoint.split(':');
    let host = parts.next().unwrap_or("127.0.0.1");
    let port = parts
        .next()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(2379);
    let socket = format!("{host}:{port}");
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if pd_has_up_store(&socket, host) {
            return;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    panic!("timed out waiting for TiUP playground PD endpoint {pd_endpoint}");
}

fn pd_has_up_store(socket: &str, host: &str) -> bool {
    let Ok(addr) = socket.parse() else {
        return false;
    };
    let Ok(mut stream) = std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(1)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(1)));

    let request =
        format!("GET /pd/api/v1/stores HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }

    let mut response = String::new();
    if stream.read_to_string(&mut response).is_err() {
        return false;
    }

    response.starts_with("HTTP/1.1 200") && response.contains("\"state_name\": \"Up\"")
}

pub fn ensure_tiup_playground() -> String {
    let existing = {
        let slot = tiup_guard_slot()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        slot.as_ref().map(|guard| guard.pd_endpoint.clone())
    };
    if let Some(endpoint) = existing {
        return endpoint;
    }

    let mut slot = tiup_guard_slot()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    if let Some(guard) = slot.as_ref() {
        return guard.pd_endpoint.clone();
    }

    let tiup = std::env::var("BITS_TIUP_BIN").unwrap_or_else(|_| {
        let home = std::env::var("HOME").expect("HOME must be set to locate tiup");
        format!("{home}/.tiup/bin/tiup")
    });
    let pd_endpoint =
        std::env::var("BITS_TIKV_ENDPOINTS").unwrap_or_else(|_| "127.0.0.1:2379".to_string());
    let child = Command::new(&tiup)
        .args([
            "playground",
            "nightly",
            "--mode",
            "tikv-slim",
            "--without-monitor",
            "--tag",
            "bits-tests",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap_or_else(|err| panic!("failed to start tiup playground via {tiup}: {err}"));

    wait_for_pd_ready(&pd_endpoint, Duration::from_secs(90));
    *slot = Some(TiupGuard {
        child,
        pd_endpoint: pd_endpoint.clone(),
    });
    pd_endpoint
}

#[cfg(feature = "nats")]
struct NatsGuard {
    child: Child,
    url: String,
    data_dir: std::path::PathBuf,
}

#[cfg(feature = "nats")]
impl Drop for NatsGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

#[cfg(feature = "nats")]
fn nats_guard_slot() -> &'static Mutex<Option<NatsGuard>> {
    static SLOT: OnceLock<Mutex<Option<NatsGuard>>> = OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(None))
}

#[cfg(feature = "nats")]
fn wait_for_nats_ready(host: &str, port: u16, timeout: Duration) {
    use std::net::ToSocketAddrs;

    let addr_str = format!("{host}:{port}");
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if let Ok(mut addrs) = addr_str.to_socket_addrs()
            && let Some(addr) = addrs.next()
            && std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(1)).is_ok()
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("timed out waiting for nats-server at {addr_str}");
}

#[cfg(feature = "nats")]
pub fn ensure_nats_server() -> String {
    if let Ok(url) = std::env::var("BITS_NATS_URL")
        && !url.is_empty()
    {
        return url;
    }

    let existing = {
        let slot = nats_guard_slot()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        slot.as_ref().map(|guard| guard.url.clone())
    };
    if let Some(url) = existing {
        return url;
    }

    let mut slot = nats_guard_slot()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    if let Some(guard) = slot.as_ref() {
        return guard.url.clone();
    }

    let nats_bin = std::env::var("BITS_NATS_BIN").unwrap_or_else(|_| "nats-server".to_string());
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        listener.local_addr().unwrap().port()
    };
    let url = format!("nats://127.0.0.1:{port}");
    let tmp = std::env::temp_dir().join(format!("bits-nats-test-{port}"));
    let _ = std::fs::remove_dir_all(&tmp);
    let _ = std::fs::create_dir_all(&tmp);

    let child = Command::new(&nats_bin)
        .args(["-js", "-p", &port.to_string(), "-sd", tmp.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap_or_else(|err| panic!("failed to start nats-server via {nats_bin}: {err}"));

    wait_for_nats_ready("127.0.0.1", port, Duration::from_secs(10));
    *slot = Some(NatsGuard {
        child,
        url: url.clone(),
        data_dir: tmp,
    });
    url
}

pub fn broker_identity(site: &str, env: &str, slot: u16) -> &'static str {
    Box::leak(format!("{site}-{env}-{slot}").into_boxed_str())
}

pub fn new_recovery_job_id(site: &str, env: &str, slot: u16) -> String {
    bits::request_id::encode(site, env, slot, chrono::Utc::now()).unwrap()
}

pub fn test_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

pub async fn read_success_body(result: JobResult) -> (String, Vec<u8>) {
    match result {
        JobResult::Success {
            content_type,
            stream,
            ..
        } => {
            let bytes = stream
                .try_fold(Vec::new(), |mut acc, chunk| async move {
                    acc.extend_from_slice(&chunk);
                    Ok(acc)
                })
                .await
                .unwrap();
            (content_type, bytes)
        }
        other => panic!("expected success result, got {other:?}"),
    }
}

pub async fn wait_for_ready(bits: &Bits, job_id: &str, timeout: Duration) -> JobResult {
    match bits.poll(job_id, Some(timeout)).await {
        PollOutcome::Ready(result) => result,
        other => panic!("expected ready result for {job_id}, got {other:?}"),
    }
}

pub async fn observed_owner(store: &Arc<dyn PersistenceStore>, job_id: &str) -> Option<String> {
    match store
        .claim_if_owner(job_id, "__never_expected__", "__inspector__")
        .await
        .unwrap()
    {
        ClaimResult::Active { owner_broker_id } => Some(owner_broker_id),
        ClaimResult::NotFound => None,
        ClaimResult::Claimed(_) => panic!("inspection should never claim job {job_id}"),
    }
}

pub async fn wait_for_owner(
    store: &Arc<dyn PersistenceStore>,
    job_id: &str,
    owner: &str,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if observed_owner(store, job_id).await.as_deref() == Some(owner) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("owner '{owner}' not observed for job '{job_id}' within {timeout:?}");
}

pub async fn wait_for_no_owner(store: &Arc<dyn PersistenceStore>, job_id: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if observed_owner(store, job_id).await.is_none() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("job '{job_id}' still had an owner within {timeout:?}");
}

pub async fn wait_for_job_presence(
    store: &Arc<dyn PersistenceStore>,
    job_id: &str,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        match bits::db::durable_job_present(store.as_ref(), job_id)
            .await
            .unwrap()
        {
            true => return,
            false => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    panic!("job '{job_id}' was never observed in durable storage within {timeout:?}");
}

pub async fn insert_job_record(
    store: &Arc<dyn PersistenceStore>,
    job_id: &str,
    broker_id: &str,
    request: Value,
) {
    store
        .upsert_job(PersistentJobRecord {
            job_id: job_id.to_string(),
            broker_id: broker_id.to_string(),
            original_request: request,
            user: json!({}),
            metadata: json!({}),
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();
}

pub struct LeaseWriteGateStore {
    inner: Arc<dyn PersistenceStore>,
    lease_writes_enabled: Arc<AtomicBool>,
}

impl LeaseWriteGateStore {
    pub fn new(inner: Arc<dyn PersistenceStore>) -> Self {
        Self {
            inner,
            lease_writes_enabled: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn lease_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.lease_writes_enabled)
    }
}

#[async_trait]
impl JobStore for LeaseWriteGateStore {
    async fn upsert_job(&self, record: PersistentJobRecord) -> Result<(), DbError> {
        self.inner.upsert_job(record).await
    }

    async fn delete_job(&self, job_id: &str) -> Result<(), DbError> {
        self.inner.delete_job(job_id).await
    }

    async fn claim_if_owner(
        &self,
        job_id: &str,
        expected_owner_broker_id: &str,
        claimant_broker_id: &str,
    ) -> Result<ClaimResult, DbError> {
        self.inner
            .claim_if_owner(job_id, expected_owner_broker_id, claimant_broker_id)
            .await
    }
}

#[async_trait]
impl BrokerLeaseStore for LeaseWriteGateStore {
    async fn upsert_broker_lease(
        &self,
        broker_id: &str,
        internal_poll_base_url: &str,
        ttl: Duration,
    ) -> Result<(), DbError> {
        if self.lease_writes_enabled.load(Ordering::Relaxed) {
            self.inner
                .upsert_broker_lease(broker_id, internal_poll_base_url, ttl)
                .await
        } else {
            Ok(())
        }
    }

    async fn get_broker_lease(
        &self,
        broker_id: &str,
    ) -> Result<Option<BrokerLeaseRecord>, DbError> {
        self.inner.get_broker_lease(broker_id).await
    }

    async fn delete_broker_lease(&self, broker_id: &str) -> Result<(), DbError> {
        self.inner.delete_broker_lease(broker_id).await
    }
}

enum StubResponse {
    Success { content_type: String, body: Vec<u8> },
    Redirect { location: String },
    Error { message: String },
    Gone,
    NotFound,
    Pending { job_id: String },
    ServerError,
}

#[derive(Clone)]
struct StubState {
    response: Arc<StubResponse>,
}

async fn owner_stub(Path(_id): Path<String>, State(state): State<StubState>) -> Response {
    match state.response.as_ref() {
        StubResponse::Success { content_type, body } => (
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_str(content_type).unwrap(),
            )],
            body.clone(),
        )
            .into_response(),
        StubResponse::Redirect { location } => (
            StatusCode::SEE_OTHER,
            [(header::LOCATION, location.clone())],
        )
            .into_response(),
        StubResponse::Error { message } => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "code": CODE_JOB_ERROR,
                "message": message,
                "retryable": false,
            })),
        )
            .into_response(),
        StubResponse::Gone => (
            StatusCode::GONE,
            Json(json!({
                "code": CODE_ACTION_CANCELLED,
                "message": "job was cancelled",
                "retryable": false,
            })),
        )
            .into_response(),
        StubResponse::NotFound => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "code": CODE_JOB_NOT_FOUND,
                "message": "job not found",
                "retryable": false,
            })),
        )
            .into_response(),
        StubResponse::Pending { job_id } => (
            StatusCode::SEE_OTHER,
            [(header::LOCATION, format!("/job/{job_id}"))],
        )
            .into_response(),
        StubResponse::ServerError => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn start_owner_stub(response: StubResponse) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/job/{id}", get(owner_stub))
        .with_state(StubState {
            response: Arc::new(response),
        });
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}/job")
}

pub async fn start_success_owner_stub(content_type: &str, body: impl Into<Vec<u8>>) -> String {
    start_owner_stub(StubResponse::Success {
        content_type: content_type.to_string(),
        body: body.into(),
    })
    .await
}

pub async fn start_redirect_owner_stub(location: &str) -> String {
    start_owner_stub(StubResponse::Redirect {
        location: location.to_string(),
    })
    .await
}

pub async fn start_error_owner_stub(message: &str) -> String {
    start_owner_stub(StubResponse::Error {
        message: message.to_string(),
    })
    .await
}

pub async fn start_gone_owner_stub() -> String {
    start_owner_stub(StubResponse::Gone).await
}

pub async fn start_not_found_owner_stub() -> String {
    start_owner_stub(StubResponse::NotFound).await
}

pub async fn start_pending_owner_stub(job_id: &str) -> String {
    start_owner_stub(StubResponse::Pending {
        job_id: job_id.to_string(),
    })
    .await
}

pub async fn start_server_error_owner_stub() -> String {
    start_owner_stub(StubResponse::ServerError).await
}

pub struct BackendFailingStore {
    attempts: std::sync::atomic::AtomicUsize,
}

impl BackendFailingStore {
    pub fn new() -> Self {
        Self {
            attempts: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub fn attempts(&self) -> usize {
        self.attempts.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl JobStore for BackendFailingStore {
    async fn upsert_job(&self, _record: PersistentJobRecord) -> Result<(), DbError> {
        Ok(())
    }

    async fn delete_job(&self, _job_id: &str) -> Result<(), DbError> {
        Ok(())
    }

    async fn claim_if_owner(
        &self,
        _job_id: &str,
        _expected_owner_broker_id: &str,
        _claimant_broker_id: &str,
    ) -> Result<ClaimResult, DbError> {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        Err(DbError::Backend("simulated backend outage".into()))
    }
}

#[async_trait]
impl BrokerLeaseStore for BackendFailingStore {
    async fn upsert_broker_lease(
        &self,
        _broker_id: &str,
        _internal_poll_base_url: &str,
        _ttl: Duration,
    ) -> Result<(), DbError> {
        Ok(())
    }

    async fn get_broker_lease(
        &self,
        _broker_id: &str,
    ) -> Result<Option<BrokerLeaseRecord>, DbError> {
        Ok(None)
    }

    async fn delete_broker_lease(&self, _broker_id: &str) -> Result<(), DbError> {
        Ok(())
    }
}

pub struct LeaseLookupFailingStore {
    inner: Arc<dyn PersistenceStore>,
}

impl LeaseLookupFailingStore {
    pub fn new(inner: Arc<dyn PersistenceStore>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl JobStore for LeaseLookupFailingStore {
    async fn upsert_job(&self, record: PersistentJobRecord) -> Result<(), DbError> {
        self.inner.upsert_job(record).await
    }

    async fn delete_job(&self, job_id: &str) -> Result<(), DbError> {
        self.inner.delete_job(job_id).await
    }

    async fn claim_if_owner(
        &self,
        job_id: &str,
        expected_owner_broker_id: &str,
        claimant_broker_id: &str,
    ) -> Result<ClaimResult, DbError> {
        self.inner
            .claim_if_owner(job_id, expected_owner_broker_id, claimant_broker_id)
            .await
    }
}

#[async_trait]
impl BrokerLeaseStore for LeaseLookupFailingStore {
    async fn upsert_broker_lease(
        &self,
        broker_id: &str,
        internal_poll_base_url: &str,
        ttl: Duration,
    ) -> Result<(), DbError> {
        self.inner
            .upsert_broker_lease(broker_id, internal_poll_base_url, ttl)
            .await
    }

    async fn get_broker_lease(
        &self,
        _broker_id: &str,
    ) -> Result<Option<BrokerLeaseRecord>, DbError> {
        Err(DbError::Backend("simulated lease lookup failure".into()))
    }

    async fn delete_broker_lease(&self, broker_id: &str) -> Result<(), DbError> {
        self.inner.delete_broker_lease(broker_id).await
    }
}
