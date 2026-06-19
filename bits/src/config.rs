use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use serde::Deserialize;

use crate::Bits;
use crate::actions::Action;
use crate::actions::registry::create_action;
use crate::actions::{TargetAction, TargetResult};
use crate::bits::DEFAULT_MAX_JOBS;
use crate::db::PersistenceStore;
use crate::dispatcher::{
    DEFAULT_QUEUE_CAPACITY, Dispatcher, ExecutorKind, QueueKind, RemotePoolConfig,
};
use crate::error::{BitsError, ConfigError, RoutingError, WorkerServerError};
use crate::routing::{Route, switch::Switch};
use crate::server::ServerConfig;
use crate::worker_server::WorkerServer;

fn duration_secs(field: &str, secs: f64) -> Result<Duration, BitsError> {
    Duration::try_from_secs_f64(secs)
        .map_err(|e| ConfigError::validation(field, e.to_string()).into())
}

fn positive_duration_secs(field: &str, secs: f64) -> Result<Duration, BitsError> {
    let d = duration_secs(field, secs)?;
    if d.is_zero() {
        return Err(ConfigError::validation(field, "must be greater than zero").into());
    }
    Ok(d)
}

fn require_site_env_tag(
    field: &str,
    value: Option<&serde_json::Value>,
) -> Result<String, BitsError> {
    let value = value.ok_or_else(|| ConfigError::missing(field))?;
    let tag = match value {
        serde_json::Value::String(tag) => tag.clone(),
        serde_json::Value::Number(number) if number.is_u64() => number.to_string(),
        _ => {
            return Err(
                ConfigError::validation(field, "must be a string or unsigned integer tag").into(),
            );
        }
    };
    crate::request_id::pack_tag(&tag)
        .map(|_| tag)
        .map_err(|err| ConfigError::validation(field, err.to_string()).into())
}

fn allocate_startup_broker_slot(
    store: Arc<dyn PersistenceStore>,
    site: String,
    env: String,
) -> Result<u16, BitsError> {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| ConfigError::validation("bits.persistence", err.to_string()))?;
        runtime
            .block_on(store.allocate_broker_slot(&site, &env))
            .map_err(BitsError::from)
    })
    .join()
    .map_err(|_| ConfigError::validation("bits.persistence", "broker slot allocator panicked"))?
}

#[derive(Default)]
struct Registries {
    checks: HashMap<String, serde_json::Value>,
    transforms: HashMap<String, serde_json::Value>,
    targets: HashMap<String, serde_json::Value>,
}

pub struct RouteFactory {
    registries: Registries,
    resolved_targets: Mutex<HashMap<String, ResolvedTarget>>,
    worker_server: Option<Arc<WorkerServer>>,
}

type ResolvedTarget = (
    Arc<dyn TargetAction>,
    Option<Dispatcher<TargetResult>>,
    Option<bool>,
);

struct ParseContext {
    registries: Registries,
    resolved_targets: RefCell<HashMap<String, ResolvedTarget>>,
    worker_server: Option<Arc<WorkerServer>>,
}

struct DispatcherSettings {
    queue: Option<QueueKind>,
    executor: Option<ExecutorKind>,
    queue_capacity: usize,
}

impl Default for DispatcherSettings {
    fn default() -> Self {
        Self {
            queue: None,
            executor: None,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerServerConfig {
    #[serde(default = "default_worker_server_host")]
    host: String,
    #[serde(default = "default_worker_server_port")]
    port: u16,
    #[serde(default)]
    advertised_addr: Option<String>,
}

fn default_worker_server_host() -> String {
    "0.0.0.0".into()
}

fn default_worker_server_port() -> u16 {
    9001
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct BitsConfig {
    #[serde(default)]
    broker_id_prefix: Option<String>,
    #[serde(default)]
    site: Option<serde_json::Value>,
    #[serde(default)]
    env: Option<serde_json::Value>,
    #[serde(default)]
    internal_poll_endpoint: Option<String>,
    #[serde(default)]
    internal_poll_timeout_secs: Option<f64>,
    #[serde(default)]
    sweep_interval_secs: Option<f64>,
    #[serde(default)]
    reconnect_buffer_secs: Option<f64>,
    #[serde(default)]
    persist_after_secs: Option<f64>,
    #[serde(default)]
    persistence: Option<PersistenceConfig>,
    #[serde(default)]
    worker_server: Option<WorkerServerConfig>,
    #[serde(default)]
    max_jobs: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
enum PersistenceConfig {
    #[cfg_attr(not(feature = "tikv"), allow(dead_code))]
    Tikv {
        endpoints: Vec<String>,
        #[serde(default = "default_broker_lease_ttl_secs")]
        broker_lease_ttl_secs: f64,
        #[serde(default)]
        connect_timeout_secs: Option<f64>,
    },
    #[cfg_attr(not(feature = "nats"), allow(dead_code))]
    Nats {
        url: String,
        #[serde(default = "default_nats_jobs_bucket")]
        jobs_bucket: String,
        #[serde(default = "default_nats_leases_bucket")]
        leases_bucket: String,
        #[serde(default = "default_broker_lease_ttl_secs")]
        broker_lease_ttl_secs: f64,
        #[serde(default = "default_nats_num_replicas")]
        num_replicas: usize,
        #[serde(default)]
        connect_timeout_secs: Option<f64>,
        #[serde(default)]
        init_max_attempts: Option<u32>,
    },
}

fn default_nats_jobs_bucket() -> String {
    "bits-jobs".to_string()
}

fn default_nats_leases_bucket() -> String {
    "bits-leases".to_string()
}

fn default_nats_num_replicas() -> usize {
    1
}

#[cfg(feature = "tikv")]
type StoreFactory = crate::db::tikv::TiKvStore;

fn default_broker_lease_ttl_secs() -> f64 {
    30.0
}

impl RouteFactory {
    fn from_parts(
        registries: Registries,
        resolved_targets: HashMap<String, ResolvedTarget>,
        worker_server: Option<Arc<WorkerServer>>,
    ) -> Self {
        Self {
            registries,
            resolved_targets: Mutex::new(resolved_targets),
            worker_server,
        }
    }

    pub fn parse_route(
        &self,
        name: &str,
        value: &serde_json::Value,
    ) -> Result<Vec<Route>, BitsError> {
        let cached_targets = self
            .resolved_targets
            .lock()
            .map_err(|_| ConfigError::validation("routes", "route target cache mutex poisoned"))?
            .clone();

        let parse_ctx = ParseContext {
            registries: self.registries.clone(),
            resolved_targets: RefCell::new(cached_targets),
            worker_server: self.worker_server.clone(),
        };

        let routes = parse_routes(value, name, &parse_ctx)?;
        let resolved_targets = parse_ctx.resolved_targets.into_inner();

        let mut shared_targets = self
            .resolved_targets
            .lock()
            .map_err(|_| ConfigError::validation("routes", "route target cache mutex poisoned"))?;
        for (target_name, target) in resolved_targets {
            shared_targets.entry(target_name).or_insert(target);
        }

        Ok(routes)
    }

    pub(crate) fn start_worker_server(&self) -> Result<(), BitsError> {
        if let Some(ws) = &self.worker_server {
            ws.start().map_err(|e| WorkerServerError::Bind {
                address: ws.address(),
                reason: e,
            })?;
        }
        Ok(())
    }

    pub(crate) fn shutdown_worker_server(&self) {
        if let Some(ws) = &self.worker_server {
            ws.shutdown();
        }
    }
}

pub(crate) struct RuntimeConfig {
    pub router: Switch,
    pub route_factory: RouteFactory,
    pub sweep_interval: Option<Duration>,
    pub reconnect_buffer: Duration,
    pub site: String,
    pub env: String,
    pub broker_slot: u16,
    pub internal_poll_endpoint: String,
    pub internal_poll_timeout: Duration,
    pub job_store: Option<Arc<dyn PersistenceStore>>,
    pub broker_lease_ttl: Duration,
    pub persist_after: Option<Duration>,
    pub max_jobs: usize,
}

/// Parsed startup configuration split into broker runtime and HTTP server parts.
pub struct Bootstrap {
    runtime_config: RuntimeConfig,
    /// Configuration for the built-in HTTP server.
    pub server_config: ServerConfig,
    /// Whether the input YAML contained a `server:` section.
    pub had_server_section: bool,
}

impl Bootstrap {
    /// Consumes the bootstrap value and constructs a broker runtime.
    pub fn into_bits(self) -> Result<Bits, BitsError> {
        Bits::from_runtime_config(self.runtime_config)
    }

    /// Consumes the bootstrap value and returns both the broker and server config.
    pub fn into_parts(self) -> Result<(Bits, ServerConfig), BitsError> {
        let bits = Bits::from_runtime_config(self.runtime_config)?;
        Ok((bits, self.server_config))
    }
}

/// Parses the top-level YAML configuration used by the BITS binaries.
pub fn parse_bootstrap(config: &str) -> Result<Bootstrap, BitsError> {
    let raw: serde_json::Value = serde_yaml::from_str(config).map_err(ConfigError::from)?;
    if !raw.is_object() {
        return Err(
            ConfigError::validation("config", "top-level config must be a YAML mapping").into(),
        );
    }

    const KNOWN_SECTIONS: &[&str] = &[
        "bits",
        "server",
        "routes",
        "checks",
        "transforms",
        "targets",
    ];
    if let Some(map) = raw.as_object() {
        for key in map.keys() {
            if !KNOWN_SECTIONS.contains(&key.as_str()) {
                tracing::warn!("unknown top-level config section '{key}' will be ignored");
            }
        }
    }

    let bits_cfg: BitsConfig = raw
        .get("bits")
        .cloned()
        .map(|v| {
            serde_json::from_value(v).map_err(|e| ConfigError::Decode {
                path: "bits".to_string(),
                target: "BitsConfig".to_string(),
                source: e,
            })
        })
        .transpose()?
        .unwrap_or_default();

    let worker_server: Option<Arc<WorkerServer>> = bits_cfg.worker_server.as_ref().map(|ws_cfg| {
        Arc::new(WorkerServer::with_advertised_addr(
            &ws_cfg.host,
            ws_cfg.port,
            ws_cfg.advertised_addr.clone(),
        ))
    });

    if bits_cfg.broker_id_prefix.is_some() {
        tracing::warn!(
            "bits.broker_id_prefix is deprecated and ignored; broker identity is bits.site-bits.env-allocated_slot"
        );
    }
    let site = require_site_env_tag("bits.site", bits_cfg.site.as_ref())?;
    let env = require_site_env_tag("bits.env", bits_cfg.env.as_ref())?;
    let has_explicit_endpoint = bits_cfg.internal_poll_endpoint.is_some();
    let has_explicit_poll_timeout = bits_cfg.internal_poll_timeout_secs.is_some();
    let internal_poll_timeout = positive_duration_secs(
        "bits.internal_poll_timeout_secs",
        bits_cfg.internal_poll_timeout_secs.unwrap_or(2.5),
    )?;
    let sweep_interval = bits_cfg
        .sweep_interval_secs
        .map(|v| positive_duration_secs("bits.sweep_interval_secs", v))
        .transpose()?;
    let reconnect_buffer = positive_duration_secs(
        "bits.reconnect_buffer_secs",
        bits_cfg.reconnect_buffer_secs.unwrap_or(5.0),
    )?;
    let mut persist_after = bits_cfg
        .persist_after_secs
        .map(|v| duration_secs("bits.persist_after_secs", v))
        .transpose()?;

    if let Some(0) = bits_cfg.max_jobs {
        return Err(ConfigError::validation("bits.max_jobs", "must be greater than zero").into());
    }

    let server_value = raw.get("server").cloned();
    let had_server_section = server_value.is_some();
    let server_config: ServerConfig = server_value
        .map(|v| {
            serde_json::from_value(v).map_err(|e| ConfigError::Decode {
                path: "server".to_string(),
                target: "ServerConfig".to_string(),
                source: e,
            })
        })
        .transpose()?
        .unwrap_or_default();

    let poll_timeout =
        positive_duration_secs("server.poll_timeout_secs", server_config.poll_timeout_secs)?;
    if server_config.retry_after_secs == 0 {
        return Err(ConfigError::validation(
            "server.retry_after_secs",
            "must be greater than zero",
        )
        .into());
    }

    let internal_poll_endpoint = bits_cfg.internal_poll_endpoint.unwrap_or_else(|| {
        let is_wildcard_v4 = server_config.host == "0.0.0.0";
        let is_wildcard_v6 = server_config.host == "::";
        let host = if is_wildcard_v4 || is_wildcard_v6 {
            tracing::warn!(
                "server.host is a wildcard address ({}); deriving internal_poll_endpoint \
                 as a loopback address which is only reachable from localhost. Set \
                 bits.internal_poll_endpoint explicitly for multi-broker deployments.",
                server_config.host,
            );
            if is_wildcard_v6 { "::1" } else { "127.0.0.1" }
        } else {
            &server_config.host
        };
        if host.contains(':') {
            format!("http://[{}]:{}/job", host, server_config.port)
        } else {
            format!("http://{}:{}/job", host, server_config.port)
        }
    });

    // Only validate persist timing when a persistence backend is configured.
    // Without a backend, persist_after is coerced to None later anyway.
    const PERSIST_GUARD: Duration = Duration::from_secs(1);
    if bits_cfg.persistence.is_some()
        && let Some(persist_after) = persist_after
        && persist_after + PERSIST_GUARD >= poll_timeout
    {
        return Err(ConfigError::validation(
            "bits.persist_after_secs",
            format!(
                "bits.persist_after_secs ({:.3} s) + 1 s guard must be less than \
                 server.poll_timeout_secs ({:.3} s)",
                persist_after.as_secs_f64(),
                poll_timeout.as_secs_f64(),
            ),
        )
        .into());
    }

    let (job_store, broker_lease_ttl): (Option<Arc<dyn PersistenceStore>>, Duration) =
        match bits_cfg.persistence {
            Some(PersistenceConfig::Tikv {
                endpoints,
                broker_lease_ttl_secs,
                connect_timeout_secs,
            }) => {
                if endpoints.is_empty() {
                    return Err(ConfigError::validation(
                        "bits.persistence.endpoints",
                        "must not be empty",
                    )
                    .into());
                }
                let ttl = Duration::try_from_secs_f64(broker_lease_ttl_secs).map_err(|e| {
                    ConfigError::validation("bits.persistence.broker_lease_ttl_secs", e.to_string())
                })?;
                if ttl < Duration::from_secs(1) {
                    return Err(ConfigError::validation(
                        "bits.persistence.broker_lease_ttl_secs",
                        "must be at least 1 second",
                    )
                    .into());
                }
                #[cfg(feature = "tikv")]
                let connect_timeout = match connect_timeout_secs {
                    Some(secs) => {
                        positive_duration_secs("bits.persistence.connect_timeout_secs", secs)?
                    }
                    None => Duration::from_secs(10),
                };
                #[cfg(not(feature = "tikv"))]
                {
                    if let Some(secs) = connect_timeout_secs {
                        positive_duration_secs("bits.persistence.connect_timeout_secs", secs)?;
                    }
                    return Err(ConfigError::FeatureDisabled {
                        path: "bits.persistence.type".into(),
                        feature: "tikv".into(),
                    }
                    .into());
                }
                #[cfg(feature = "tikv")]
                {
                    (
                        Some(Arc::new(StoreFactory::new(endpoints, connect_timeout))
                            as Arc<dyn PersistenceStore>),
                        ttl,
                    )
                }
            }
            Some(PersistenceConfig::Nats {
                url,
                jobs_bucket,
                leases_bucket,
                broker_lease_ttl_secs,
                num_replicas,
                connect_timeout_secs,
                init_max_attempts,
            }) => {
                if url.is_empty() {
                    return Err(ConfigError::validation(
                        "bits.persistence.url",
                        "must not be empty",
                    )
                    .into());
                }
                if jobs_bucket.is_empty() {
                    return Err(ConfigError::validation(
                        "bits.persistence.jobs_bucket",
                        "must not be empty",
                    )
                    .into());
                }
                if leases_bucket.is_empty() {
                    return Err(ConfigError::validation(
                        "bits.persistence.leases_bucket",
                        "must not be empty",
                    )
                    .into());
                }
                if num_replicas < 1 {
                    return Err(ConfigError::validation(
                        "bits.persistence.num_replicas",
                        "must be at least 1",
                    )
                    .into());
                }
                let ttl = Duration::try_from_secs_f64(broker_lease_ttl_secs).map_err(|e| {
                    ConfigError::validation("bits.persistence.broker_lease_ttl_secs", e.to_string())
                })?;
                if ttl < Duration::from_secs(1) {
                    return Err(ConfigError::validation(
                        "bits.persistence.broker_lease_ttl_secs",
                        "must be at least 1 second",
                    )
                    .into());
                }
                #[cfg(feature = "nats")]
                let connect_timeout = match connect_timeout_secs {
                    Some(secs) => {
                        positive_duration_secs("bits.persistence.connect_timeout_secs", secs)?
                    }
                    None => Duration::from_secs(10),
                };
                let max_attempts = init_max_attempts.unwrap_or(6);
                if max_attempts < 1 {
                    return Err(ConfigError::validation(
                        "bits.persistence.init_max_attempts",
                        "must be at least 1",
                    )
                    .into());
                }
                #[cfg(not(feature = "nats"))]
                {
                    if let Some(secs) = connect_timeout_secs {
                        positive_duration_secs("bits.persistence.connect_timeout_secs", secs)?;
                    }
                    return Err(ConfigError::FeatureDisabled {
                        path: "bits.persistence.type".into(),
                        feature: "nats".into(),
                    }
                    .into());
                }
                #[cfg(feature = "nats")]
                {
                    let store = crate::db::nats::NatsStore::new(
                        url,
                        jobs_bucket,
                        leases_bucket,
                        ttl,
                        num_replicas,
                        connect_timeout,
                    );
                    let handle = tokio::runtime::Handle::try_current().map_err(|_| {
                        ConfigError::validation(
                            "bits.persistence.type",
                            "nats requires a running Tokio runtime for init",
                        )
                    })?;

                    // Retry NATS init with backoff. In Kubernetes the NATS cluster
                    // may not be ready when the broker pod starts; retrying here
                    // avoids CrashLoopBackOff delays from the kubelet.
                    let mut last_err: Option<String> = None;
                    let mut delay = Duration::from_secs(1);
                    for attempt in 1..=max_attempts {
                        match tokio::task::block_in_place(|| handle.block_on(store.init())) {
                            Ok(()) => {
                                last_err = None;
                                break;
                            }
                            Err(e) => {
                                last_err = Some(e.to_string());
                                if attempt < max_attempts {
                                    tracing::warn!(
                                        attempt,
                                        max_attempts,
                                        error = %last_err.as_deref().unwrap_or("unknown"),
                                        retry_in_secs = delay.as_secs(),
                                        "NATS init failed, retrying"
                                    );
                                    tokio::task::block_in_place(|| std::thread::sleep(delay));
                                    delay = delay.saturating_mul(2).min(Duration::from_secs(10));
                                }
                            }
                        }
                    }
                    if let Some(reason) = last_err {
                        return Err(ConfigError::PersistenceInit {
                            path: "bits.persistence".into(),
                            backend: "nats".into(),
                            reason,
                        }
                        .into());
                    }

                    (Some(Arc::new(store) as Arc<dyn PersistenceStore>), ttl)
                }
            }
            None => {
                if persist_after.is_some() {
                    tracing::warn!(
                        "bits.persist_after_secs is set but has no effect without bits.persistence"
                    );
                    persist_after = None;
                }
                if has_explicit_endpoint {
                    tracing::warn!(
                        "bits.internal_poll_endpoint is set but has no effect without bits.persistence"
                    );
                }
                if has_explicit_poll_timeout {
                    tracing::warn!(
                        "bits.internal_poll_timeout_secs is set but has no effect without bits.persistence"
                    );
                }
                (
                    None,
                    Duration::from_secs(default_broker_lease_ttl_secs() as u64),
                )
            }
        };

    let broker_slot = match &job_store {
        Some(store) => allocate_startup_broker_slot(store.clone(), site.clone(), env.clone())?,
        None => 0,
    };

    let checks: HashMap<String, serde_json::Value> = raw
        .get("checks")
        .map(|v| {
            serde_json::from_value(v.clone()).map_err(|e| ConfigError::Decode {
                path: "checks".to_string(),
                target: "map".to_string(),
                source: e,
            })
        })
        .transpose()?
        .unwrap_or_default();
    let transforms: HashMap<String, serde_json::Value> = raw
        .get("transforms")
        .map(|v| {
            serde_json::from_value(v.clone()).map_err(|e| ConfigError::Decode {
                path: "transforms".to_string(),
                target: "map".to_string(),
                source: e,
            })
        })
        .transpose()?
        .unwrap_or_default();
    let targets: HashMap<String, serde_json::Value> = raw
        .get("targets")
        .map(|v| {
            serde_json::from_value(v.clone()).map_err(|e| ConfigError::Decode {
                path: "targets".to_string(),
                target: "map".to_string(),
                source: e,
            })
        })
        .transpose()?
        .unwrap_or_default();

    let route_factory = RouteFactory::from_parts(
        Registries {
            checks,
            transforms,
            targets,
        },
        HashMap::new(),
        worker_server.clone(),
    );

    let branches = if let Some(routes_val) = raw.get("routes") {
        route_factory.parse_route("routes", routes_val)?
    } else {
        vec![]
    };

    let has_branches = !branches.is_empty();
    let router = Switch::new(branches);
    if has_branches {
        validate_switch(&router)?;
    }

    Ok(Bootstrap {
        runtime_config: RuntimeConfig {
            router,
            route_factory,
            sweep_interval,
            reconnect_buffer,
            site,
            env,
            broker_slot,
            internal_poll_endpoint,
            internal_poll_timeout,
            job_store,
            broker_lease_ttl,
            persist_after,
            max_jobs: bits_cfg.max_jobs.unwrap_or(DEFAULT_MAX_JOBS),
        },
        server_config,
        had_server_section,
    })
}

fn validate_switch(switch: &Switch) -> Result<(), BitsError> {
    switch.validate().map_err(BitsError::from)
}

/// Parses an ordered list of named routes from a JSON array of single-key objects.
///
/// Each element must be a JSON object with exactly one key, where the key is the
/// route name and the value is the array of actions for that route.
///
/// ```yaml
/// routes:
///   - my_route:
///       - action1
///       - action2
/// ```
fn parse_routes(
    value: &serde_json::Value,
    section: &str,
    ctx: &ParseContext,
) -> Result<Vec<Route>, BitsError> {
    let entries = value.as_array().ok_or_else(|| RoutingError::InvalidRoute {
        route: section.to_string(),
        reason: "must be an array".to_string(),
    })?;

    let mut routes = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        let map = entry
            .as_object()
            .ok_or_else(|| RoutingError::InvalidRoute {
                route: format!("{section}[{index}]"),
                reason: "entry must be an object".to_string(),
            })?;
        if map.len() != 1 {
            return Err(RoutingError::InvalidRoute {
                route: format!("{section}[{index}]"),
                reason: format!(
                    "entry must have exactly one key (the route name), got {}",
                    map.len()
                ),
            }
            .into());
        }
        let (name, route_val) = map
            .iter()
            .next()
            .ok_or_else(|| RoutingError::InvalidRoute {
                route: format!("{section}[{index}]"),
                reason: "entry must have exactly one key (the route name)".to_string(),
            })?;
        let action_values = route_val
            .as_array()
            .ok_or_else(|| RoutingError::InvalidRoute {
                route: name.clone(),
                reason: format!("{section} route must be an array of actions"),
            })?;
        let actions = action_values
            .iter()
            .map(|v| parse_action(v, name, ctx))
            .collect::<Result<Vec<_>, _>>()?;
        routes.push(Route::new(name.clone(), actions));
    }
    Ok(routes)
}

fn parse_action(
    value: &serde_json::Value,
    route_name: &str,
    ctx: &ParseContext,
) -> Result<Action, BitsError> {
    match value {
        serde_json::Value::String(name) => {
            if name == "persist" {
                return Err(RoutingError::InvalidAction {
                    route: route_name.to_string(),
                    action: name.clone(),
                    reason: "'persist' step has been removed; use bits.persist_after_secs instead"
                        .to_string(),
                }
                .into());
            }
            let (ns, entry_name) = split_ns(name, route_name)?;
            resolve_named(ns, entry_name, route_name, ctx)
        }
        serde_json::Value::Object(map) => {
            if let Some(switch_val) = map.get("switch") {
                let routes = parse_routes(switch_val, "switch", ctx)?;
                let switch = Switch::new(routes);
                validate_switch(&switch)?;
                return Ok(Action::Switch(switch));
            }

            for (key, config) in map {
                if key.contains("::") {
                    let (ns, action_name) = split_ns(key, route_name)?;
                    if action_name == "remote" {
                        return Err(RoutingError::InvalidAction {
                            route: route_name.to_string(),
                            action: key.clone(),
                            reason: "target::remote must be defined as a named registry entry (not inline); the pool name is derived from the registry entry name".to_string(),
                        }
                        .into());
                    }
                    let action = create_action(action_name, config.clone()).map_err(|e| {
                        RoutingError::InvalidAction {
                            route: route_name.to_string(),
                            action: key.clone(),
                            reason: e.to_string(),
                        }
                    })?;
                    validate_inline_action(ns, route_name, key, &action)?;
                    let settings = parse_dispatcher_fields(map.get("dispatcher"))?;
                    let silent = map
                        .get("silent")
                        .map(|v| {
                            v.as_bool().ok_or_else(|| RoutingError::InvalidAction {
                                route: route_name.to_string(),
                                action: key.clone(),
                                reason: "silent must be a boolean".to_string(),
                            })
                        })
                        .transpose()?;
                    return attach_dispatcher(
                        action_name,
                        action_name,
                        action,
                        settings,
                        silent,
                        ctx,
                    );
                }
            }

            Err(RoutingError::InvalidAction {
                route: route_name.to_string(),
                action: value.to_string(),
                reason: "unrecognised action".to_string(),
            }
            .into())
        }
        _ => Err(RoutingError::InvalidAction {
            route: route_name.to_string(),
            action: value.to_string(),
            reason: "action must be string or object".to_string(),
        }
        .into()),
    }
}

fn split_ns<'a>(s: &'a str, route_name: &str) -> Result<(&'a str, &'a str), BitsError> {
    let mut parts = s.splitn(2, "::");
    let ns = parts.next().ok_or_else(|| RoutingError::InvalidAction {
        route: route_name.to_string(),
        action: s.to_string(),
        reason: "invalid reference".to_string(),
    })?;
    let name = parts.next().ok_or_else(|| RoutingError::InvalidAction {
        route: route_name.to_string(),
        action: s.to_string(),
        reason: "must be namespace::name".to_string(),
    })?;
    Ok((ns, name))
}

fn resolve_named(
    ns: &str,
    name: &str,
    route_name: &str,
    ctx: &ParseContext,
) -> Result<Action, BitsError> {
    match ns {
        "check" => {
            let entry =
                ctx.registries
                    .checks
                    .get(name)
                    .ok_or_else(|| RoutingError::InvalidAction {
                        route: route_name.to_string(),
                        action: format!("{ns}::{name}"),
                        reason: format!("unknown check '{name}'"),
                    })?;
            action_from_entry(ns, name, entry, ctx)
        }
        "transform" => {
            let entry =
                ctx.registries
                    .transforms
                    .get(name)
                    .ok_or_else(|| RoutingError::InvalidAction {
                        route: route_name.to_string(),
                        action: format!("{ns}::{name}"),
                        reason: format!("unknown transform '{name}'"),
                    })?;
            action_from_entry(ns, name, entry, ctx)
        }
        "target" => {
            if let Some((target, dispatcher, surface)) = ctx.resolved_targets.borrow().get(name) {
                return Ok(Action::Target(
                    Arc::clone(target),
                    dispatcher.clone(),
                    *surface,
                ));
            }
            let entry =
                ctx.registries
                    .targets
                    .get(name)
                    .ok_or_else(|| RoutingError::InvalidAction {
                        route: route_name.to_string(),
                        action: format!("{ns}::{name}"),
                        reason: format!("unknown target '{name}'"),
                    })?;
            let action = action_from_entry(ns, name, entry, ctx)?;
            if let Action::Target(target, dispatcher, surface) = &action {
                ctx.resolved_targets.borrow_mut().insert(
                    name.to_string(),
                    (Arc::clone(target), dispatcher.clone(), *surface),
                );
            }
            Ok(action)
        }
        _ => Err(RoutingError::InvalidAction {
            route: route_name.to_string(),
            action: format!("{ns}::{name}"),
            reason: format!("unknown namespace '{ns}'"),
        }
        .into()),
    }
}

fn validate_inline_action(
    ns: &str,
    route_name: &str,
    action_name: &str,
    action: &Action,
) -> Result<(), BitsError> {
    match (ns, &action) {
        ("check", Action::Check(..)) => Ok(()),
        ("transform", Action::Transform(..)) => Ok(()),
        ("target", Action::Target(..)) => Ok(()),
        _ => Err(RoutingError::InvalidAction {
            route: route_name.to_string(),
            action: action_name.to_string(),
            reason: format!("inline action namespace '{ns}' does not match action type"),
        }
        .into()),
    }
}

fn parse_dispatcher_fields(
    dispatcher: Option<&serde_json::Value>,
) -> Result<DispatcherSettings, BitsError> {
    let mut settings = DispatcherSettings::default();

    if let Some(d) = dispatcher {
        let map = d
            .as_object()
            .ok_or_else(|| ConfigError::validation("dispatcher", "must be an object"))?;
        settings.queue = map
            .get("queue")
            .map(|v| {
                serde_json::from_value(v.clone()).map_err(|e| ConfigError::Decode {
                    path: "dispatcher.queue".to_string(),
                    target: "QueueKind".to_string(),
                    source: e,
                })
            })
            .transpose()?;
        settings.executor = map
            .get("executor")
            .map(|v| {
                serde_json::from_value(v.clone()).map_err(|e| ConfigError::Decode {
                    path: "dispatcher.executor".to_string(),
                    target: "ExecutorKind".to_string(),
                    source: e,
                })
            })
            .transpose()?;
        if let Some(v) = map.get("queue_capacity") {
            let cap = v.as_u64().ok_or_else(|| {
                ConfigError::validation("dispatcher.queue_capacity", "must be a positive integer")
            })?;
            if cap == 0 {
                return Err(ConfigError::validation(
                    "dispatcher.queue_capacity",
                    "must be greater than zero",
                )
                .into());
            }
            settings.queue_capacity = usize::try_from(cap).map_err(|_| {
                ConfigError::validation(
                    "dispatcher.queue_capacity",
                    "value too large for this platform",
                )
            })?;
        }
        if map.contains_key("concurrency") {
            return Err(ConfigError::validation(
                "dispatcher.concurrency",
                "removed; set concurrency inside the executor block instead",
            )
            .into());
        }
        if map.contains_key("persistent") || map.contains_key("lock_ttl_secs") {
            return Err(ConfigError::validation(
                "dispatcher",
                "persistent/lock_ttl_secs removed; use bits.persist_after_secs policy",
            )
            .into());
        }
    }

    Ok(settings)
}

fn action_from_entry(
    ns: &str,
    entry_name: &str,
    entry: &serde_json::Value,
    ctx: &ParseContext,
) -> Result<Action, BitsError> {
    let map = entry.as_object().ok_or_else(|| {
        ConfigError::validation(
            format!("{ns}.{entry_name}"),
            format!("must be an object, got: {entry}"),
        )
    })?;
    if map.contains_key("persistent") || map.contains_key("lock_ttl_secs") {
        return Err(ConfigError::validation(
            format!("{ns}.{entry_name}"),
            "persistent/lock_ttl_secs removed; use bits.persist_after_secs",
        )
        .into());
    }
    let type_name = map
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ConfigError::missing(format!("{ns}.{entry_name}.type")))?;
    let settings = parse_dispatcher_fields(map.get("dispatcher"))?;
    let silent = map
        .get("silent")
        .map(|v| {
            v.as_bool().ok_or_else(|| {
                ConfigError::validation(format!("{ns}.{entry_name}.silent"), "must be a boolean")
            })
        })
        .transpose()?;

    let remaining: serde_json::Map<_, _> = map
        .iter()
        .filter(|(k, _)| !matches!(k.as_str(), "type" | "dispatcher" | "silent"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let config = if remaining.is_empty() {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        remaining.into()
    };
    let action = create_action(type_name, config)
        .map_err(|e| ConfigError::validation(format!("{ns}.{entry_name}.type"), e.to_string()))?;
    attach_dispatcher(entry_name, type_name, action, settings, silent, ctx)
}

fn attach_dispatcher(
    entry_name: &str,
    action_name: &str,
    action: Action,
    mut settings: DispatcherSettings,
    silent: Option<bool>,
    ctx: &ParseContext,
) -> Result<Action, BitsError> {
    let is_remote_action = action_name == "remote";
    let is_remote_pool = matches!(&settings.executor, Some(ExecutorKind::RemotePool { .. }));

    if is_remote_action {
        match &settings.executor {
            None => {
                settings.executor = Some(ExecutorKind::RemotePool {
                    config: RemotePoolConfig {
                        heartbeat_timeout_secs: 60.0,
                    },
                })
            }
            Some(ExecutorKind::RemotePool { .. }) => {}
            Some(_) => {
                return Err(ConfigError::validation(
                    "dispatcher.executor",
                    "'remote' target requires executor: remote_pool",
                )
                .into());
            }
        }
        if ctx.worker_server.is_none() {
            return Err(ConfigError::missing("bits.worker_server").into());
        }
    } else if is_remote_pool {
        return Err(ConfigError::validation(
            "dispatcher.executor",
            "remote_pool requires a 'remote' target action",
        )
        .into());
    }

    let has_dispatcher = settings.queue.is_some() || settings.executor.is_some();

    match action {
        Action::Check(check, _, _) => {
            let dispatcher = Dispatcher::from_config(
                settings.queue.as_ref(),
                settings.executor.as_ref(),
                None,
                None,
                settings.queue_capacity,
            )
            .map_err(|e| ConfigError::validation("dispatcher", e))?;
            Ok(Action::Check(check, dispatcher, silent))
        }
        Action::Transform(transform, _, _) => {
            let dispatcher = Dispatcher::from_config(
                settings.queue.as_ref(),
                settings.executor.as_ref(),
                None,
                None,
                settings.queue_capacity,
            )
            .map_err(|e| ConfigError::validation("dispatcher", e))?;
            Ok(Action::Transform(transform, dispatcher, silent))
        }
        Action::Target(target, _, _) => {
            let pool_name = if is_remote_action {
                Some(entry_name)
            } else {
                None
            };
            let dispatcher = Dispatcher::from_config(
                settings.queue.as_ref(),
                settings.executor.as_ref(),
                pool_name,
                ctx.worker_server.clone(),
                settings.queue_capacity,
            )
            .map_err(|e| ConfigError::validation("dispatcher", e))?;
            Ok(Action::Target(target, dispatcher, silent))
        }
        _ if has_dispatcher => Err(ConfigError::validation(
            "dispatcher",
            "config only valid for Check, Transform, and Target actions",
        )
        .into()),
        _ => Ok(action),
    }
}

impl Clone for Registries {
    fn clone(&self) -> Self {
        Self {
            checks: self.checks.clone(),
            transforms: self.transforms.clone(),
            targets: self.targets.clone(),
        }
    }
}

impl Default for RouteFactory {
    fn default() -> Self {
        Self::from_parts(Registries::default(), HashMap::new(), None)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;

    use super::{RouteFactory, RuntimeConfig};
    use crate::Bits;
    use crate::actions::Action;
    use crate::db::{
        BrokerLeaseRecord, ClaimResult, DbError, PersistenceStore, PersistentJobRecord,
    };

    fn extract_target(route: &crate::routing::Route) -> Arc<dyn crate::actions::TargetAction> {
        match route.actions.first() {
            Some(Action::Target(target, _, _)) => Arc::clone(target),
            _ => panic!("expected first action to be a target"),
        }
    }

    fn assert_send_sync<T: Send + Sync>() {}

    fn assert_site_env_config_error(config: &str, expected_field: &str) {
        let err = crate::config::parse_bootstrap(config)
            .err()
            .expect("invalid site/env config should be rejected");
        assert_eq!(err.code(), "CONFIG_VALIDATION", "unexpected error: {err}");
        assert!(
            err.to_string().contains(expected_field),
            "error should identify {expected_field}: {err}"
        );
    }

    fn runtime_config_with_site_env(
        site: &str,
        env: &str,
        broker_slot: u16,
        job_store: Option<Arc<dyn PersistenceStore>>,
    ) -> RuntimeConfig {
        RuntimeConfig {
            router: crate::routing::switch::Switch::new(vec![]),
            route_factory: RouteFactory::default(),
            sweep_interval: Some(Duration::from_secs(60)),
            reconnect_buffer: Duration::from_secs(5),
            site: site.to_string(),
            env: env.to_string(),
            broker_slot,
            internal_poll_endpoint: "http://127.0.0.1:8080/job".to_string(),
            internal_poll_timeout: Duration::from_millis(2500),
            job_store,
            broker_lease_ttl: Duration::from_secs(30),
            persist_after: None,
            max_jobs: crate::bits::DEFAULT_MAX_JOBS,
        }
    }

    #[test]
    fn site_env_missing_config_is_rejected() {
        let err = crate::config::parse_bootstrap("{}")
            .err()
            .expect("missing bits.site/bits.env should be rejected");
        assert_eq!(
            err.code(),
            "CONFIG_MISSING_FIELD",
            "unexpected error: {err}"
        );
        assert!(
            err.to_string().contains("bits.site") || err.to_string().contains("bits.env"),
            "error should identify the missing site/env field: {err}"
        );
    }

    #[test]
    fn site_env_accepts_one_two_three_character_tags() {
        for (site, env) in [("a", "0"), ("b1", "d2"), ("bol", "123")] {
            let config = format!(
                r#"
bits:
  site: {site}
  env: {env}
"#
            );
            crate::config::parse_bootstrap(&config)
                .unwrap_or_else(|err| panic!("site={site:?} env={env:?} should parse: {err}"));
        }
    }

    #[test]
    fn site_env_rejects_empty_too_long_uppercase_and_punctuation() {
        for (field, value) in [
            ("site", "''"),
            ("env", "''"),
            ("site", "abcd"),
            ("env", "abcd"),
            ("site", "AB"),
            ("env", "AB"),
            ("site", "a-b"),
            ("env", "a-b"),
        ] {
            let (site, env) = if field == "site" {
                (value, "dev")
            } else {
                ("bol", value)
            };
            let config = format!(
                r#"
bits:
  site: {site}
  env: {env}
"#
            );
            assert_site_env_config_error(&config, &format!("bits.{field}"));
        }
    }

    #[tokio::test]
    async fn site_env_startup_uses_allocated_slot_in_broker_id() {
        let store = Arc::new(crate::db::memory::MemoryStore::new());
        let slot = crate::db::BrokerSlotStore::allocate_broker_slot(store.as_ref(), "bol", "dev")
            .await
            .expect("slot allocation should succeed");
        assert_eq!(slot, 0);

        let bits = Bits::from_runtime_config(runtime_config_with_site_env(
            "bol",
            "dev",
            slot,
            Some(store as Arc<dyn PersistenceStore>),
        ))
        .expect("broker should start with allocated slot");

        assert_eq!(bits.broker_id(), "bol-dev-0");
    }

    #[tokio::test]
    async fn site_env_no_persistence_uses_ephemeral_slot_zero() {
        let config = r#"
bits:
  site: a
  env: dev
"#;
        let bits = crate::config::parse_bootstrap(config)
            .expect("site/env-only config should parse")
            .into_bits()
            .expect("single-process broker should start without persistence");

        assert_eq!(bits.broker_id(), "a-dev-0");
    }

    struct UnsupportedSlotStore;

    #[async_trait]
    impl crate::db::JobStore for UnsupportedSlotStore {
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
            Ok(ClaimResult::NotFound)
        }
    }

    #[async_trait]
    impl crate::db::BrokerLeaseStore for UnsupportedSlotStore {
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

    async fn runtime_config_after_startup_slot_allocation(
        site: &str,
        env: &str,
        store: Arc<dyn PersistenceStore>,
    ) -> Result<RuntimeConfig, crate::error::BitsError> {
        let broker_slot = store.allocate_broker_slot(site, env).await?;
        Ok(runtime_config_with_site_env(
            site,
            env,
            broker_slot,
            Some(store),
        ))
    }

    #[tokio::test]
    async fn site_env_startup_fails_when_slot_allocation_fails() {
        let err = runtime_config_after_startup_slot_allocation(
            "bol",
            "dev",
            Arc::new(UnsupportedSlotStore) as Arc<dyn PersistenceStore>,
        )
        .await
        .err()
        .expect("startup should fail when slot allocation fails");

        assert!(matches!(err, crate::error::BitsError::Persistence(_)));
        assert_eq!(err.code(), "PERSISTENCE_BACKEND");
    }

    #[tokio::test]
    async fn test_empty_pipeline() {
        let config = r#"
bits:
  site: tst
  env: dev
routes:
  - test_pipeline: []
"#;
        let err = Bits::from_config(config)
            .err()
            .expect("empty pipeline should be rejected");
        assert!(
            err.to_string().contains("must not be empty"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn route_factory_is_send_sync() {
        assert_send_sync::<crate::config::RouteFactory>();
    }

    #[tokio::test]
    async fn test_no_routes_section() {
        let config = r#"
bits:
  site: tst
  env: dev
targets:
  my_target:
    type: http
    url: http://127.0.0.1:1
"#;
        let bootstrap =
            crate::config::parse_bootstrap(config).expect("should parse without routes section");
        let bits = bootstrap.into_bits().expect("should build bits");
        assert!(bits.route_names().is_empty());
    }

    #[tokio::test]
    async fn test_route_factory_shared_targets() {
        let config = r#"
bits:
  site: tst
  env: dev
targets:
  my_target:
    type: http
    url: http://127.0.0.1:1
"#;

        let bits = crate::config::parse_bootstrap(config)
            .expect("should parse")
            .into_bits()
            .expect("should build bits");

        let routes1 = bits
            .route_factory
            .parse_route(
                "extra_routes_1",
                &serde_json::json!([
                    {"route_a": ["target::my_target"]}
                ]),
            )
            .expect("parse route set 1");
        let routes2 = bits
            .route_factory
            .parse_route(
                "extra_routes_2",
                &serde_json::json!([
                    {"route_b": ["target::my_target"]}
                ]),
            )
            .expect("parse route set 2");

        let target1 = extract_target(&routes1[0]);
        let target2 = extract_target(&routes2[0]);
        assert!(Arc::ptr_eq(&target1, &target2));
    }
}
