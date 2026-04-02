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
    Option<Arc<crate::circuit_breaker::CircuitBreaker>>,
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
    pub broker_id_prefix: String,
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

    let worker_server: Option<Arc<WorkerServer>> = bits_cfg
        .worker_server
        .as_ref()
        .map(|ws_cfg| Arc::new(WorkerServer::new(&ws_cfg.host, ws_cfg.port)));

    let broker_id = bits_cfg
        .broker_id_prefix
        .unwrap_or_else(|| format!("broker-{}", uuid::Uuid::new_v4()));
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

    let (job_store, broker_lease_ttl) = match bits_cfg.persistence {
        Some(PersistenceConfig::Tikv {
            endpoints,
            broker_lease_ttl_secs,
            #[cfg(feature = "tikv")]
            connect_timeout_secs,
            #[cfg(not(feature = "tikv"))]
                connect_timeout_secs: _,
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
            #[cfg(not(feature = "tikv"))]
            {
                return Err(ConfigError::FeatureDisabled {
                    path: "bits.persistence.type".into(),
                    feature: "tikv".into(),
                }
                .into());
            }
            #[cfg(feature = "tikv")]
            {
                let connect_timeout = match connect_timeout_secs {
                    Some(secs) => {
                        positive_duration_secs("bits.persistence.connect_timeout_secs", secs)?
                    }
                    None => Duration::from_secs(10),
                };
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
            #[cfg(feature = "nats")]
            connect_timeout_secs,
            #[cfg(not(feature = "nats"))]
                connect_timeout_secs: _,
            #[cfg(feature = "nats")]
            init_max_attempts,
            #[cfg(not(feature = "nats"))]
                init_max_attempts: _,
        }) => {
            if url.is_empty() {
                return Err(
                    ConfigError::validation("bits.persistence.url", "must not be empty").into(),
                );
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
            #[cfg(not(feature = "nats"))]
            {
                return Err(ConfigError::FeatureDisabled {
                    path: "bits.persistence.type".into(),
                    feature: "nats".into(),
                }
                .into());
            }
            #[cfg(feature = "nats")]
            {
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
            broker_id_prefix: broker_id,
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
                        None,
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
            if let Some((target, dispatcher, surface, breaker)) =
                ctx.resolved_targets.borrow().get(name)
            {
                return Ok(Action::Target(
                    Arc::clone(target),
                    dispatcher.clone(),
                    *surface,
                    breaker.clone(),
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
            if let Action::Target(target, dispatcher, surface, breaker) = &action {
                ctx.resolved_targets.borrow_mut().insert(
                    name.to_string(),
                    (
                        Arc::clone(target),
                        dispatcher.clone(),
                        *surface,
                        breaker.clone(),
                    ),
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

    let cb_config = map
        .get("circuit_breaker")
        .map(|v| {
            serde_json::from_value::<crate::circuit_breaker::CircuitBreakerConfig>(v.clone())
                .map_err(|e| {
                    ConfigError::validation(
                        format!("{ns}.{entry_name}.circuit_breaker"),
                        e.to_string(),
                    )
                })
        })
        .transpose()?;
    if let Some(ref cb) = cb_config {
        cb.validate().map_err(|e| {
            ConfigError::validation(format!("{ns}.{entry_name}.circuit_breaker"), e)
        })?;
    }

    let remaining: serde_json::Map<_, _> = map
        .iter()
        .filter(|(k, _)| {
            !matches!(
                k.as_str(),
                "type" | "dispatcher" | "silent" | "circuit_breaker"
            )
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let config = if remaining.is_empty() {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        remaining.into()
    };
    let action = create_action(type_name, config)
        .map_err(|e| ConfigError::validation(format!("{ns}.{entry_name}.type"), e.to_string()))?;
    attach_dispatcher(
        entry_name, type_name, action, settings, silent, cb_config, ctx,
    )
}

fn attach_dispatcher(
    entry_name: &str,
    action_name: &str,
    action: Action,
    mut settings: DispatcherSettings,
    silent: Option<bool>,
    cb_config: Option<crate::circuit_breaker::CircuitBreakerConfig>,
    ctx: &ParseContext,
) -> Result<Action, BitsError> {
    if cb_config.is_some() && !matches!(action, Action::Target(..)) {
        return Err(ConfigError::validation(
            format!("{entry_name}.circuit_breaker"),
            "only valid on target actions",
        )
        .into());
    }

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
        Action::Target(target, _, _, _) => {
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
            let breaker = cb_config.map(|cfg| {
                Arc::new(crate::circuit_breaker::CircuitBreaker::new(
                    &cfg,
                    entry_name.to_string(),
                ))
            });
            Ok(Action::Target(target, dispatcher, silent, breaker))
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

    use crate::Bits;
    use crate::actions::Action;

    fn extract_target(route: &crate::routing::Route) -> Arc<dyn crate::actions::TargetAction> {
        match route.actions.first() {
            Some(Action::Target(target, _, _, _)) => Arc::clone(target),
            _ => panic!("expected first action to be a target"),
        }
    }

    fn assert_send_sync<T: Send + Sync>() {}

    #[tokio::test]
    async fn test_empty_pipeline() {
        let config = r#"
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
