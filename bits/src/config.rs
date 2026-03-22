use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use crate::Bits;
use crate::actions::Action;
use crate::actions::registry::create_action;
use crate::actions::{TargetAction, TargetResult};
use crate::db::PersistenceStore;
use crate::dispatcher::{Dispatcher, ExecutorKind, QueueKind, RemotePoolConfig};
use crate::routing::{Route, switch::Switch};
use crate::server::ServerConfig;
use crate::worker_server::WorkerServer;

struct Registries {
    checks: HashMap<String, serde_json::Value>,
    transforms: HashMap<String, serde_json::Value>,
    targets: HashMap<String, serde_json::Value>,
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

#[derive(Default)]
struct DispatcherSettings {
    queue: Option<QueueKind>,
    executor: Option<ExecutorKind>,
}

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BitsConfig {
    #[serde(default)]
    broker_id: Option<String>,
    #[serde(default)]
    internal_poll_base_url: Option<String>,
    #[serde(default)]
    internal_poll_timeout_ms: Option<u64>,
    #[serde(default)]
    job_cleanup_interval_ms: Option<u64>,
    #[serde(default)]
    persist_after_ms: Option<u64>,
    #[serde(default)]
    poll_timeout_ms: Option<u64>,
    #[serde(default)]
    persist_guard_ms: Option<u64>,
    #[serde(default)]
    persistence: Option<PersistenceConfig>,
    #[serde(default)]
    worker_server: Option<WorkerServerConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
enum PersistenceConfig {
    #[cfg_attr(not(feature = "tikv"), allow(dead_code))]
    Tikv {
        endpoints: Vec<String>,
        #[serde(default = "default_broker_lease_ttl_secs")]
        broker_lease_ttl_secs: f64,
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

pub(crate) struct RuntimeConfig {
    pub router: Switch,
    pub sweep_interval: Option<Duration>,
    pub broker_id: String,
    pub internal_poll_base_url: String,
    pub internal_poll_timeout: Duration,
    pub job_store: Option<Arc<dyn PersistenceStore>>,
    pub broker_lease_ttl: Duration,
    pub persist_after: Option<Duration>,
}

/// Parsed startup configuration split into broker runtime and HTTP server parts.
pub struct Bootstrap {
    runtime_config: RuntimeConfig,
    /// Configuration for the built-in HTTP server.
    pub server_config: ServerConfig,
}

impl Bootstrap {
    /// Consumes the bootstrap value and constructs a broker runtime.
    pub fn into_bits(self) -> Result<Bits, Box<dyn std::error::Error>> {
        Bits::from_runtime_config(self.runtime_config)
    }

    /// Consumes the bootstrap value and returns both the broker and server config.
    pub fn into_parts(self) -> Result<(Bits, ServerConfig), Box<dyn std::error::Error>> {
        let Bootstrap {
            runtime_config,
            server_config,
        } = self;
        let bits = Bits::from_runtime_config(runtime_config)?;
        Ok((bits, server_config))
    }
}

/// Parses the top-level YAML configuration used by the BITS binaries.
pub fn parse_bootstrap(config: &str) -> Result<Bootstrap, Box<dyn std::error::Error>> {
    let raw: serde_json::Value = serde_yaml::from_str(config)?;

    let bits_cfg: BitsConfig = raw
        .get("bits")
        .cloned()
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or(BitsConfig {
            broker_id: None,
            internal_poll_base_url: None,
            internal_poll_timeout_ms: None,
            job_cleanup_interval_ms: None,
            persist_after_ms: None,
            poll_timeout_ms: None,
            persist_guard_ms: None,
            persistence: None,
            worker_server: None,
        });

    let worker_server: Option<Arc<WorkerServer>> = bits_cfg
        .worker_server
        .as_ref()
        .map(|ws_cfg| Arc::new(WorkerServer::new(&ws_cfg.host, ws_cfg.port)));

    let broker_id = bits_cfg
        .broker_id
        .unwrap_or_else(|| format!("broker-{}", uuid::Uuid::new_v4()));
    let internal_poll_base_url = bits_cfg
        .internal_poll_base_url
        .unwrap_or_else(|| "http://127.0.0.1:8080/job".to_string());
    let internal_poll_timeout =
        Duration::from_millis(bits_cfg.internal_poll_timeout_ms.unwrap_or(2500));
    let sweep_interval = bits_cfg.job_cleanup_interval_ms.map(Duration::from_millis);

    let poll_timeout = Duration::from_millis(bits_cfg.poll_timeout_ms.unwrap_or(30_000));
    let persist_guard = Duration::from_millis(bits_cfg.persist_guard_ms.unwrap_or(1_000));
    let persist_after = bits_cfg.persist_after_ms.map(Duration::from_millis);

    if let Some(persist_after) = persist_after
        && persist_after + persist_guard >= poll_timeout
    {
        return Err(
            "bits.persist_after_ms + bits.persist_guard_ms must be less than bits.poll_timeout_ms"
                .into(),
        );
    }

    let (job_store, broker_lease_ttl) = match bits_cfg.persistence {
        Some(PersistenceConfig::Tikv {
            endpoints,
            broker_lease_ttl_secs,
        }) => {
            if endpoints.is_empty() {
                return Err("bits.persistence.endpoints must not be empty".into());
            }
            let ttl = Duration::try_from_secs_f64(broker_lease_ttl_secs).map_err(
                |e| -> Box<dyn std::error::Error> {
                    format!("bits.persistence.broker_lease_ttl_secs: {e}").into()
                },
            )?;
            if ttl < Duration::from_secs(1) {
                return Err(
                    "bits.persistence.broker_lease_ttl_secs must be at least 1 second".into(),
                );
            }
            #[cfg(not(feature = "tikv"))]
            {
                return Err(
                    "bits.persistence.type=tikv but crate built without 'tikv' feature".into(),
                );
            }
            #[cfg(feature = "tikv")]
            {
                (
                    Some(Arc::new(StoreFactory::new(endpoints)) as Arc<dyn PersistenceStore>),
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
        }) => {
            if url.is_empty() {
                return Err("bits.persistence.url must not be empty".into());
            }
            if jobs_bucket.is_empty() {
                return Err("bits.persistence.jobs_bucket must not be empty".into());
            }
            if leases_bucket.is_empty() {
                return Err("bits.persistence.leases_bucket must not be empty".into());
            }
            if num_replicas < 1 {
                return Err("bits.persistence.num_replicas must be at least 1".into());
            }
            let ttl = Duration::try_from_secs_f64(broker_lease_ttl_secs).map_err(
                |e| -> Box<dyn std::error::Error> {
                    format!("bits.persistence.broker_lease_ttl_secs: {e}").into()
                },
            )?;
            if ttl < Duration::from_secs(1) {
                return Err(
                    "bits.persistence.broker_lease_ttl_secs must be at least 1 second".into(),
                );
            }
            #[cfg(not(feature = "nats"))]
            {
                return Err(
                    "bits.persistence.type=nats but crate built without 'nats' feature".into(),
                );
            }
            #[cfg(feature = "nats")]
            {
                let store = crate::db::nats::NatsStore::new(
                    url,
                    jobs_bucket,
                    leases_bucket,
                    ttl,
                    num_replicas,
                );
                let handle = tokio::runtime::Handle::try_current().map_err(
                    |_| -> Box<dyn std::error::Error> {
                        "bits.persistence.type=nats requires a running Tokio runtime for init"
                            .into()
                    },
                )?;
                tokio::task::block_in_place(|| handle.block_on(store.init())).map_err(
                    |e| -> Box<dyn std::error::Error> {
                        format!("NATS store init failed: {e}").into()
                    },
                )?;
                (Some(Arc::new(store) as Arc<dyn PersistenceStore>), ttl)
            }
        }
        None => (
            None,
            Duration::from_secs_f64(default_broker_lease_ttl_secs()),
        ),
    };

    let server_config: ServerConfig = raw
        .get("server")
        .cloned()
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_default();

    let checks: HashMap<String, serde_json::Value> = raw
        .get("checks")
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()?
        .unwrap_or_default();
    let transforms: HashMap<String, serde_json::Value> = raw
        .get("transforms")
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()?
        .unwrap_or_default();
    let targets: HashMap<String, serde_json::Value> = raw
        .get("targets")
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()?
        .unwrap_or_default();

    let parse_ctx = ParseContext {
        registries: Registries {
            checks,
            transforms,
            targets,
        },
        resolved_targets: RefCell::new(HashMap::new()),
        worker_server: worker_server.clone(),
    };

    let branches = parse_routes(
        raw.get("routes")
            .ok_or("config must have a 'routes' section")?,
        "routes",
        &parse_ctx,
    )?;

    let router = Switch::new(branches);
    router
        .validate()
        .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })?;

    if let Some(ws) = &worker_server {
        ws.start()?;
    }

    Ok(Bootstrap {
        runtime_config: RuntimeConfig {
            router,
            sweep_interval,
            broker_id,
            internal_poll_base_url,
            internal_poll_timeout,
            job_store,
            broker_lease_ttl,
            persist_after,
        },
        server_config,
    })
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
) -> Result<Vec<Route>, Box<dyn std::error::Error>> {
    let entries = value
        .as_array()
        .ok_or_else(|| format!("{section} must be an array"))?;

    let mut routes = Vec::new();
    for entry in entries {
        let map = entry
            .as_object()
            .ok_or_else(|| format!("each {section} entry must be an object"))?;
        if map.len() != 1 {
            return Err(format!(
                "each {section} entry must have exactly one key (the route name), got {}",
                map.len()
            )
            .into());
        }
        let (name, route_val) = map.iter().next().ok_or_else(|| {
            format!("each {section} entry must have exactly one key (the route name)")
        })?;
        let action_values = route_val
            .as_array()
            .ok_or_else(|| format!("{section} route '{name}' must be an array of actions"))?;
        let actions = action_values
            .iter()
            .map(|v| parse_action(v, ctx))
            .collect::<Result<Vec<_>, _>>()?;
        routes.push(Route::new(name.clone(), actions));
    }
    Ok(routes)
}

fn parse_action(
    value: &serde_json::Value,
    ctx: &ParseContext,
) -> Result<Action, Box<dyn std::error::Error>> {
    match value {
        serde_json::Value::String(name) => {
            if name == "persist" {
                return Err(
                    "'persist' step has been removed; use bits.persist_after_ms instead".into(),
                );
            }
            let (ns, entry_name) = split_ns(name)?;
            resolve_named(ns, entry_name, ctx)
        }
        serde_json::Value::Object(map) => {
            if let Some(switch_val) = map.get("switch") {
                let routes = parse_routes(switch_val, "switch", ctx)?;
                let switch = Switch::new(routes);
                switch
                    .validate()
                    .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })?;
                return Ok(Action::Switch(switch));
            }

            for (key, config) in map {
                if key.contains("::") {
                    let (ns, action_name) = split_ns(key)?;
                    if action_name == "remote" {
                        return Err("target::remote must be defined as a named registry entry (not inline); the pool name is derived from the registry entry name".into());
                    }
                    let action = create_action(action_name, config.clone())?;
                    let action = validate_inline_action(ns, action)?;
                    let settings = parse_dispatcher_fields(map.get("dispatcher"))?;
                    let silent = map
                        .get("silent")
                        .map(|v| {
                            v.as_bool()
                                .ok_or_else(|| format!("{key}: silent must be a boolean"))
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

            Err(format!("unrecognised action: {value:?}").into())
        }
        _ => Err(format!("action must be string or object, got {value:?}").into()),
    }
}

fn split_ns(s: &str) -> Result<(&str, &str), Box<dyn std::error::Error>> {
    let mut parts = s.splitn(2, "::");
    let ns = parts
        .next()
        .ok_or_else(|| format!("invalid reference '{s}'"))?;
    let name = parts
        .next()
        .ok_or_else(|| format!("reference '{s}' must be namespace::name"))?;
    Ok((ns, name))
}

fn resolve_named(
    ns: &str,
    name: &str,
    ctx: &ParseContext,
) -> Result<Action, Box<dyn std::error::Error>> {
    match ns {
        "check" => {
            let entry = ctx
                .registries
                .checks
                .get(name)
                .ok_or_else(|| format!("unknown check '{name}'"))?;
            action_from_entry(ns, name, entry, ctx)
        }
        "transform" => {
            let entry = ctx
                .registries
                .transforms
                .get(name)
                .ok_or_else(|| format!("unknown transform '{name}'"))?;
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
            let entry = ctx
                .registries
                .targets
                .get(name)
                .ok_or_else(|| format!("unknown target '{name}'"))?;
            let action = action_from_entry(ns, name, entry, ctx)?;
            if let Action::Target(target, dispatcher, surface) = &action {
                ctx.resolved_targets.borrow_mut().insert(
                    name.to_string(),
                    (Arc::clone(target), dispatcher.clone(), *surface),
                );
            }
            Ok(action)
        }
        _ => Err(format!("unknown namespace '{ns}' in '{ns}::{name}'").into()),
    }
}

fn validate_inline_action(ns: &str, action: Action) -> Result<Action, Box<dyn std::error::Error>> {
    match (ns, &action) {
        ("check", Action::Check(..)) => Ok(action),
        ("transform", Action::Transform(..)) => Ok(action),
        ("target", Action::Target(..)) => Ok(action),
        _ => Err(format!("inline action namespace '{ns}' does not match action type").into()),
    }
}

fn parse_dispatcher_fields(
    dispatcher: Option<&serde_json::Value>,
) -> Result<DispatcherSettings, Box<dyn std::error::Error>> {
    let mut settings = DispatcherSettings::default();

    if let Some(d) = dispatcher {
        let map = d.as_object().ok_or("dispatcher must be an object")?;
        settings.queue = map
            .get("queue")
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()?;
        settings.executor = map
            .get("executor")
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()?;
        if map.contains_key("concurrency") {
            return Err(
                "dispatcher.concurrency removed; set concurrency inside the executor block instead"
                    .into(),
            );
        }
        if map.contains_key("persistent") || map.contains_key("lock_ttl_secs") {
            return Err(
                "dispatcher.persistent/lock_ttl_secs removed; use bits.persist_after_ms policy"
                    .into(),
            );
        }
    }

    Ok(settings)
}

fn action_from_entry(
    ns: &str,
    entry_name: &str,
    entry: &serde_json::Value,
    ctx: &ParseContext,
) -> Result<Action, Box<dyn std::error::Error>> {
    let map = entry
        .as_object()
        .ok_or_else(|| format!("{ns} '{entry_name}' must be an object, got: {entry}"))?;
    if map.contains_key("persistent") || map.contains_key("lock_ttl_secs") {
        return Err(format!(
            "{ns} '{entry_name}': persistent/lock_ttl_secs removed; use bits.persist_after_ms"
        )
        .into());
    }
    let type_name = map
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("{ns} '{entry_name}' is missing a 'type' field (got: {entry})"))?;
    let settings = parse_dispatcher_fields(map.get("dispatcher"))?;
    let silent = map
        .get("silent")
        .map(|v| {
            v.as_bool()
                .ok_or_else(|| format!("{ns} '{entry_name}': silent must be a boolean"))
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
    let action = create_action(type_name, config)?;
    attach_dispatcher(entry_name, type_name, action, settings, silent, ctx)
}

fn attach_dispatcher(
    entry_name: &str,
    action_name: &str,
    action: Action,
    mut settings: DispatcherSettings,
    silent: Option<bool>,
    ctx: &ParseContext,
) -> Result<Action, Box<dyn std::error::Error>> {
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
            Some(_) => return Err("'remote' target requires executor: remote_pool".into()),
        }
        if ctx.worker_server.is_none() {
            return Err("remote_pool executor requires bits.worker_server to be configured (add 'worker_server: { host: ..., port: ... }' under 'bits:')".into());
        }
    } else if is_remote_pool {
        return Err("executor: remote_pool requires a 'remote' target action".into());
    }

    let has_dispatcher = settings.queue.is_some() || settings.executor.is_some();

    match action {
        Action::Check(check, _, _) => {
            let dispatcher = Dispatcher::from_config(
                settings.queue.as_ref(),
                settings.executor.as_ref(),
                None,
                None,
            )?;
            Ok(Action::Check(check, dispatcher, silent))
        }
        Action::Transform(transform, _, _) => {
            let dispatcher = Dispatcher::from_config(
                settings.queue.as_ref(),
                settings.executor.as_ref(),
                None,
                None,
            )?;
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
            )?;
            Ok(Action::Target(target, dispatcher, silent))
        }
        _ if has_dispatcher => {
            Err("dispatcher config only valid for Check, Transform, and Target actions".into())
        }
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

#[cfg(test)]
mod tests {
    use crate::Bits;

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
            err.to_string()
                .contains("route 'test_pipeline' must not be empty"),
            "unexpected error: {err}"
        );
    }
}
