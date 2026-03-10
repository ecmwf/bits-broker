use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use crate::actions::{target_remote::RemoteTarget, Action};
use crate::db::PersistenceStore;
use crate::dispatcher::{Dispatcher, ExecutorKind, QueueKind, RemotePoolConfig};
use crate::routing::registry::create_action;
use crate::routing::{switch::Switch, Route};

struct Registries {
    checks: HashMap<String, serde_json::Value>,
    transforms: HashMap<String, serde_json::Value>,
    targets: HashMap<String, serde_json::Value>,
}

#[derive(Clone)]
struct ParseContext {
    registries: Registries,
}

#[derive(Default)]
struct DispatcherSettings {
    queue: Option<QueueKind>,
    executor: Option<ExecutorKind>,
    concurrency: Option<usize>,
}

#[derive(Debug, Deserialize)]
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
    tikv: Option<TiKvConfig>,
}

#[cfg_attr(not(feature = "tikv"), allow(dead_code))]
#[derive(Debug, Deserialize)]
struct TiKvConfig {
    endpoints: Vec<String>,
    #[serde(default = "default_broker_lease_ttl_secs")]
    broker_lease_ttl_secs: f64,
}

#[cfg(feature = "tikv")]
type StoreFactory = crate::db::tikv::TiKvStore;

fn default_broker_lease_ttl_secs() -> f64 {
    30.0
}

pub(crate) struct ParsedConfig {
    pub router: Switch,
    pub sweep_interval: Option<Duration>,
    pub broker_id: String,
    pub internal_poll_base_url: String,
    pub internal_poll_timeout: Duration,
    pub job_store: Option<Arc<dyn PersistenceStore>>,
    pub broker_lease_ttl: Duration,
    pub persist_after: Option<Duration>,
}

pub(crate) fn parse_config(config: &str) -> Result<ParsedConfig, Box<dyn std::error::Error>> {
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
            tikv: None,
        });

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

    if let Some(persist_after) = persist_after {
        if persist_after + persist_guard >= poll_timeout {
            return Err(
                "bits.persist_after_ms + bits.persist_guard_ms must be less than bits.poll_timeout_ms"
                    .into(),
            );
        }
    }

    let (job_store, broker_lease_ttl) = match bits_cfg.tikv {
        Some(tikv) => {
            if tikv.endpoints.is_empty() {
                return Err("bits.tikv.endpoints must not be empty".into());
            }
            #[cfg(not(feature = "tikv"))]
            {
                return Err("bits.tikv configured but crate built without 'tikv' feature".into());
            }
            #[cfg(feature = "tikv")]
            {
                (
                    Some(Arc::new(StoreFactory::new(tikv.endpoints)) as Arc<dyn PersistenceStore>),
                    Duration::from_secs_f64(tikv.broker_lease_ttl_secs),
                )
            }
        }
        None => (
            None,
            Duration::from_secs_f64(default_broker_lease_ttl_secs()),
        ),
    };

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
    };

    let routes = raw
        .get("routes")
        .ok_or("config must have a 'routes' section")?
        .as_object()
        .ok_or("routes must be an object")?;

    let mut branches = Vec::new();
    for (name, route_val) in routes {
        let action_values = route_val
            .as_array()
            .ok_or_else(|| format!("route '{name}' must be an array"))?;
        let actions = action_values
            .iter()
            .map(|v| parse_action(v, &parse_ctx))
            .collect::<Result<Vec<_>, _>>()?;
        branches.push(Route::new(name.clone(), actions));
    }

    Ok(ParsedConfig {
        router: Switch::new(branches),
        sweep_interval,
        broker_id,
        internal_poll_base_url,
        internal_poll_timeout,
        job_store,
        broker_lease_ttl,
        persist_after,
    })
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
                let branches = switch_val.as_object().ok_or("switch must be an object")?;
                let mut routes = Vec::new();
                for (route_name, route_val) in branches {
                    let action_list = route_val
                        .as_array()
                        .ok_or_else(|| format!("switch route '{route_name}' must be an array"))?;
                    let actions = action_list
                        .iter()
                        .map(|v| parse_action(v, ctx))
                        .collect::<Result<Vec<_>, _>>()?;
                    routes.push(Route::new(route_name.clone(), actions));
                }
                return Ok(Action::Switch(Switch::new(routes)));
            }

            for (key, config) in map {
                if key.contains("::") {
                    let (ns, action_name) = split_ns(key)?;
                    let action = create_action(action_name, config.clone())?;
                    let action = validate_inline_action(ns, action)?;
                    let settings = parse_dispatcher_fields(map.get("dispatcher"))?;
                    return attach_dispatcher(action_name, action, settings);
                }
            }

            Err(format!("unrecognised action: {value:?}").into())
        }
        _ => Err(format!("action must be string or object, got {value:?}").into()),
    }
}

fn split_ns<'a>(s: &'a str) -> Result<(&'a str, &'a str), Box<dyn std::error::Error>> {
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
            action_from_entry(entry)
        }
        "transform" => {
            let entry = ctx
                .registries
                .transforms
                .get(name)
                .ok_or_else(|| format!("unknown transform '{name}'"))?;
            action_from_entry(entry)
        }
        "target" => {
            let entry = ctx
                .registries
                .targets
                .get(name)
                .ok_or_else(|| format!("unknown target '{name}'"))?;
            action_from_entry(entry)
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
        settings.concurrency = map
            .get("concurrency")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize);
        if map.contains_key("persistent") || map.contains_key("lock_ttl_secs") {
            return Err(
                "dispatcher.persistent/lock_ttl_secs removed; use bits.persist_after_ms policy"
                    .into(),
            );
        }
    }

    Ok(settings)
}

fn action_from_entry(entry: &serde_json::Value) -> Result<Action, Box<dyn std::error::Error>> {
    let map = entry
        .as_object()
        .ok_or("registry entry must be an object")?;
    if map.contains_key("persistent") || map.contains_key("lock_ttl_secs") {
        return Err(
            "registry-level persistent/lock_ttl_secs removed; use bits.persist_after_ms".into(),
        );
    }
    let type_name = map
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or("registry entry must have a 'type' field")?;
    let settings = parse_dispatcher_fields(map.get("dispatcher"))?;

    let config: serde_json::Value = map
        .iter()
        .filter(|(k, _)| !matches!(k.as_str(), "type" | "dispatcher"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<serde_json::Map<_, _>>()
        .into();
    let action = create_action(type_name, config)?;
    attach_dispatcher(type_name, action, settings)
}

fn attach_dispatcher(
    action_name: &str,
    action: Action,
    mut settings: DispatcherSettings,
) -> Result<Action, Box<dyn std::error::Error>> {
    let is_remote_action = action_name == "remote";
    let is_remote_pool = matches!(&settings.executor, Some(ExecutorKind::RemotePool(_)));

    if is_remote_action {
        match &settings.executor {
            None => {
                settings.executor = Some(ExecutorKind::RemotePool(RemotePoolConfig {
                    bind: "0.0.0.0:9001".into(),
                    heartbeat_timeout_secs: 60.0,
                }))
            }
            Some(ExecutorKind::RemotePool(_)) => {}
            Some(_) => return Err("'remote' target requires executor: remote_pool".into()),
        }
    } else if is_remote_pool {
        return Err("executor: remote_pool requires a 'remote' target action".into());
    }

    let has_dispatcher =
        settings.queue.is_some() || settings.executor.is_some() || settings.concurrency.is_some();
    let action_type_id = if action_name == "remote" {
        TypeId::of::<RemoteTarget>()
    } else {
        TypeId::of::<()>()
    };

    match action {
        Action::Check(check, _) => {
            let dispatcher = Dispatcher::from_config(
                settings.queue.as_ref(),
                settings.executor.as_ref(),
                settings.concurrency,
                action_type_id,
            );
            Ok(Action::Check(check, dispatcher))
        }
        Action::Transform(transform, _) => {
            let dispatcher = Dispatcher::from_config(
                settings.queue.as_ref(),
                settings.executor.as_ref(),
                settings.concurrency,
                action_type_id,
            );
            Ok(Action::Transform(transform, dispatcher))
        }
        Action::Target(target, _) => {
            let dispatcher = Dispatcher::from_config(
                settings.queue.as_ref(),
                settings.executor.as_ref(),
                settings.concurrency,
                action_type_id,
            );
            Ok(Action::Target(target, dispatcher))
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
