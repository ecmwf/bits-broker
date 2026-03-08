use std::any::TypeId;
use std::collections::HashMap;
use std::time::Duration;

use crate::actions::{Action, target_remote::RemoteTarget};
use crate::dispatcher::{Dispatcher, ExecutorKind, QueueKind};
use crate::routing::{switch::Switch, Route};
use crate::routing::registry::create_action;

struct Registries {
    pub checks: HashMap<String, serde_json::Value>,
    pub transforms: HashMap<String, serde_json::Value>,
    pub targets: HashMap<String, serde_json::Value>,
}

// ================================
//   ParsedConfig
// ================================

pub(crate) struct ParsedConfig {
    pub router: Switch,
    pub sweep_interval: Option<Duration>,
}

// ================================
//   parse_config
// ================================

pub(crate) fn parse_config(config: &str) -> Result<ParsedConfig, Box<dyn std::error::Error>> {
    let raw: serde_json::Value = serde_yaml::from_str(config)?;

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

    let registries = Registries { checks, transforms, targets };

    let sweep_interval = match raw.get("bits").and_then(|v| v.get("job_cleanup_interval_ms")) {
        None => None,
        Some(value) => Some(Duration::from_millis(
            value
                .as_u64()
                .ok_or("bits.job_cleanup_interval_ms must be a number")?,
        )),
    };

    let pipelines_val = raw
        .get("routes")
        .ok_or("config must have a 'routes' section")?;
    let pipelines_obj = pipelines_val
        .as_object()
        .ok_or("routes must be an object")?;

    let mut branches = Vec::new();
    for (name, route_val) in pipelines_obj {
        let action_values = route_val
            .as_array()
            .ok_or_else(|| format!("route '{}' must be an array", name))?;
        let actions = action_values
            .iter()
            .map(|v| parse_action(v, &registries))
            .collect::<Result<Vec<_>, _>>()?;
        branches.push(Route::new(name.clone(), actions));
    }

    let router = Switch::new(branches);

    Ok(ParsedConfig { router, sweep_interval })
}

/// Parse a single action value, resolving named registry references.
fn parse_action(
    value: &serde_json::Value,
    reg: &Registries,
) -> Result<Action, Box<dyn std::error::Error>> {
    match value {
        serde_json::Value::String(name) => match name.as_str() {
            "persist" => Ok(Action::Persist),
            _ => {
                let (ns, entry_name) = split_ns(name)?;
                resolve_named(ns, entry_name, reg)
            }
        },

        serde_json::Value::Object(map) => {
            // switch: { branch_name: [actions], ... }
            if let Some(switch_val) = map.get("switch") {
                let branches = switch_val
                    .as_object()
                    .ok_or("switch must be an object")?;
                let mut routes = Vec::new();
                for (route_name, route_val) in branches {
                    let action_list = route_val.as_array().ok_or_else(|| {
                        format!("switch route '{}' must be an array", route_name)
                    })?;
                    let actions = action_list
                        .iter()
                        .map(|v| parse_action(v, reg))
                        .collect::<Result<Vec<_>, _>>()?;
                    routes.push(Route::new(route_name.clone(), actions));
                }
                return Ok(Action::Switch(Switch::new(routes)));
            }

            // namespace::name: { config } — inline action, bypasses named registries.
            // An optional sibling key `dispatcher: { queue, executor, concurrency }`
            // attaches a dispatcher to the action.
            for (key, config) in map {
                if key.contains("::") {
                    let (ns, action_name) = split_ns(key)?;
                    let action = create_action(action_name, config.clone())?;
                    let action = validate_inline_action(ns, action)?;

                    let (queue, executor, concurrency) =
                        parse_dispatcher_fields(map.get("dispatcher"))?;

                    return attach_dispatcher(action_name, action, queue, executor, concurrency);
                }
            }

            Err(format!("unrecognised action: {:?}", value).into())
        }

        _ => Err(format!("action must be a string or object, got {:?}", value).into()),
    }
}

/// Split "namespace::name" into ("namespace", "name").
fn split_ns<'a>(s: &'a str) -> Result<(&'a str, &'a str), Box<dyn std::error::Error>> {
    let mut parts = s.splitn(2, "::");
    let ns = parts.next().ok_or_else(|| format!("invalid reference '{}'", s))?;
    let name = parts.next().ok_or_else(|| format!("reference '{}' must be namespace::name", s))?;
    Ok((ns, name))
}

/// Resolve a named registry entry into an Action.
fn resolve_named(
    ns: &str,
    name: &str,
    reg: &Registries,
) -> Result<Action, Box<dyn std::error::Error>> {
    match ns {
        "check" => {
            let entry = reg.checks.get(name)
                .ok_or_else(|| format!("unknown check '{}'", name))?;
            action_from_entry(entry)
        }
        "transform" => {
            let entry = reg.transforms.get(name)
                .ok_or_else(|| format!("unknown transform '{}'", name))?;
            action_from_entry(entry)
        }
        "target" => {
            let entry = reg.targets.get(name)
                .ok_or_else(|| format!("unknown target '{}'", name))?;
            action_from_entry(entry)
        }
        _ => Err(format!("unknown namespace '{}' in '{}::{}'", ns, ns, name).into()),
    }
}

/// Ensure inline namespace matches the resolved action type.
fn validate_inline_action(
    ns: &str,
    action: Action,
) -> Result<Action, Box<dyn std::error::Error>> {
    match (ns, &action) {
        ("check", Action::Check(..)) => Ok(action),
        ("transform", Action::Transform(..)) => Ok(action),
        ("target", Action::Target(..)) => Ok(action),
        _ => Err(format!("inline action namespace '{}' does not match action type", ns).into()),
    }
}

/// Extract `queue`, `executor`, and `concurrency` from an optional `dispatcher` object.
fn parse_dispatcher_fields(
    dispatcher: Option<&serde_json::Value>,
) -> Result<(Option<QueueKind>, Option<ExecutorKind>, Option<usize>), Box<dyn std::error::Error>> {
    let Some(d) = dispatcher else {
        return Ok((None, None, None));
    };
    let map = d.as_object().ok_or("dispatcher must be an object")?;
    let queue = map.get("queue")
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()?;
    let executor = map.get("executor")
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()?;
    let concurrency = map.get("concurrency")
        .and_then(|v| v.as_u64())
        .map(|n| n as usize);
    Ok((queue, executor, concurrency))
}

/// Build an Action from a named registry entry.
///
/// The entry may include a `dispatcher` object with `queue`, `executor`, and
/// `concurrency` fields that are used to build a dispatcher.
fn action_from_entry(entry: &serde_json::Value) -> Result<Action, Box<dyn std::error::Error>> {
    let map = entry.as_object().ok_or("registry entry must be an object")?;
    let type_name = map.get("type")
        .and_then(|v| v.as_str())
        .ok_or("registry entry must have a 'type' field")?;

    let (queue, executor, concurrency) = parse_dispatcher_fields(map.get("dispatcher"))?;

    let config: serde_json::Value = map.iter()
        .filter(|(k, _)| !matches!(k.as_str(), "type" | "dispatcher"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<serde_json::Map<_, _>>()
        .into();

    let action = create_action(type_name, config)?;
    attach_dispatcher(type_name, action, queue, executor, concurrency)
}

/// Attach a dispatcher to a target action, validating `remote` ↔ `remote_pool` pairing.
///
/// Rules:
/// - `remote` action always uses `remote_pool`; any other executor is an error.
///   If no executor is specified for a `remote` action, `remote_pool` is auto-selected.
/// - `remote_pool` executor requires a `remote` action; using it with any other
///   action type is an error.
/// - For non-target actions, dispatcher config is invalid.
fn attach_dispatcher(
    action_name: &str,
    action: Action,
    queue: Option<QueueKind>,
    mut executor: Option<ExecutorKind>,
    concurrency: Option<usize>,
) -> Result<Action, Box<dyn std::error::Error>> {
    let is_remote_action = action_name == "remote";
    let is_remote_pool = matches!(&executor, Some(ExecutorKind::RemotePool(_)));

    if is_remote_action {
        match &executor {
            None => executor = Some(ExecutorKind::RemotePool(crate::dispatcher::RemotePoolConfig {
                bind: "0.0.0.0:9001".into(),
                heartbeat_timeout_secs: 60.0,
            })),
            Some(ExecutorKind::RemotePool(_)) => {}
            Some(_) => return Err("'remote' target requires executor: remote_pool".into()),
        }
    } else if is_remote_pool {
        return Err("executor: remote_pool requires a 'remote' target action".into());
    }

    let has_dispatcher_config = queue.is_some() || executor.is_some() || concurrency.is_some();

    // Capture the concrete action type so executors can guard against misconfiguration.
    let action_type_id = if action_name == "remote" {
        TypeId::of::<RemoteTarget>()
    } else {
        TypeId::of::<()>()
    };

    match action {
        Action::Check(check, _) => {
            let dispatcher = Dispatcher::from_config(queue.as_ref(), executor.as_ref(), concurrency, action_type_id);
            Ok(Action::Check(check, dispatcher))
        }
        Action::Transform(transform, _) => {
            let dispatcher = Dispatcher::from_config(queue.as_ref(), executor.as_ref(), concurrency, action_type_id);
            Ok(Action::Transform(transform, dispatcher))
        }
        Action::Target(target, _) => {
            let dispatcher = Dispatcher::from_config(queue.as_ref(), executor.as_ref(), concurrency, action_type_id);
            Ok(Action::Target(target, dispatcher))
        }
        _ if has_dispatcher_config => {
            Err("queue/executor/concurrency are only valid for Check, Transform, and Target actions".into())
        }
        _ => Ok(action),
    }
}
