use std::collections::HashMap;

use crate::actions::Action;
use crate::queue::Queue;
use crate::routing::{switch::Switch, Route};
use crate::routing::registry::create_action;

struct Registries {
    pub checks: HashMap<String, serde_json::Value>,
    pub transforms: HashMap<String, serde_json::Value>,
    pub targets: HashMap<String, serde_json::Value>,
}

pub(crate) fn parse_config(config: &str) -> Result<Switch, Box<dyn std::error::Error>> {
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

    let pipelines_val = raw
        .get("routes")
        .ok_or("config must have a 'routes' section")?;
    let pipelines_raw: HashMap<String, Vec<serde_json::Value>> =
        serde_json::from_value(pipelines_val.clone())?;

    let mut branches = HashMap::new();
    for (name, action_values) in pipelines_raw {
        let actions = action_values
            .iter()
            .map(|v| parse_action(v, &registries))
            .collect::<Result<Vec<_>, _>>()?;
        branches.insert(name.clone(), Route::new(name, actions));
    }

    Ok(Switch::new(branches))
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
                let mut route_map = HashMap::new();
                for (route_name, route_val) in branches {
                    let action_list = route_val.as_array().ok_or_else(|| {
                        format!("switch route '{}' must be an array", route_name)
                    })?;
                    let actions = action_list
                        .iter()
                        .map(|v| parse_action(v, reg))
                        .collect::<Result<Vec<_>, _>>()?;
                    route_map.insert(
                        route_name.clone(),
                        Route::new(route_name.clone(), actions),
                    );
                }
                return Ok(Action::Switch(Switch::new(route_map)));
            }

            // namespace::name: { config } — inline action, bypasses named registries
            for (key, config) in map {
                if key.contains("::") {
                    let action_name = key
                        .split("::")
                        .nth(1)
                        .ok_or_else(|| format!("invalid action key '{}'", key))?;
                    return create_action(action_name, config.clone()).map_err(Into::into);
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
            target_from_entry(entry)
        }
        _ => Err(format!("unknown namespace '{}' in '{}::{}'", ns, ns, name).into()),
    }
}

/// Build a Check or Transform Action from a named registry entry.
fn action_from_entry(entry: &serde_json::Value) -> Result<Action, Box<dyn std::error::Error>> {
    let map = entry.as_object().ok_or("registry entry must be an object")?;
    let type_name = map.get("type")
        .and_then(|v| v.as_str())
        .ok_or("registry entry must have a 'type' field")?;
    let config: serde_json::Value = map.iter()
        .filter(|(k, _)| k.as_str() != "type")
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<serde_json::Map<_, _>>()
        .into();
    create_action(type_name, config).map_err(Into::into)
}

/// Build a Target Action (with optional queue) from a named registry entry.
fn target_from_entry(entry: &serde_json::Value) -> Result<Action, Box<dyn std::error::Error>> {
    let map = entry.as_object().ok_or("target entry must be an object")?;
    let type_name = map.get("type")
        .and_then(|v| v.as_str())
        .ok_or("target entry must have a 'type' field")?;
    let queue_val = map.get("queue").cloned();
    let config: serde_json::Value = map.iter()
        .filter(|(k, _)| k.as_str() != "type" && k.as_str() != "queue")
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<serde_json::Map<_, _>>()
        .into();
    let action = create_action(type_name, config)?;

    if let Some(q) = queue_val {
        let q = q.as_object().ok_or("queue must be an object")?;
        let capacity = q.get("capacity").and_then(|v| v.as_u64()).unwrap_or(1000) as usize;
        let workers = q.get("workers").and_then(|v| v.as_u64()).map(|n| n as usize);
        Ok(Action::Queue(Queue::new(capacity, workers, action)))
    } else {
        Ok(action)
    }
}
