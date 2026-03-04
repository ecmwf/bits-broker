use std::collections::HashMap;

use crate::actions::{Action, TargetAction, TargetResult};
use crate::job::Job;
use crate::result::JobResult;
use crate::routing::{switch::Switch, Pipeline};
use crate::routing::registry::create_action;

pub struct Bits {
    router: Switch,
}

impl Bits {
    pub fn from_config(config: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let raw: serde_json::Value = serde_yaml::from_str(config)?;

        // Named typed registries — optional sections
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

        // Pipelines — required
        let pipelines_val = raw
            .get("pipelines")
            .ok_or("config must have a 'pipelines' section")?;
        let pipelines_raw: HashMap<String, Vec<serde_json::Value>> =
            serde_json::from_value(pipelines_val.clone())?;

        let mut branches = HashMap::new();
        for (name, action_values) in pipelines_raw {
            let actions = action_values
                .iter()
                .map(|v| parse_action(v, &registries))
                .collect::<Result<Vec<_>, _>>()?;
            branches.insert(name.clone(), Pipeline::new(name, actions));
        }

        Ok(Bits {
            router: Switch::new(branches),
        })
    }

    pub async fn process(&self, job: Job) -> JobResult {
        match self.router.dispatch(&job).await {
            Ok(TargetResult::Complete(result)) => result,
            Ok(TargetResult::Reject { reason }) => JobResult::Error {
                message: format!("All pipelines rejected: {}", reason),
            },
            Err(err) => JobResult::Failed {
                reason: format!("Dispatch failed: {}", err),
            },
        }
    }
}

struct Registries {
    checks: HashMap<String, serde_json::Value>,
    transforms: HashMap<String, serde_json::Value>,
    targets: HashMap<String, serde_json::Value>,
}

/// Parse a single action value, resolving named registry references.
fn parse_action(
    value: &serde_json::Value,
    reg: &Registries,
) -> Result<Action, Box<dyn std::error::Error>> {
    match value {
        // Bare string: "persist" built-in or a namespaced registry reference
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
                        Pipeline::new(route_name.clone(), actions),
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
/// Entry format: { type: action_type_name, ...config fields }
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
/// Entry format: { type: action_type_name, ...config, queue?: { type, capacity, workers? } }
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
        Ok(Action::Queue {
            capacity,
            workers,
            action: Box::new(action),
        })
    } else {
        Ok(action)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_empty_pipeline() {
        let config = r#"
pipelines:
  test_pipeline: []
"#;
        let bits = Bits::from_config(config).expect("Failed to parse config");
        let job = Job::new(json!({"class": "od"}));
        match bits.process(job).await {
            JobResult::Error { .. } => {}
            r => panic!("Expected error for empty pipeline, got: {:?}", r),
        }
    }

    #[tokio::test]
    async fn test_inline_actions() {
        let config = r#"
pipelines:
  test_pipeline:
    - check::match:
        class: "od"
    - transform::metkit_expansion:
        expand_parameters: true
    - target::mars_destination:
        endpoint: "mars.example.com:8080"
"#;
        let bits = Bits::from_config(config).expect("Failed to parse config");
        let job = Job::new(json!({"class": "od"}));
        match bits.process(job).await {
            JobResult::Success { content_type, size, .. } => {
                assert_eq!(content_type, "application/json");
                assert_eq!(size, 55);
            }
            r => panic!("Expected success, got: {:?}", r),
        }
    }

    #[tokio::test]
    async fn test_named_registries() {
        let config = r#"
checks:
  is_od:
    type: match
    class: "od"

targets:
  mars:
    type: mars_destination
    endpoint: "mars.example.com:8080"

pipelines:
  test_pipeline:
    - check::is_od
    - target::mars
"#;
        let bits = Bits::from_config(config).expect("Failed to parse config");
        let job = Job::new(json!({"class": "od"}));
        match bits.process(job).await {
            JobResult::Success { .. } => {}
            r => panic!("Expected success, got: {:?}", r),
        }
    }

    #[tokio::test]
    async fn test_persist_sets_flag() {
        let config = r#"
pipelines:
  test_pipeline:
    - check::match:
        class: "od"
    - persist
    - target::mars_destination:
        endpoint: "mars.example.com:8080"
"#;
        let bits = Bits::from_config(config).expect("Failed to parse config");
        let job = Job::new(json!({"class": "od"}));
        match bits.process(job).await {
            JobResult::Success { .. } => {}
            r => panic!("Expected success, got: {:?}", r),
        }
    }

    #[tokio::test]
    async fn test_nested_switch() {
        let config = r#"
pipelines:
  complex_pipeline:
    - check::match:
        class: "ea"
    - switch:
        privileged:
          - check::has_license:
              license: "era5"
          - target::mars_destination:
              endpoint: "mars.example.com:8080"
        public:
          - target::dss_destination:
              endpoint: "dss.example.com:9090"
"#;
        let bits = Bits::from_config(config).expect("Failed to parse config");
        let job = Job::new(json!({"class": "ea"}));
        match bits.process(job).await {
            JobResult::Success { content_type, .. } => {
                assert_eq!(content_type, "application/json");
            }
            r => panic!("Expected success, got: {:?}", r),
        }
    }
}
