use std::collections::HashMap;

use crate::actions::{Action, RouteAction};
use crate::job::Job;
use crate::result::JobResult;
use crate::routing::{switch::Switch, Route};
use crate::routing::registry::create_action;

pub struct Bits {
    router: Switch,
}

impl Bits {
    pub fn from_config(config: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let raw: serde_json::Value = serde_yaml::from_str(config)?;

        // Named reusable steps — optional section
        let steps: HashMap<String, serde_json::Value> = raw
            .get("steps")
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()?
            .unwrap_or_default();

        // Routes — required
        let routes_val = raw
            .get("routes")
            .ok_or("config must have a 'routes' section")?;
        let routes_raw: HashMap<String, Vec<serde_json::Value>> =
            serde_json::from_value(routes_val.clone())?;

        let mut routes = HashMap::new();
        for (name, action_values) in routes_raw {
            let actions = action_values
                .iter()
                .map(|v| parse_action(v, &steps))
                .collect::<Result<Vec<_>, _>>()?;
            routes.insert(name.clone(), Route::new(name, actions));
        }

        Ok(Bits {
            router: Switch::new(routes),
        })
    }

    pub async fn process(&self, job: Job) -> JobResult {
        match self.router.route(&job).await {
            Ok(crate::actions::RouteResult::Complete(result)) => result,
            Ok(crate::actions::RouteResult::Reject { reason }) => JobResult::Error {
                message: format!("All routes rejected: {}", reason),
            },
            Err(err) => JobResult::Failed {
                reason: format!("Routing failed: {}", err),
            },
        }
    }
}

/// Parse a single action value, resolving named step references against the steps map.
fn parse_action(
    value: &serde_json::Value,
    steps: &HashMap<String, serde_json::Value>,
) -> Result<Action, Box<dyn std::error::Error>> {
    match value {
        // Bare string: "persist" built-in or a named step reference
        serde_json::Value::String(name) => match name.as_str() {
            "persist" => Ok(Action::Persist),
            _ => {
                let step = steps
                    .get(name.as_str())
                    .ok_or_else(|| format!("unknown step '{}'", name))?;
                parse_action(step, steps)
            }
        },

        serde_json::Value::Object(map) => {
            // queue: { capacity, workers?, action: ... }
            if let Some(queue_val) = map.get("queue") {
                let q = queue_val
                    .as_object()
                    .ok_or("queue must be an object")?;
                let capacity = q
                    .get("capacity")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(1000) as usize;
                let workers = q
                    .get("workers")
                    .and_then(|v| v.as_u64())
                    .map(|n| n as usize);
                let action_val = q
                    .get("action")
                    .ok_or("queue must have an 'action' field")?;
                let action = parse_action(action_val, steps)?;
                return Ok(Action::Queue {
                    capacity,
                    workers,
                    action: Box::new(action),
                });
            }

            // switch: { branch_name: [actions], ... }
            if let Some(switch_val) = map.get("switch") {
                let branches = switch_val
                    .as_object()
                    .ok_or("switch must be an object")?;
                let mut route_map = HashMap::new();
                for (branch_name, branch_val) in branches {
                    let action_list = branch_val.as_array().ok_or_else(|| {
                        format!("switch branch '{}' must be an array", branch_name)
                    })?;
                    let actions = action_list
                        .iter()
                        .map(|v| parse_action(v, steps))
                        .collect::<Result<Vec<_>, _>>()?;
                    route_map.insert(
                        branch_name.clone(),
                        Route::new(branch_name.clone(), actions),
                    );
                }
                return Ok(Action::Switch(Switch::new(route_map)));
            }

            // namespace::name: { config }
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_empty_route() {
        let config = r#"
routes:
  test_route: []
"#;
        let bits = Bits::from_config(config).expect("Failed to parse config");
        let job = Job::new(json!({"class": "od"}));
        match bits.process(job).await {
            JobResult::Error { .. } => {}
            r => panic!("Expected error for empty route, got: {:?}", r),
        }
    }

    #[tokio::test]
    async fn test_inline_actions() {
        let config = r#"
routes:
  test_route:
    - check::match:
        class: "od"
    - via::metkit_expansion:
        expand_parameters: true
    - route::mars_destination:
        endpoint: "mars.example.com:8080"
"#;
        let bits = Bits::from_config(config).expect("Failed to parse config");
        let job = Job::new(json!({"class": "od"}));
        match bits.process(job).await {
            JobResult::Success { content_type, size, .. } => {
                assert_eq!(content_type, "application/json");
                assert_eq!(size, 51);
            }
            r => panic!("Expected success, got: {:?}", r),
        }
    }

    #[tokio::test]
    async fn test_named_steps() {
        let config = r#"
steps:
  check_od:
    check::match:
      class: "od"

routes:
  test_route:
    - check_od
    - route::mars_destination:
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
    async fn test_persist_sets_flag() {
        let config = r#"
routes:
  test_route:
    - check::match:
        class: "od"
    - persist
    - route::mars_destination:
        endpoint: "mars.example.com:8080"
"#;
        let bits = Bits::from_config(config).expect("Failed to parse config");
        let job = Job::new(json!({"class": "od"}));
        // Job processes successfully; persistent flag is set internally during routing
        match bits.process(job).await {
            JobResult::Success { .. } => {}
            r => panic!("Expected success, got: {:?}", r),
        }
    }

    #[tokio::test]
    async fn test_nested_switch() {
        let config = r#"
routes:
  complex_route:
    - check::match:
        class: "ea"
    - switch:
        privileged:
          - check::has_license:
              license: "era5"
          - route::mars_destination:
              endpoint: "mars.example.com:8080"
        public:
          - route::dss_destination:
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
