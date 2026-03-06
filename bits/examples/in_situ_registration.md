# In-Situ Action Registration

The `bits` library supports in-situ action registration, allowing actions to be defined and registered anywhere in the codebase. This design enables:

1. **Modular action definitions** - Actions can be defined in separate modules, crates, or plugins
2. **Automatic discovery** - Actions are automatically discovered at compile time using the `inventory` crate
3. **No central registry** - No need to maintain a central list of actions

## How It Works

Actions are registered using the `register_action!` macro immediately after their definition:

```rust
use bits::*;

// Define your action
#[derive(Debug, Serialize, Deserialize)]
pub struct MyCustomAction {
    pub config_field: String,
}

#[async_trait]
impl CheckAction for MyCustomAction {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        // Your action logic here
        Ok(CheckResult::Pass)
    }
}

// Register the action in-situ
register_action!(check, "my_custom_action", MyCustomAction);
```

## Registration Macro

The `register_action!` macro supports three action types:

- `register_action!(check, "action_name", ActionType);` - For check actions
- `register_action!(via, "action_name", ActionType);` - For via actions  
- `register_action!(route, "action_name", ActionType);` - For route actions

## Examples

### Check Action
```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct ValidateField {
    pub field_name: String,
    pub expected_value: String,
}

#[async_trait]
impl CheckAction for ValidateField {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        if let Some(value) = job.request.get(&self.field_name) {
            if value.as_str() == Some(&self.expected_value) {
                Ok(CheckResult::Pass)
            } else {
                Ok(CheckResult::Reject {
                    reason: format!("Field {} has wrong value", self.field_name)
                })
            }
        } else {
            Ok(CheckResult::Reject {
                reason: format!("Field {} not found", self.field_name)
            })
        }
    }
}

register_action!(check, "validate_field", ValidateField);
```

### Via Action
```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct AddMetadata {
    pub key: String,
    pub value: String,
}

#[async_trait]
impl ViaAction for AddMetadata {
    async fn execute(&self, job: &mut Job) -> Result<ViaResult, ActionError> {
        let mut metadata = job.metadata.as_object().unwrap_or(&serde_json::Map::new()).clone();
        metadata.insert(self.key.clone(), serde_json::json!(self.value));
        job.metadata = serde_json::Value::Object(metadata);
        Ok(ViaResult::Continue)
    }
}

register_action!(via, "add_metadata", AddMetadata);
```

### Route Action
```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct HttpEndpoint {
    pub url: String,
}

#[async_trait]
impl RouteAction for HttpEndpoint {
    async fn route(&self, job: &Job) -> Result<RouteResult, ActionError> {
        let response = format!("HTTP response from {}", self.url);
        let data_bytes = bytes::Bytes::from(response.into_bytes());
        let size = data_bytes.len() as i64;
        let stream: Box<dyn futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + Unpin> = 
            Box::new(futures::stream::iter(vec![Ok(data_bytes)]));
        
        Ok(RouteResult::Complete(JobResult::Success {
            content_type: "application/json".to_string(),
            size,
            stream,
        }))
    }
}

register_action!(route, "http_endpoint", HttpEndpoint);
```

## Benefits

1. **Flexibility** - Actions can be defined anywhere in your codebase
2. **Modularity** - Actions can be organized into separate modules or crates
3. **Plugin Support** - Third-party crates can define their own actions
4. **Compile-time Discovery** - All actions are discovered at compile time
5. **No Boilerplate** - No need to maintain central registration lists

## Usage in Configuration

Once registered, actions can be used in YAML configuration files:

```yaml
routes:
  my_route:
    - check::validate_field:
        field_name: "type"
        expected_value: "forecast"
    - via::add_metadata:
        key: "processed"
        value: "true"
    - route::http_endpoint:
        url: "https://api.example.com/data"
```

The in-situ registration system makes it easy to extend the `bits` library with custom actions while maintaining clean, modular code organization. 