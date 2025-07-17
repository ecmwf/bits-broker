use std::any::TypeId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::ops::Deref;
use serde::{Deserialize, Deserializer};
use serde_yaml::Value;

/// Registry for managing shared resources
#[derive(Debug)]
pub struct ResourceRegistry {
    resources: Mutex<HashMap<(TypeId, Value), Box<dyn std::any::Any + Send + Sync>>>,
}

impl ResourceRegistry {
    /// Create a new resource registry
    pub fn new() -> Self {
        Self {
            resources: Mutex::new(HashMap::new()),
        }
    }

    /// Get or create a resource of type T with the given config value
    pub fn get_or_create<T>(&self, config: &Value) -> Result<Arc<T>, Box<dyn std::error::Error>>
    where
        T: Resource + 'static,
    {
        let key = (TypeId::of::<T>(), config.clone());
        
        // Try to get existing resource
        {
            let resources = self.resources.lock().unwrap();
            if let Some(resource) = resources.get(&key) {
                if let Some(arc_resource) = resource.downcast_ref::<Arc<T>>() {
                    return Ok(arc_resource.clone());
                }
            }
        }

        // Create new resource
        let resource = Arc::new(T::from_config(config)?);
        
        // Store in registry
        {
            let mut resources = self.resources.lock().unwrap();
            resources.insert(key, Box::new(resource.clone()));
        }

        Ok(resource)
    }

    /// Create a Shared<T> instance from a config value
    pub fn create_shared<T>(&self, config: &Value) -> Result<Shared<T>, Box<dyn std::error::Error>>
    where
        T: Resource + 'static,
    {
        let arc_resource = self.get_or_create::<T>(config)?;
        Ok(Shared { inner: arc_resource })
    }

    /// Clear all resources (useful for testing)
    pub fn clear(&self) {
        let mut resources = self.resources.lock().unwrap();
        resources.clear();
    }
}

/// Trait for types that can be created from configuration values
pub trait Resource: Send + Sync {
    /// Create a resource from a configuration value
    fn from_config(config: &Value) -> Result<Self, Box<dyn std::error::Error>>
    where
        Self: Sized;
}

/// A shared resource wrapper that automatically deduplicates based on config
#[derive(Debug)]
pub struct Shared<T> {
    inner: Arc<T>,
}

impl<T> Clone for Shared<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Shared<T> {
    /// Create a new shared resource
    pub fn new(resource: T) -> Self {
        Self {
            inner: Arc::new(resource),
        }
    }

    /// Create a shared resource from config using a registry
    pub fn from_config_with_registry(
        registry: &ResourceRegistry,
        config: &Value,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: Resource + 'static,
    {
        registry.create_shared(config)
    }

    /// Get the inner Arc
    pub fn inner(&self) -> &Arc<T> {
        &self.inner
    }
}

impl<T> Deref for Shared<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'de, T> Deserialize<'de> for Shared<T>
where
    T: Resource + 'static,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let config_value = Value::deserialize(deserializer)?;
        
        // For now, we'll need to find a way to access the registry during deserialization
        // This is a limitation of the current approach - we'll need to implement this differently
        // For now, create a temporary resource without sharing
        let resource = T::from_config(&config_value).map_err(serde::de::Error::custom)?;
        Ok(Shared::new(resource))
    }
}

// Custom deserializer that can work with a registry
pub fn deserialize_shared_with_registry<'de, T, D>(
    deserializer: D,
    registry: &ResourceRegistry,
) -> Result<Shared<T>, D::Error>
where
    T: Resource + 'static,
    D: Deserializer<'de>,
{
    let config_value = Value::deserialize(deserializer)?;
    registry
        .create_shared::<T>(&config_value)
        .map_err(serde::de::Error::custom)
}

// Example resource implementations

/// Example database connection resource
#[derive(Debug)]
pub struct DatabaseConnection {
    pub host: String,
    pub port: u16,
}

impl Resource for DatabaseConnection {
    fn from_config(config: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        // Handle both string and object formats
        match config {
            Value::String(s) => {
                // Parse connection string format like "host:port"
                let parts: Vec<&str> = s.split(':').collect();
                let host = parts.get(0).unwrap_or(&"localhost").to_string();
                let port = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(5432);
                Ok(DatabaseConnection { host, port })
            }
            Value::Mapping(map) => {
                // Parse object format like {host: "postgres.com", port: 900}
                let host = map.get(&Value::String("host".to_string()))
                    .and_then(|v| v.as_str())
                    .unwrap_or("localhost")
                    .to_string();
                let port = map.get(&Value::String("port".to_string()))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(5432) as u16;
                Ok(DatabaseConnection { host, port })
            }
            _ => Err("Invalid database configuration format".into()),
        }
    }
}

/// Example HTTP client resource
#[derive(Debug)]
pub struct HttpClient {
    pub base_url: String,
    pub timeout_seconds: u64,
}

impl Resource for HttpClient {
    fn from_config(config: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        match config {
            Value::String(s) => {
                Ok(HttpClient {
                    base_url: s.clone(),
                    timeout_seconds: 30,
                })
            }
            Value::Mapping(map) => {
                let base_url = map.get(&Value::String("url".to_string()))
                    .and_then(|v| v.as_str())
                    .unwrap_or("http://localhost")
                    .to_string();
                let timeout_seconds = map.get(&Value::String("timeout".to_string()))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(30);
                Ok(HttpClient { base_url, timeout_seconds })
            }
            _ => Err("Invalid HTTP client configuration format".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml;

    #[derive(Debug, PartialEq)]
    struct TestResource {
        value: String,
    }

    impl Resource for TestResource {
        fn from_config(config: &Value) -> Result<Self, Box<dyn std::error::Error>> {
            match config {
                Value::String(s) => Ok(TestResource { value: s.clone() }),
                _ => Ok(TestResource { value: format!("{:?}", config) }),
            }
        }
    }

    #[test]
    fn test_resource_deduplication_with_values() {
        let registry = ResourceRegistry::new();
        
        let config1 = Value::String("test_config".to_string());
        let config2 = Value::String("test_config".to_string());
        
        let resource1 = registry.get_or_create::<TestResource>(&config1).unwrap();
        let resource2 = registry.get_or_create::<TestResource>(&config2).unwrap();
        
        // Should be the same Arc
        assert!(Arc::ptr_eq(&resource1, &resource2));
        
        let config3 = Value::String("different_config".to_string());
        let resource3 = registry.get_or_create::<TestResource>(&config3).unwrap();
        
        // Should be different Arc
        assert!(!Arc::ptr_eq(&resource1, &resource3));
    }

    #[test]
    fn test_shared_deref() {
        let resource = TestResource {
            value: "test".to_string(),
        };
        let shared = Shared::new(resource);
        
        // Should be able to access fields through Deref
        assert_eq!(shared.value, "test");
    }

    #[test]
    fn test_shared_from_config_with_registry() {
        let registry = ResourceRegistry::new();
        
        let config = Value::String("test_config".to_string());
        let shared1 = Shared::<TestResource>::from_config_with_registry(&registry, &config).unwrap();
        let shared2 = Shared::<TestResource>::from_config_with_registry(&registry, &config).unwrap();
        
        // Should be the same underlying Arc
        assert!(Arc::ptr_eq(&shared1.inner, &shared2.inner));
        
        let config2 = Value::String("different_config".to_string());
        let shared3 = Shared::<TestResource>::from_config_with_registry(&registry, &config2).unwrap();
        
        // Should be different underlying Arc
        assert!(!Arc::ptr_eq(&shared1.inner, &shared3.inner));
    }

    #[test]
    fn test_database_connection_object_format() {
        let config_yaml = r#"
host: postgres.com
port: 900
"#;
        let config: Value = serde_yaml::from_str(config_yaml).unwrap();
        let db = DatabaseConnection::from_config(&config).unwrap();
        
        assert_eq!(db.host, "postgres.com");
        assert_eq!(db.port, 900);
    }

    #[test]
    fn test_database_connection_string_format() {
        let config = Value::String("postgres.com:900".to_string());
        let db = DatabaseConnection::from_config(&config).unwrap();
        
        assert_eq!(db.host, "postgres.com");
        assert_eq!(db.port, 900);
    }

    #[test]
    fn test_value_equality_matching() {
        let registry = ResourceRegistry::new();
        
        // These should be the same even with different key ordering
        let config1_yaml = r#"
host: postgres.com
port: 900
"#;
        let config2_yaml = r#"
port: 900
host: postgres.com
"#;
        
        let config1: Value = serde_yaml::from_str(config1_yaml).unwrap();
        let config2: Value = serde_yaml::from_str(config2_yaml).unwrap();
        
        let db1 = registry.create_shared::<DatabaseConnection>(&config1).unwrap();
        let db2 = registry.create_shared::<DatabaseConnection>(&config2).unwrap();
        
        // Should be the same Arc because Values are equal
        assert!(Arc::ptr_eq(&db1.inner, &db2.inner));
    }

    #[test]
    fn test_different_values_different_resources() {
        let registry = ResourceRegistry::new();
        
        let config1_yaml = r#"
host: postgres.com
port: 900
"#;
        let config2_yaml = r#"
host: postgres.com
port: 901
"#;
        
        let config1: Value = serde_yaml::from_str(config1_yaml).unwrap();
        let config2: Value = serde_yaml::from_str(config2_yaml).unwrap();
        
        let db1 = registry.create_shared::<DatabaseConnection>(&config1).unwrap();
        let db2 = registry.create_shared::<DatabaseConnection>(&config2).unwrap();
        
        // Should be different Arc because Values are different
        assert!(!Arc::ptr_eq(&db1.inner, &db2.inner));
    }
}
