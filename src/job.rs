use serde::{Deserialize, Serialize};
use serde_json::Value;
use bytes::Bytes;
use std::collections::HashMap;

/// A job that flows through the BITS system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// Unique job identifier.
    pub id: String,
    /// Job data as JSON (MARS keys, meteorological parameters, etc.).
    pub data: Value,
    /// Additional metadata.
    pub metadata: HashMap<String, String>,
}

impl Job {
    /// Create a new job with given data.
    pub fn new(data: Value) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            data,
            metadata: HashMap::new(),
        }
    }

    /// Create a job with a specific ID.
    pub fn with_id(id: String, data: Value) -> Self {
        Self {
            id,
            data,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the job.
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    /// Get a field from the job data.
    pub fn get_field(&self, field: &str) -> Option<&Value> {
        self.data.get(field)
    }

    /// Check if job matches a condition.
    pub fn matches_condition(&self, condition: &Value) -> bool {
        // Simple implementation - check if all condition fields match job data
        if let Value::Object(condition_map) = condition {
            for (key, expected_value) in condition_map {
                match self.data.get(key) {
                    Some(actual_value) => {
                        if !values_match(actual_value, expected_value) {
                            return false;
                        }
                    }
                    None => return false,
                }
            }
            true
        } else {
            false
        }
    }
}

/// Check if two JSON values match (with array membership support).
fn values_match(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        // Exact match
        (a, e) if a == e => true,
        // Check if actual value is in expected array
        (actual, Value::Array(expected_array)) => {
            expected_array.iter().any(|e| actual == e)
        }
        _ => false,
    }
}

/// The result of processing a job.
#[derive(Debug)]
pub enum JobResult {
    /// Job completed successfully with result data.
    Completed(Bytes),
    /// Job should be redirected to another location.
    Redirect(String),
    /// Job failed with an error.
    Error(String),
    /// Job was forwarded to external system (no immediate result).
    Forwarded,
}

impl JobResult {
    /// Create a completed result from any serializable data.
    pub fn completed<T: Serialize>(data: T) -> Result<Self, serde_json::Error> {
        let json = serde_json::to_vec(&data)?;
        Ok(JobResult::Completed(Bytes::from(json)))
    }

    /// Create an error result.
    pub fn error<S: Into<String>>(message: S) -> Self {
        JobResult::Error(message.into())
    }

    /// Create a redirect result.
    pub fn redirect<S: Into<String>>(location: S) -> Self {
        JobResult::Redirect(location.into())
    }
} 