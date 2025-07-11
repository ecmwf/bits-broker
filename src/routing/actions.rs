use std::collections::HashMap;

use crate::job::Job;
use crate::result::JobResult;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, OneOrMany};

// ================================
//        Action Errors
// ================================

/// System-level errors that can occur during action execution
#[derive(Debug)]
pub enum ActionError {
    NetworkError(String),
    QueueFull(String),
    Timeout(String),
    ConfigError(String),
    AuthError(String),
    ResourceError(String),
}

impl std::fmt::Display for ActionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActionError::NetworkError(msg) => write!(f, "Network error: {}", msg),
            ActionError::QueueFull(msg) => write!(f, "Queue full: {}", msg),
            ActionError::Timeout(msg) => write!(f, "Timeout: {}", msg),
            ActionError::ConfigError(msg) => write!(f, "Config error: {}", msg),
            ActionError::AuthError(msg) => write!(f, "Auth error: {}", msg),
            ActionError::ResourceError(msg) => write!(f, "Resource error: {}", msg),
        }
    }
}

impl std::error::Error for ActionError {}

// ================================
//   Actions
// ================================

pub enum Action {
    Check(Box<dyn CheckAction>),
    Via(Box<dyn ViaAction>),
    Router(Box<dyn RouteAction>),
}

// ================================
//   Check Actions
// ================================

/// Actions which check conditions on a job, typically checking the job data itself or the job's metadata
#[async_trait]
pub trait CheckAction: Send + Sync {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError>;
}

/// Result of a check action
#[derive(Debug)]
pub enum CheckResult {
    Pass,
    Reject { reason: String },
}

// ================================
//   Via Actions
// ================================

/// Actions that transform jobs, typically by adding or changing metadata
#[async_trait]
pub trait ViaAction: Send + Sync {
    async fn execute(&self, job: &mut Job) -> Result<ViaResult, ActionError>;
}

pub enum ViaResult {
    Continue,
    Reject { reason: String },
}

// ================================
//    Route Actions
// ================================

/// Actions that direct jobs to another route segment or destination
#[async_trait]
pub trait RouteAction: Send + Sync {
    async fn route(&self, job: &mut Job) -> Result<RouteResult, ActionError>;
}

pub enum RouteResult {
    Complete(JobResult),
    Reject { reason: String },
}


// Check Actions
// -------------


/// Match based on request keys
#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct Match {
    #[serde_as(as = "HashMap<_, OneOrMany<_>>")]
    #[serde(flatten, default)]
    matches: HashMap<String, Vec<String>>,
}


#[async_trait]
impl CheckAction for Match {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        for (key, match_values) in &self.matches {
            match job.request.get(key) {
                Some(serde_json::Value::String(job_value)) => {
                    // Single string value - check if it matches any value
                    if !match_values.contains(job_value) {
                        return Ok(CheckResult::Reject { 
                            reason: format!("Match {} failed: '{}' not in allowed values", key, job_value) 
                        });
                    }
                }
                Some(serde_json::Value::Array(job_values)) => {
                    // Array of values - all must be strings and all must match
                    for job_val in job_values {
                        match job_val {
                            serde_json::Value::String(s) => {
                                if !match_values.contains(s) {
                                    return Ok(CheckResult::Reject { 
                                        reason: format!("Match {} failed: '{}' not in allowed values", key, s) 
                                    });
                                }
                            }
                            _ => {
                                return Ok(CheckResult::Reject { 
                                    reason: format!("Match {} failed: array contains non-string value", key) 
                                });
                            }
                        }
                    }
                }
                Some(_) => {
                    // Invalid data type (not string or array)
                    return Ok(CheckResult::Reject { 
                        reason: format!("Match {} failed: value must be string or array of strings", key) 
                    });
                }
                None => {
                    // Key not found in request
                    return Ok(CheckResult::Reject { 
                        reason: format!("Match {} failed: key not found in request", key) 
                    });
                }
            }
        }
        Ok(CheckResult::Pass)
    }
}

/// Has Role
#[derive(Debug, Serialize, Deserialize)]
pub struct HasRole {
    roles: Vec<String>,
}

#[async_trait]
impl CheckAction for HasRole {
    async fn evaluate(&self, _job: &Job) -> Result<CheckResult, ActionError> {
        // TODO: Implement role check
        Ok(CheckResult::Pass)
    }
}


/// Has License
#[derive(Debug, Serialize, Deserialize)]
pub struct HasLicense {
    licenses: Vec<String>,
}

#[async_trait]
impl CheckAction for HasLicense {
    async fn evaluate(&self, _job: &Job) -> Result<CheckResult, ActionError> {
        // TODO: Implement license check
        Ok(CheckResult::Pass)
    }
}

// Via Actions
// -----------


/// Metkit Expansion
#[derive(Debug, Serialize, Deserialize)]
pub struct MetkitExpansion {
}

#[async_trait]
impl ViaAction for MetkitExpansion {
    async fn execute(&self, job: &mut Job) -> Result<ViaResult, ActionError> {
        // TODO: Implement metkit expansion
        let mut metadata = job.metadata.as_object().unwrap_or(&serde_json::Map::new()).clone();
        metadata.insert("expansion".to_string(), serde_json::json!(true));
        job.metadata = serde_json::Value::Object(metadata);
        Ok(ViaResult::Continue)
    }
}

// Route Actions
// -------------

/// Mars Destination
#[derive(Debug, Serialize, Deserialize)]
pub struct MarsDestination {
    host: String,
    port: u16,
}

#[async_trait]
impl RouteAction for MarsDestination {
    async fn route(&self, _job: &mut Job) -> Result<RouteResult, ActionError> {
        // Generate 100 random ASCII characters as dummy data
        use rand::Rng;
        let mut rng = rand::rng();
        let random_chars: String = (0..100)
            .map(|_| rng.random_range(32..127) as u8 as char) // ASCII printable characters
            .collect();
        
        let random_bytes = bytes::Bytes::from(random_chars.into_bytes());
        let stream: Box<dyn futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + Unpin> = 
            Box::new(futures::stream::iter(vec![Ok(random_bytes)]));
        
        Ok(RouteResult::Complete(JobResult::Success {
            content_type: "application/octet-stream".to_string(),
            size: 100,
            stream,
        }))
    }
}