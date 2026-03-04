use crate::actions::{ActionError, CheckAction, CheckResult};
use crate::job::Job;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Check if the job carries a specific role (e.g. set by an auth middleware).
#[derive(Debug, Serialize, Deserialize)]
pub struct HasRole {
    pub role: String,
}

#[async_trait]
impl CheckAction for HasRole {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        let roles = job.metadata.get("roles").and_then(|v| v.as_array());
        match roles {
            Some(arr) if arr.iter().any(|v| v.as_str() == Some(&self.role)) => {
                Ok(CheckResult::Pass)
            }
            Some(_) => Ok(CheckResult::Reject {
                reason: format!("role '{}' not found", self.role),
            }),
            None => Ok(CheckResult::Reject {
                reason: "no roles field found".to_string(),
            }),
        }
    }
}

crate::register_action!(check, "has_role", HasRole);
