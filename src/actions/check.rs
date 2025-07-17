use crate::actions::{ActionError, CheckAction, CheckResult};
use crate::job::Job;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

// ================================
//   Match Action
// ================================

/// Check if a job matches a specific class
#[derive(Debug, Serialize, Deserialize)]
pub struct Match {
    pub class: String,
}

#[async_trait]
impl CheckAction for Match {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        if let Some(class_value) = job.request.get("class") {
            if class_value.as_str() == Some(&self.class) {
                Ok(CheckResult::Pass)
            } else {
                Ok(CheckResult::Reject {
                    reason: format!("Class '{}' does not match required '{}'", class_value, self.class)
                })
            }
        } else {
            Ok(CheckResult::Reject {
                reason: "No class field found".to_string()
            })
        }
    }
}

// Register the Match action
crate::register_action!(check, "match", Match);

// ================================
//   HasRole Action
// ================================

/// Check if a job has a specific role
#[derive(Debug, Serialize, Deserialize)]
pub struct HasRole {
    pub role: String,
}

#[async_trait]
impl CheckAction for HasRole {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        if let Some(roles) = job.metadata.get("roles") {
            if let Some(role_array) = roles.as_array() {
                for role_value in role_array {
                    if role_value.as_str() == Some(&self.role) {
                        return Ok(CheckResult::Pass);
                    }
                }
                Ok(CheckResult::Reject {
                    reason: format!("Role '{}' not found in job roles", self.role)
                })
            } else {
                Ok(CheckResult::Reject {
                    reason: "Roles field is not an array".to_string()
                })
            }
        } else {
            Ok(CheckResult::Reject {
                reason: "No roles field found".to_string()
            })
        }
    }
}

// Register the HasRole action
crate::register_action!(check, "has_role", HasRole);

// ================================
//   HasLicense Action
// ================================

/// Check if a job has a specific license
#[derive(Debug, Serialize, Deserialize)]
pub struct HasLicense {
    pub license: String,
}

#[async_trait]
impl CheckAction for HasLicense {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        if let Some(license_value) = job.metadata.get("license") {
            if license_value.as_str() == Some(&self.license) {
                Ok(CheckResult::Pass)
            } else {
                Ok(CheckResult::Reject {
                    reason: format!("License '{}' does not match required '{}'", license_value, self.license)
                })
            }
        } else {
            Ok(CheckResult::Reject {
                reason: "No license field found".to_string()
            })
        }
    }
}

// Register the HasLicense action
crate::register_action!(check, "has_license", HasLicense); 