use std::collections::HashMap;

use async_trait::async_trait;
use bits::Job;
use bits::actions::{ActionError, CheckAction, CheckResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::date_check::date_check;
use crate::schedule::{ScheduleCatalog, ScheduleReleased};

#[derive(Debug, Serialize, Deserialize)]
pub struct Match {
    #[serde(flatten)]
    pub fields: HashMap<String, Value>,
}

#[async_trait]
impl CheckAction for Match {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        for (key, expected) in &self.fields {
            let Some(actual) = job.request.get(key) else {
                return Ok(CheckResult::Reject {
                    reason: format!("request missing key '{key}'"),
                    silent: true,
                });
            };
            if actual != expected {
                return Ok(CheckResult::Reject {
                    reason: format!(
                        "{key}: '{}' does not match required '{}'",
                        actual, expected
                    ),
                    silent: true,
                });
            }
        }
        Ok(CheckResult::Pass)
    }
}

bits::register_action!(check, "match", Match);

/// Check if the job carries a specific ECMWF data license.
#[derive(Debug, Serialize, Deserialize)]
pub struct HasLicense {
    pub license: String,
}

#[async_trait]
impl CheckAction for HasLicense {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        match job.metadata.get("license").and_then(|v| v.as_str()) {
            Some(l) if l == self.license => Ok(CheckResult::Pass),
            Some(l) => Ok(CheckResult::Reject {
                reason: format!("license '{}' does not match required '{}'", l, self.license),
                silent: true,
            }),
            None => Ok(CheckResult::Reject {
                reason: "no license field found".to_string(),
                silent: true,
            }),
        }
    }
}

bits::register_action!(check, "has_license", HasLicense);

#[derive(Debug, Serialize, Deserialize)]
pub struct DateChecker {
    #[serde(default = "default_date_key")]
    pub key: String,
    pub allowed_values: Vec<String>,
}

fn default_date_key() -> String {
    "date".into()
}

#[async_trait]
impl CheckAction for DateChecker {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        let Some(value) = job.request.get(&self.key) else {
            return Ok(CheckResult::Reject {
                reason: format!("request does not contain expected key '{}'", self.key),
                silent: false,
            });
        };
        match date_check(value, &self.allowed_values) {
            Ok(()) => Ok(CheckResult::Pass),
            Err(err) => Ok(CheckResult::Reject {
                reason: err.to_string(),
                silent: false,
            }),
        }
    }
}

bits::register_action!(check, "date_checker", DateChecker);

#[derive(Debug, Serialize, Deserialize)]
pub struct HasKey {
    pub key: String,
}

#[async_trait]
impl CheckAction for HasKey {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        if job.request.get(&self.key).is_some() {
            Ok(CheckResult::Pass)
        } else {
            Ok(CheckResult::Reject {
                reason: format!("request does not contain key '{}'", self.key),
                silent: true,
            })
        }
    }
}

bits::register_action!(check, "has_key", HasKey);

#[async_trait]
impl CheckAction for ScheduleReleased {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        let catalog = ScheduleCatalog::from_path(&self.path)?;
        match catalog.assert_request_released(&job.request, self.current_time()?) {
            Ok(()) => Ok(CheckResult::Pass),
            Err(ActionError::ResourceError(reason)) => Ok(CheckResult::Reject {
                reason,
                silent: false,
            }),
            Err(ActionError::ConfigError(reason)) => Ok(CheckResult::Reject {
                reason,
                silent: false,
            }),
            Err(err) => Err(err),
        }
    }
}

bits::register_action!(check, "schedule_released", ScheduleReleased);
