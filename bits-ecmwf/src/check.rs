use async_trait::async_trait;
use bits::Job;
use bits::actions::{ActionError, CheckAction, CheckResult};
use serde::{Deserialize, Serialize};

use crate::date_check::date_check;
use crate::schedule::{ScheduleCatalog, ScheduleReleased};

/// Check if a job matches a specific MARS class (e.g. "od", "ea").
#[derive(Debug, Serialize, Deserialize)]
pub struct Match {
    pub class: String,
}

#[async_trait]
impl CheckAction for Match {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        match job.request.get("class").and_then(|v| v.as_str()) {
            Some(c) if c == self.class => Ok(CheckResult::Pass),
            Some(c) => Ok(CheckResult::Reject {
                reason: format!("class '{}' does not match required '{}'", c, self.class),
            }),
            None => Ok(CheckResult::Reject {
                reason: "no class field found".to_string(),
            }),
        }
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
            }),
            None => Ok(CheckResult::Reject {
                reason: "no license field found".to_string(),
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
            });
        };
        match date_check(value, &self.allowed_values) {
            Ok(()) => Ok(CheckResult::Pass),
            Err(err) => Ok(CheckResult::Reject {
                reason: err.to_string(),
            }),
        }
    }
}

bits::register_action!(check, "date_checker", DateChecker);

#[async_trait]
impl CheckAction for ScheduleReleased {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        let catalog = ScheduleCatalog::from_path(&self.path)?;
        match catalog.assert_request_released(&job.request, self.current_time()?) {
            Ok(()) => Ok(CheckResult::Pass),
            Err(ActionError::ResourceError(reason)) | Err(ActionError::ConfigError(reason)) => {
                Ok(CheckResult::Reject { reason })
            }
            Err(err) => Err(err),
        }
    }
}

bits::register_action!(check, "schedule_released", ScheduleReleased);
