use async_trait::async_trait;
use bits::Job;
use bits::actions::{ActionError, CheckAction, CheckResult};
use serde::{Deserialize, Serialize};

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
