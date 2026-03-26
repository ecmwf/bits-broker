use std::collections::HashMap;

use async_trait::async_trait;
use bits::Job;
use bits::actions::{ActionError, CheckAction, CheckResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing;

use authotron_types::User as AuthUser;

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
                    reason: format!("{key}: '{}' does not match required '{}'", actual, expected),
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

/// Authorization gate: rejects jobs whose authenticated user lacks a required role.
///
/// Reads the auth context from `job.user["auth"]`, which is set by polytope-server
/// when it forwards the authenticated user into the job.
///
/// Use `HasRole` when a route **requires** specific credentials — rejections are
/// non-silent with a generic "insufficient permissions" message (detailed reasons
/// are logged).  For route selection based on auth *presence* alone, use
/// [`HasAuth`] instead.
///
/// Returns `ActionError::AuthError` when:
/// - The auth context exists but is malformed (can't deserialize into `User`)
#[derive(Debug, Serialize, Deserialize)]
pub struct HasRole {
    pub role: String,
    #[serde(default)]
    pub realm: Option<String>,
}

const ACCESS_DENIED: &str = "insufficient permissions";

#[async_trait]
impl CheckAction for HasRole {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        let Some(auth_value) = job.user.get("auth") else {
            tracing::warn!("has_role: no authentication context in job");
            return Ok(CheckResult::Reject {
                reason: ACCESS_DENIED.to_string(),
                silent: false,
            });
        };

        let auth_user: AuthUser = serde_json::from_value(auth_value.clone())
            .map_err(|e| ActionError::AuthError(format!("invalid auth context: {}", e)))?;

        if auth_user.version != 1 {
            tracing::warn!(
                version = auth_user.version,
                "has_role: unsupported auth schema version, expected 1"
            );
            return Ok(CheckResult::Reject {
                reason: ACCESS_DENIED.to_string(),
                silent: false,
            });
        }

        if let Some(ref required_realm) = self.realm
            && &auth_user.realm != required_realm
        {
            tracing::warn!(
                user = auth_user.username,
                realm = auth_user.realm,
                required_realm = required_realm.as_str(),
                "has_role: realm mismatch"
            );
            return Ok(CheckResult::Reject {
                reason: ACCESS_DENIED.to_string(),
                silent: false,
            });
        }

        if auth_user.roles.contains(&self.role) {
            Ok(CheckResult::Pass)
        } else {
            tracing::warn!(
                user = auth_user.username,
                role = self.role,
                "has_role: user lacks required role"
            );
            Ok(CheckResult::Reject {
                reason: ACCESS_DENIED.to_string(),
                silent: false,
            })
        }
    }
}

bits::register_action!(check, "has_role", HasRole);

/// Routing discriminator: passes when the job carries a valid authenticated user.
///
/// Unlike [`HasRole`], rejections are **silent** so the router falls through to
/// the next route (e.g. a public/anonymous path).  Use `HasAuth` to split traffic
/// between authenticated and unauthenticated routes; use `HasRole` to enforce
/// specific credentials within an authenticated route.
///
/// Returns `ActionError::AuthError` when `job.user["auth"]` exists but cannot
/// be parsed as a valid `User` — this indicates a broken auth context, not
/// simply an unauthenticated request.
#[derive(Debug, Serialize, Deserialize)]
pub struct HasAuth {}

#[async_trait]
impl CheckAction for HasAuth {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        let Some(auth_value) = job.user.get("auth") else {
            return Ok(CheckResult::Reject {
                reason: "no authentication context in job".to_string(),
                silent: true,
            });
        };

        if auth_value.is_null() {
            return Ok(CheckResult::Reject {
                reason: "no authentication context in job".to_string(),
                silent: true,
            });
        }

        serde_json::from_value::<AuthUser>(auth_value.clone())
            .map_err(|e| ActionError::AuthError(format!("invalid auth context: {}", e)))?;

        Ok(CheckResult::Pass)
    }
}

bits::register_action!(check, "has_auth", HasAuth);

#[cfg(test)]
mod has_auth_tests {
    use super::*;
    use authotron_types::User;
    use serde_json::json;

    #[tokio::test]
    async fn pass_when_auth_present() {
        let user = User::new("ecmwf".into(), "alice".into(), None, None, None, None);
        let mut job = Job::new(json!({}));
        *job.user_mut() = json!({
            "client_ip": "1.2.3.4",
            "auth": serde_json::to_value(&user).unwrap(),
        });
        let result = (HasAuth {}).evaluate(&job).await.unwrap();
        assert!(matches!(result, CheckResult::Pass));
    }

    #[tokio::test]
    async fn reject_silently_when_no_auth() {
        let mut job = Job::new(json!({}));
        *job.user_mut() = json!({"client_ip": "1.2.3.4"});
        let result = (HasAuth {}).evaluate(&job).await.unwrap();
        match result {
            CheckResult::Reject { silent, .. } => assert!(silent, "should be silent for routing"),
            _ => panic!("expected Reject"),
        }
    }

    #[tokio::test]
    async fn reject_silently_when_user_empty() {
        let job = Job::new(json!({}));
        let result = (HasAuth {}).evaluate(&job).await.unwrap();
        assert!(matches!(result, CheckResult::Reject { silent: true, .. }));
    }

    #[tokio::test]
    async fn reject_silently_when_auth_is_null() {
        let mut job = Job::new(json!({}));
        *job.user_mut() = json!({"auth": null});
        let result = (HasAuth {}).evaluate(&job).await.unwrap();
        assert!(matches!(result, CheckResult::Reject { silent: true, .. }));
    }

    #[tokio::test]
    async fn error_when_auth_is_malformed() {
        let mut job = Job::new(json!({}));
        *job.user_mut() = json!({"auth": "not a valid User object"});
        let result = (HasAuth {}).evaluate(&job).await;
        assert!(
            matches!(result, Err(ActionError::AuthError(_))),
            "malformed auth should be an error, not silent rejection"
        );
    }

    #[tokio::test]
    async fn error_when_auth_is_partial_object() {
        let mut job = Job::new(json!({}));
        *job.user_mut() = json!({"auth": {"username": "alice"}});
        let result = (HasAuth {}).evaluate(&job).await;
        assert!(
            matches!(result, Err(ActionError::AuthError(_))),
            "partial auth object missing required fields should be an error"
        );
    }
}

#[cfg(test)]
mod has_role_tests {
    use super::*;
    use authotron_types::User;
    use serde_json::json;

    fn job_with_auth(auth_user: &User) -> Job {
        let mut job = Job::new(json!({}));
        *job.user_mut() = json!({
            "client_ip": "1.2.3.4",
            "auth": serde_json::to_value(auth_user).unwrap(),
        });
        job
    }

    fn test_user(roles: Vec<&str>, realm: &str) -> User {
        User {
            version: 1,
            username: "alice".to_string(),
            realm: realm.to_string(),
            roles: roles.into_iter().map(String::from).collect(),
            attributes: HashMap::new(),
            scopes: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_has_role_pass() {
        let user = test_user(vec!["data_access", "default"], "ecmwf");
        let job = job_with_auth(&user);
        let check = HasRole {
            role: "data_access".to_string(),
            realm: Some("ecmwf".to_string()),
        };
        let result = check.evaluate(&job).await.unwrap();
        assert!(matches!(result, CheckResult::Pass));
    }

    #[tokio::test]
    async fn test_has_role_missing_role() {
        let user = test_user(vec!["default"], "ecmwf");
        let job = job_with_auth(&user);
        let check = HasRole {
            role: "admin".to_string(),
            realm: Some("ecmwf".to_string()),
        };
        let result = check.evaluate(&job).await.unwrap();
        match result {
            CheckResult::Reject { reason, silent } => {
                assert!(!silent, "rejection should be non-silent");
                assert_eq!(reason, ACCESS_DENIED);
            }
            _ => panic!("expected Reject"),
        }
    }

    #[tokio::test]
    async fn test_has_role_wrong_realm() {
        let user = test_user(vec!["admin"], "other");
        let job = job_with_auth(&user);
        let check = HasRole {
            role: "admin".to_string(),
            realm: Some("ecmwf".to_string()),
        };
        let result = check.evaluate(&job).await.unwrap();
        match result {
            CheckResult::Reject { reason, silent } => {
                assert!(!silent, "rejection should be non-silent");
                assert_eq!(reason, ACCESS_DENIED);
            }
            _ => panic!("expected Reject"),
        }
    }

    #[tokio::test]
    async fn test_has_role_no_auth_context() {
        let mut job = Job::new(json!({}));
        *job.user_mut() = json!({"client_ip": "1.2.3.4"});
        let check = HasRole {
            role: "admin".to_string(),
            realm: None,
        };
        let result = check.evaluate(&job).await.unwrap();
        match result {
            CheckResult::Reject { reason, silent } => {
                assert!(!silent, "rejection should be non-silent");
                assert_eq!(reason, ACCESS_DENIED);
            }
            _ => panic!("expected Reject"),
        }
    }

    #[tokio::test]
    async fn test_has_role_realm_optional() {
        let user = test_user(vec!["viewer"], "anything");
        let job = job_with_auth(&user);
        let check = HasRole {
            role: "viewer".to_string(),
            realm: None, // No realm constraint
        };
        let result = check.evaluate(&job).await.unwrap();
        assert!(matches!(result, CheckResult::Pass));
    }

    #[tokio::test]
    async fn test_has_role_malformed_auth() {
        let mut job = Job::new(json!({}));
        *job.user_mut() = json!({"auth": "not a valid User object"});
        let check = HasRole {
            role: "admin".to_string(),
            realm: None,
        };
        let result = check.evaluate(&job).await;
        assert!(
            matches!(result, Err(ActionError::AuthError(_))),
            "malformed auth should return AuthError"
        );
    }

    #[tokio::test]
    async fn test_has_role_missing_version_defaults_to_v1() {
        let mut job = Job::new(json!({}));
        *job.user_mut() = json!({
            "client_ip": "1.2.3.4",
            "auth": {
                "username": "alice",
                "realm": "ecmwf",
                "roles": ["data_access"],
                "attributes": {},
                "scopes": {}
            }
        });
        let check = HasRole {
            role: "data_access".to_string(),
            realm: Some("ecmwf".to_string()),
        };
        let result = check.evaluate(&job).await.unwrap();
        assert!(matches!(result, CheckResult::Pass));
    }

    #[tokio::test]
    async fn test_has_role_unsupported_version_rejected_non_silently() {
        let mut user = test_user(vec!["data_access"], "ecmwf");
        user.version = 2;
        let job = job_with_auth(&user);
        let check = HasRole {
            role: "data_access".to_string(),
            realm: Some("ecmwf".to_string()),
        };
        let result = check.evaluate(&job).await.unwrap();
        match result {
            CheckResult::Reject { reason, silent } => {
                assert!(!silent, "rejection should be non-silent");
                assert_eq!(reason, ACCESS_DENIED);
            }
            _ => panic!("expected Reject"),
        }
    }

    #[tokio::test]
    async fn test_has_role_malformed_roles_type_returns_auth_error() {
        let mut job = Job::new(json!({}));
        *job.user_mut() = json!({
            "auth": {
                "version": 1,
                "username": "alice",
                "realm": "ecmwf",
                "roles": "admin"
            }
        });
        let check = HasRole {
            role: "admin".to_string(),
            realm: None,
        };
        let result = check.evaluate(&job).await;
        assert!(
            matches!(result, Err(ActionError::AuthError(_))),
            "malformed roles type should return AuthError"
        );
    }

    #[tokio::test]
    async fn test_has_role_reject_is_non_silent() {
        // All rejection paths should have silent: false
        let user = test_user(vec!["default"], "ecmwf");
        let job = job_with_auth(&user);
        let check = HasRole {
            role: "nonexistent".to_string(),
            realm: Some("ecmwf".to_string()),
        };
        match check.evaluate(&job).await.unwrap() {
            CheckResult::Reject { silent, .. } => {
                assert!(!silent, "HasRole rejections must be non-silent");
            }
            _ => panic!("expected Reject"),
        }
    }

    #[tokio::test]
    async fn test_has_role_old_job_without_auth() {
        // Simulates a restored old job that only has client_ip
        let mut job = Job::new(json!({"class": "od"}));
        *job.user_mut() = json!({"client_ip": "10.0.0.1"});
        let check = HasRole {
            role: "data_access".to_string(),
            realm: Some("ecmwf".to_string()),
        };
        let result = check.evaluate(&job).await.unwrap();
        assert!(
            matches!(result, CheckResult::Reject { .. }),
            "old jobs without auth should be rejected"
        );
    }
}
