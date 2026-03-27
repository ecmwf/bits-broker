use std::collections::HashMap;

use async_trait::async_trait;
use bits::Job;
use bits::actions::{ActionError, CheckAction, CheckResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;
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

#[derive(Debug, Serialize, Deserialize)]
pub struct HasRole {
    pub roles: HashMap<String, Vec<String>>,
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

        if let Some(allowed_roles) = self.roles.get(&auth_user.realm) {
            if allowed_roles.iter().any(|r| auth_user.roles.contains(r)) {
                return Ok(CheckResult::Pass);
            }
        }

        tracing::warn!(
            user = auth_user.username,
            realm = auth_user.realm,
            "has_role: no matching realm/role pair"
        );
        Ok(CheckResult::Reject {
            reason: ACCESS_DENIED.to_string(),
            silent: false,
        })
    }
}

bits::register_action!(check, "has_role", HasRole);

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

    fn roles(entries: &[(&str, &[&str])]) -> HashMap<String, Vec<String>> {
        entries
            .iter()
            .map(|(realm, roles)| {
                (
                    realm.to_string(),
                    roles.iter().map(|r| r.to_string()).collect(),
                )
            })
            .collect()
    }

    fn assert_reject(result: CheckResult) {
        match result {
            CheckResult::Reject { reason, silent } => {
                assert!(!silent, "HasRole rejections must be non-silent");
                assert_eq!(reason, ACCESS_DENIED);
            }
            _ => panic!("expected Reject, got Pass"),
        }
    }

    #[tokio::test]
    async fn single_realm_pass() {
        let user = test_user(vec!["data_access", "default"], "ecmwf");
        let job = job_with_auth(&user);
        let check = HasRole {
            roles: roles(&[("ecmwf", &["data_access"])]),
        };
        let result = check.evaluate(&job).await.unwrap();
        assert!(matches!(result, CheckResult::Pass));
    }

    #[tokio::test]
    async fn single_realm_wrong_role() {
        let user = test_user(vec!["default"], "ecmwf");
        let job = job_with_auth(&user);
        let check = HasRole {
            roles: roles(&[("ecmwf", &["admin"])]),
        };
        assert_reject(check.evaluate(&job).await.unwrap());
    }

    #[tokio::test]
    async fn single_realm_wrong_realm() {
        let user = test_user(vec!["admin"], "other");
        let job = job_with_auth(&user);
        let check = HasRole {
            roles: roles(&[("ecmwf", &["admin"])]),
        };
        assert_reject(check.evaluate(&job).await.unwrap());
    }

    #[tokio::test]
    async fn multi_realm_one_matches() {
        let user = test_user(vec!["data_access"], "cds");
        let job = job_with_auth(&user);
        let check = HasRole {
            roles: roles(&[("ecmwf", &["admin"]), ("cds", &["data_access"])]),
        };
        let result = check.evaluate(&job).await.unwrap();
        assert!(matches!(result, CheckResult::Pass));
    }

    #[tokio::test]
    async fn multi_realm_none_match() {
        let user = test_user(vec!["viewer"], "ecmwf");
        let job = job_with_auth(&user);
        let check = HasRole {
            roles: roles(&[("ecmwf", &["admin"]), ("cds", &["data_access"])]),
        };
        assert_reject(check.evaluate(&job).await.unwrap());
    }

    #[tokio::test]
    async fn multi_realm_role_in_wrong_realm() {
        let user = test_user(vec!["admin", "data_access"], "other");
        let job = job_with_auth(&user);
        let check = HasRole {
            roles: roles(&[("ecmwf", &["admin"]), ("cds", &["data_access"])]),
        };
        assert_reject(check.evaluate(&job).await.unwrap());
    }

    #[tokio::test]
    async fn same_realm_multiple_roles() {
        let user = test_user(vec!["viewer", "default"], "ecmwf");
        let job = job_with_auth(&user);
        let check = HasRole {
            roles: roles(&[("ecmwf", &["admin", "viewer"])]),
        };
        let result = check.evaluate(&job).await.unwrap();
        assert!(matches!(result, CheckResult::Pass));
    }

    #[tokio::test]
    async fn empty_roles_map_rejects() {
        let user = test_user(vec!["admin"], "ecmwf");
        let job = job_with_auth(&user);
        let check = HasRole {
            roles: HashMap::new(),
        };
        assert_reject(check.evaluate(&job).await.unwrap());
    }

    #[tokio::test]
    async fn realm_with_empty_allowed_roles_rejects() {
        let user = test_user(vec!["admin", "default"], "ecmwf");
        let job = job_with_auth(&user);
        let check = HasRole {
            roles: roles(&[("ecmwf", &[])]),
        };
        assert_reject(check.evaluate(&job).await.unwrap());
    }

    #[tokio::test]
    async fn user_with_no_roles_rejects() {
        let user = test_user(vec![], "ecmwf");
        let job = job_with_auth(&user);
        let check = HasRole {
            roles: roles(&[("ecmwf", &["data_access"])]),
        };
        assert_reject(check.evaluate(&job).await.unwrap());
    }

    #[tokio::test]
    async fn no_auth_context() {
        let mut job = Job::new(json!({}));
        *job.user_mut() = json!({"client_ip": "1.2.3.4"});
        let check = HasRole {
            roles: roles(&[("ecmwf", &["admin"])]),
        };
        assert_reject(check.evaluate(&job).await.unwrap());
    }

    #[tokio::test]
    async fn malformed_auth() {
        let mut job = Job::new(json!({}));
        *job.user_mut() = json!({"auth": "not a valid User object"});
        let check = HasRole {
            roles: roles(&[("ecmwf", &["admin"])]),
        };
        assert!(matches!(
            check.evaluate(&job).await,
            Err(ActionError::AuthError(_))
        ));
    }

    #[tokio::test]
    async fn malformed_roles_type_returns_auth_error() {
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
            roles: roles(&[("ecmwf", &["admin"])]),
        };
        assert!(
            matches!(check.evaluate(&job).await, Err(ActionError::AuthError(_))),
            "string instead of array for roles should return AuthError"
        );
    }

    #[tokio::test]
    async fn unsupported_version() {
        let mut user = test_user(vec!["data_access"], "ecmwf");
        user.version = 2;
        let job = job_with_auth(&user);
        let check = HasRole {
            roles: roles(&[("ecmwf", &["data_access"])]),
        };
        assert_reject(check.evaluate(&job).await.unwrap());
    }

    #[test]
    fn deserialize_from_yaml() {
        let yaml = r#"
roles:
  ecmwf:
    - admin
    - data_access
  cds:
    - viewer
"#;
        let check: HasRole = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(check.roles.len(), 2);
        assert_eq!(check.roles["ecmwf"], vec!["admin", "data_access"]);
        assert_eq!(check.roles["cds"], vec!["viewer"]);
    }
}
