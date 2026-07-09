// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use crate::actions::ActionError;
use crate::db::DbError;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BitsError {
    #[error("config: {0}")]
    Config(#[from] ConfigError),

    #[error("routing: {0}")]
    Routing(#[from] RoutingError),

    #[error("persistence: {0}")]
    Persistence(#[from] DbError),

    #[error("action: {0}")]
    Action(#[from] ActionError),

    #[error("worker server: {0}")]
    WorkerServer(#[from] WorkerServerError),
}

impl BitsError {
    pub fn code(&self) -> &'static str {
        match self {
            BitsError::Config(e) => e.code(),
            BitsError::Routing(e) => e.code(),
            BitsError::Persistence(e) => e.code(),
            BitsError::Action(e) => e.code(),
            BitsError::WorkerServer(e) => e.code(),
        }
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            BitsError::Persistence(e) => e.is_retryable(),
            BitsError::Action(e) => e.is_retryable(),
            _ => false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    #[error("YAML syntax error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("{path}: {reason}")]
    Validation { path: String, reason: String },

    #[error("{path}: missing required field")]
    MissingField { path: String },

    #[error("{path}: feature '{feature}' not enabled")]
    FeatureDisabled { path: String, feature: String },

    #[error("{path}: failed to decode {target}: {source}")]
    Decode {
        path: String,
        target: String,
        #[source]
        source: serde_json::Error,
    },

    #[error("{path}: {backend} initialization failed: {reason}")]
    PersistenceInit {
        path: String,
        backend: String,
        reason: String,
    },
}

impl ConfigError {
    pub fn code(&self) -> &'static str {
        match self {
            ConfigError::Yaml(_) => "CONFIG_YAML_SYNTAX",
            ConfigError::Validation { .. } => "CONFIG_VALIDATION",
            ConfigError::MissingField { .. } => "CONFIG_MISSING_FIELD",
            ConfigError::FeatureDisabled { .. } => "CONFIG_FEATURE_DISABLED",
            ConfigError::Decode { .. } => "CONFIG_DECODE",
            ConfigError::PersistenceInit { .. } => "CONFIG_PERSISTENCE_INIT",
        }
    }

    pub fn validation(path: impl Into<String>, reason: impl Into<String>) -> Self {
        ConfigError::Validation {
            path: path.into(),
            reason: reason.into(),
        }
    }

    pub fn missing(path: impl Into<String>) -> Self {
        ConfigError::MissingField { path: path.into() }
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RoutingError {
    #[error("route '{route}': {reason}")]
    InvalidRoute { route: String, reason: String },

    #[error("route '{route}' action '{action}': {reason}")]
    InvalidAction {
        route: String,
        action: String,
        reason: String,
    },

    #[error("route '{route}': must end with a target or switch")]
    MissingTarget { route: String },
}

impl RoutingError {
    pub fn code(&self) -> &'static str {
        match self {
            RoutingError::InvalidRoute { .. } => "ROUTING_INVALID_ROUTE",
            RoutingError::InvalidAction { .. } => "ROUTING_INVALID_ACTION",
            RoutingError::MissingTarget { .. } => "ROUTING_MISSING_TARGET",
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkerServerError {
    #[error("failed to bind to {address}: {reason}")]
    Bind { address: String, reason: String },

    #[error("pool '{name}': {reason}")]
    PoolRegistration { name: String, reason: String },
}

impl WorkerServerError {
    pub fn code(&self) -> &'static str {
        match self {
            WorkerServerError::Bind { .. } => "WORKER_BIND",
            WorkerServerError::PoolRegistration { .. } => "WORKER_POOL",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::ActionError;
    use crate::db::DbError;

    #[test]
    fn config_error_codes_are_stable() {
        assert_eq!(
            ConfigError::validation("x", "y").code(),
            "CONFIG_VALIDATION"
        );
        assert_eq!(ConfigError::missing("x").code(), "CONFIG_MISSING_FIELD");
        assert_eq!(
            ConfigError::FeatureDisabled {
                path: "x".into(),
                feature: "nats".into()
            }
            .code(),
            "CONFIG_FEATURE_DISABLED"
        );
    }

    #[test]
    fn routing_error_codes_are_stable() {
        assert_eq!(
            RoutingError::InvalidRoute {
                route: "r".into(),
                reason: "bad".into()
            }
            .code(),
            "ROUTING_INVALID_ROUTE"
        );
        assert_eq!(
            RoutingError::MissingTarget { route: "r".into() }.code(),
            "ROUTING_MISSING_TARGET"
        );
    }

    #[test]
    fn action_retryability() {
        assert!(ActionError::NetworkError("timeout".into()).is_retryable());
        assert!(ActionError::Timeout("5s".into()).is_retryable());
        assert!(ActionError::QueueFull("full".into()).is_retryable());
        assert!(!ActionError::Cancelled.is_retryable());
        assert!(!ActionError::ClientGone.is_retryable());
        assert!(!ActionError::AuthError("denied".into()).is_retryable());
    }

    #[test]
    fn db_retryability() {
        assert!(DbError::Conflict("cas".into()).is_retryable());
        assert!(DbError::Backend("timeout".into()).is_retryable());
    }

    #[test]
    fn bits_error_delegates_code() {
        let err = BitsError::Config(ConfigError::missing("routes"));
        assert_eq!(err.code(), "CONFIG_MISSING_FIELD");
        assert!(!err.is_retryable());

        let err = BitsError::Persistence(DbError::Backend("down".into()));
        assert_eq!(err.code(), "PERSISTENCE_BACKEND");
        assert!(err.is_retryable());
    }

    #[test]
    fn worker_server_error_codes() {
        assert_eq!(
            WorkerServerError::Bind {
                address: "0.0.0.0:9001".into(),
                reason: "in use".into()
            }
            .code(),
            "WORKER_BIND"
        );
    }
}
