// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use bits::actions::{ActionError, TargetAction, TargetResult};
use bits::job::Job;

/// A target that always rejects with `ActionError::UserLimitExceeded`, for
/// exercising the HTTP-level mapping to `429 Too Many Requests` without
/// needing to drive a real per-user dispatcher cap through the HTTP router
/// (which always submits jobs with an anonymous/unidentifiable user).
#[derive(Debug, Serialize, Deserialize)]
pub struct TargetAlwaysUserLimitExceeded;

#[async_trait]
impl TargetAction for TargetAlwaysUserLimitExceeded {
    async fn dispatch(&self, _job: &Job) -> Result<TargetResult, ActionError> {
        Err(ActionError::UserLimitExceeded(
            "user is at the per-user limit (6) for this route".to_string(),
        ))
    }
}

bits::register_action!(
    target,
    "always_user_limit_exceeded",
    TargetAlwaysUserLimitExceeded
);
