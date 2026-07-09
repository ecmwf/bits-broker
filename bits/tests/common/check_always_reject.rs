// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use bits::actions::{ActionError, CheckAction, CheckResult};
use bits::job::Job;

#[derive(Debug, Serialize, Deserialize)]
pub struct CheckAlwaysReject;

#[async_trait]
impl CheckAction for CheckAlwaysReject {
    async fn evaluate(&self, _job: &Job) -> Result<CheckResult, ActionError> {
        Ok(CheckResult::Reject {
            reason: "always rejects".into(),
            silent: true,
        })
    }
}

bits::register_action!(check, "always_reject", CheckAlwaysReject);
