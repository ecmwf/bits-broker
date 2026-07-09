// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use bits::actions::{ActionError, TargetAction, TargetResult};
use bits::job::Job;

#[derive(Debug, Serialize, Deserialize)]
pub struct TargetPanicking;

#[async_trait]
impl TargetAction for TargetPanicking {
    async fn dispatch(&self, _job: &Job) -> Result<TargetResult, ActionError> {
        panic!("intentional panic from TargetPanicking action");
    }
}

bits::register_action!(target, "panicking", TargetPanicking);
