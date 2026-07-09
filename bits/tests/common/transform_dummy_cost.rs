// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use bits::actions::{ActionError, TransformAction, TransformResult};
use bits::job::Job;

/// A transform that writes a fixed cost value into `job.metadata["cost"]`.
/// Use upstream of any action that reads cost for weighted scheduling.
#[derive(Debug, Serialize, Deserialize)]
pub struct TransformDummyCost {
    pub cost: u64,
}

#[async_trait]
impl TransformAction for TransformDummyCost {
    async fn execute(&self, job: &mut Job) -> Result<TransformResult, ActionError> {
        job.metadata_mut()["cost"] = serde_json::json!(self.cost);
        Ok(TransformResult::Continue)
    }
}

bits::register_action!(transform, "dummy_cost", TransformDummyCost);
