// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use crate::bits::SubmitOutcome;
use crate::job::Job;
use crate::routing::switch::Switch;
use crate::runtime::submission::{SubmissionAdmission, SubmitContext};

#[derive(Clone)]
pub struct RouteHandle {
    pub(crate) name: String,
    pub(crate) router: Arc<Switch>,
    pub(crate) submit_context: SubmitContext,
}

impl RouteHandle {
    pub fn submit(&self, job: Job) -> SubmitOutcome {
        self.submit_context.submit(
            self.router.clone(),
            job,
            SubmissionAdmission::EnforceLimit,
            Some(&self.name),
        )
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the validated site tag inherited from the broker runtime.
    pub fn site(&self) -> &str {
        &self.submit_context.site
    }

    /// Returns the validated environment tag inherited from the broker runtime.
    pub fn env(&self) -> &str {
        &self.submit_context.env
    }

    /// Returns the allocated broker slot inherited from the broker runtime.
    pub fn broker_slot(&self) -> u16 {
        self.submit_context.broker_slot
    }
}
