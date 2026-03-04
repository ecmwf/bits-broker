use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    /// The original request as submitted by the client. Never modified after creation.
    /// Used as the restart point on broker recovery.
    pub original_request: Value,
    /// The working request, mutated by transform actions as the job flows through the pipeline.
    pub request: Value,
    pub user: Value,
    pub created_at: DateTime<Utc>,
    pub metadata: Value,
    pub persistent: bool,
}

impl Job {
    pub fn new(request: Value) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            original_request: request.clone(),
            request,
            user: serde_json::json!({}),
            created_at: Utc::now(),
            metadata: serde_json::json!({}),
            persistent: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_job_creation() {
        let request = json!({"class": "od", "stream": "oper"});
        let job = Job::new(request.clone());
        assert_eq!(job.original_request, request);
        assert_eq!(job.request, request);
        assert!(!job.persistent);
    }
}
