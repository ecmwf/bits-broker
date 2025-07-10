use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub request: Value,
    pub user: Value,
    pub created_at: DateTime<Utc>,
    pub metadata: Value,
}

impl Job {
    pub fn new(request: Value) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            request,
            user: serde_json::json!({}),
            created_at: Utc::now(),
            metadata: serde_json::json!({}),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_job_creation() {
        let request = json!({
            "class": "od",
            "stream": "oper",
            "type": "fc"
        });

        let job = Job::new(request.clone());
        assert_eq!(job.request, request);
        assert_eq!(job.user, serde_json::json!({}));
    }
}
