use async_trait::async_trait;
use bits::Job;
use bits::actions::{ActionError, TargetAction, TargetResult};
use bits::result::JobResult;
use serde::{Deserialize, Serialize};

/// Dispatch to a MARS retrieval endpoint.
#[derive(Debug, Serialize, Deserialize)]
pub struct MarsDestination {
    pub endpoint: String,
}

#[async_trait]
impl TargetAction for MarsDestination {
    async fn dispatch(&self, _job: &Job) -> Result<TargetResult, ActionError> {
        let data = format!("Data dispatched to MARS endpoint: {}", self.endpoint);
        let data_bytes = bytes::Bytes::from(data.into_bytes());
        let size = data_bytes.len() as i64;
        let stream: Box<
            dyn futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + Unpin,
        > = Box::new(futures::stream::iter(vec![Ok(data_bytes)]));
        Ok(TargetResult::Complete(JobResult::Success {
            content_type: "application/json".to_string(),
            size,
            stream,
        }))
    }
}

bits::register_action!(target, "mars_destination", MarsDestination);

/// Dispatch to a DSS (Data Store Service) endpoint.
#[derive(Debug, Serialize, Deserialize)]
pub struct DssDestination {
    pub endpoint: String,
}

#[async_trait]
impl TargetAction for DssDestination {
    async fn dispatch(&self, _job: &Job) -> Result<TargetResult, ActionError> {
        let data = format!("Data dispatched to DSS endpoint: {}", self.endpoint);
        let data_bytes = bytes::Bytes::from(data.into_bytes());
        let size = data_bytes.len() as i64;
        let stream: Box<
            dyn futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + Unpin,
        > = Box::new(futures::stream::iter(vec![Ok(data_bytes)]));
        Ok(TargetResult::Complete(JobResult::Success {
            content_type: "application/json".to_string(),
            size,
            stream,
        }))
    }
}

bits::register_action!(target, "dss_destination", DssDestination);
