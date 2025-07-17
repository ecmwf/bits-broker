use crate::actions::{ActionError, RouteAction, RouteResult};
use crate::job::Job;
use crate::result::JobResult;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

// ================================
//   MarsDestination Action
// ================================

/// Route to MARS destination
#[derive(Debug, Serialize, Deserialize)]
pub struct MarsDestination {
    pub endpoint: String,
}

#[async_trait]
impl RouteAction for MarsDestination {
    async fn route(&self, _job: &Job) -> Result<RouteResult, ActionError> {
        let data = format!("Data routed to MARS endpoint: {}", self.endpoint);
        let data_bytes = bytes::Bytes::from(data.into_bytes());
        let size = data_bytes.len() as i64;
        let stream: Box<dyn futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + Unpin> = 
            Box::new(futures::stream::iter(vec![Ok(data_bytes)]));
        
        Ok(RouteResult::Complete(JobResult::Success {
            content_type: "application/json".to_string(),
            size,
            stream,
        }))
    }
}

// Register the MarsDestination action
crate::register_action!(route, "mars_destination", MarsDestination);

// ================================
//   DssDestination Action
// ================================

/// Route to DSS destination
#[derive(Debug, Serialize, Deserialize)]
pub struct DssDestination {
    pub endpoint: String,
}

#[async_trait]
impl RouteAction for DssDestination {
    async fn route(&self, _job: &Job) -> Result<RouteResult, ActionError> {
        let data = format!("Data routed to DSS endpoint: {}", self.endpoint);
        let data_bytes = bytes::Bytes::from(data.into_bytes());
        let size = data_bytes.len() as i64;
        let stream: Box<dyn futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + Unpin> = 
            Box::new(futures::stream::iter(vec![Ok(data_bytes)]));
        
        Ok(RouteResult::Complete(JobResult::Success {
            content_type: "application/json".to_string(),
            size,
            stream,
        }))
    }
}

// Register the DssDestination action
crate::register_action!(route, "dss_destination", DssDestination); 