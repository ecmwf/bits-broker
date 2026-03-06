type ByteStream =
    Box<dyn futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + Unpin>;

pub enum JobResult {
    /// Job completed successfully with streaming data.
    Success {
        content_type: String,
        size: i64,
        stream: ByteStream,
    },
    /// Job should be redirected to another location.
    Redirect { location: String, message: String },
    /// Job-level error (invalid request, etc.).
    Error { message: String },
    /// System-level failure (routing failed, network error, etc.).
    Failed { reason: String },
}

impl std::fmt::Debug for JobResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobResult::Success {
                content_type, size, ..
            } => write!(f, "Success({} bytes, {})", size, content_type),
            JobResult::Redirect { location, message } => {
                write!(f, "Redirect({}, {})", location, message)
            }
            JobResult::Error { message } => write!(f, "Error({})", message),
            JobResult::Failed { reason } => write!(f, "Failed({})", reason),
        }
    }
}

// ------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_result_creation() {
        let _result = JobResult::Success {
            content_type: "text/plain".to_string(),
            size: 100,
            stream: Box::new(futures::stream::empty()),
        };
    }

    #[test]
    fn test_result_debug() {
        let result = JobResult::Success {
            content_type: "text/plain".to_string(),
            size: 100,
            stream: Box::new(futures::stream::empty()),
        };
        assert_eq!(format!("{:?}", result), "Success(100 bytes, text/plain)");
    }

    #[test]
    fn test_result_redirect() {
        let result = JobResult::Redirect {
            location: "https://example.com".to_string(),
            message: "Redirecting to example.com".to_string(),
        };
        assert_eq!(
            format!("{:?}", result),
            "Redirect(https://example.com, Redirecting to example.com)"
        );
    }

    #[test]
    fn test_result_error() {
        let result = JobResult::Error {
            message: "An error occurred".to_string(),
        };
        assert_eq!(format!("{:?}", result), "Error(An error occurred)");
    }

    #[test]
    fn test_result_failed() {
        let result = JobResult::Failed {
            reason: "Queue is full".to_string(),
        };
        assert_eq!(format!("{:?}", result), "Failed(Queue is full)");
    }
}
