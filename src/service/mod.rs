pub mod http;

pub use http::HttpService;

use async_trait::async_trait;

#[async_trait]
pub trait Service: Send + Sync {
    async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}
