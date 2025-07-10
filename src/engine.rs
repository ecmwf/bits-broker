use crate::config::BitsConfig;
use crate::job::{Job, JobResult};
use crate::routing::Router;
use crate::queue::QueueManager;

/// The main BITS engine that orchestrates job processing.
pub struct BitsEngine {
    config: BitsConfig,
    router: Router,
    queue_manager: QueueManager,
}

impl BitsEngine {
    /// Create a new BITS engine from configuration.
    pub fn new(config: BitsConfig) -> Self {
        let queue_manager = QueueManager::new(&config.queues);
        let router = Router::new(&config.routes, &queue_manager);
        
        Self {
            config,
            router,
            queue_manager,
        }
    }

    /// Accept and process a job through the routing system.
    pub async fn accept_job(&self, job: Job) -> JobResult {
        self.router.route_job(job).await
    }

    /// Start the engine (initialize workers, HTTP server, etc.)
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Start internal workers for queues
        self.queue_manager.start_workers().await?;
        
        // TODO: Start HTTP server for external worker API
        // TODO: Start any background tasks
        
        Ok(())
    }

    /// Shutdown the engine gracefully.
    pub async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.queue_manager.shutdown().await?;
        Ok(())
    }
} 