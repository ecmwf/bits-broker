// bits-server: a generic BITS HTTP service.
//
// Starts the built-in HTTP server with routes and actions defined
// entirely by the YAML configuration file.
//
// Usage: bits-server <config.yaml>

use std::sync::Arc;

use bits::Bits;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config_path = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: bits-server <config.yaml>");
        std::process::exit(1);
    });
    let config_str = std::fs::read_to_string(&config_path)?;
    let bits = Arc::new(Bits::from_config(&config_str)?);
    let server_config = bits.server_config().clone();

    bits::server::serve(bits, server_config).await
}
