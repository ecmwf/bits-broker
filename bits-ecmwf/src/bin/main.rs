// bits-ecmwf: the BITS HTTP service with ECMWF actions pre-loaded.
//
// ECMWF actions are registered automatically at startup via inventory.
// Configure via a YAML file passed as the first argument.
//
// Usage: bits-ecmwf <config.yaml>

use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .event_format(bits::telemetry::PrettyFormat)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,tikv_client=error")),
        )
        .init();

    let config_path = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: bits-ecmwf <config.yaml>");
        std::process::exit(1);
    });
    let config_str = std::fs::read_to_string(&config_path)?;
    let (bits, server_config) = bits::parse_bootstrap(&config_str)?.into_parts()?;
    let bits = Arc::new(bits);

    bits::server::serve_with_shutdown(bits, server_config, bits::server::shutdown_signal()).await
}
