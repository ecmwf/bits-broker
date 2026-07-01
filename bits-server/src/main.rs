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
        eprintln!("usage: bits-server <config.yaml>");
        std::process::exit(1);
    });
    let config_str = std::fs::read_to_string(&config_path)?;
    let bootstrap = bits::parse_bootstrap(&config_str)?;
    let metrics_buckets = bootstrap.metrics_buckets();
    let (bits, server_config) = bootstrap.into_parts()?;
    let bits = Arc::new(bits);

    // Installs the global Prometheus meter provider; `serve_with_shutdown` picks
    // up the resulting handle and exposes it on `/metrics`.
    bits::metrics::init_prometheus(metrics_buckets);

    bits::server::serve_with_shutdown(bits, server_config, bits::server::shutdown_signal()).await
}
