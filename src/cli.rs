use std::path::PathBuf;
use std::process;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "bits", about = "Broker for Intelligent Task Scheduling")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the BITS service using a config file
    Serve {
        /// Path to the YAML config file
        config: PathBuf,
    },
}

/// Parse arguments and run the BITS service.
///
/// Exposed as a library function so extension crates can provide their own
/// binary entry point, pre-registering actions before delegating here.
pub async fn run() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    if std::io::IsTerminal::is_terminal(&std::io::stdout()) {
        tracing_subscriber::fmt()
            .event_format(crate::telemetry::PrettyFormat)
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init();
    }

    let cli = Cli::parse();

    match cli.command {
        Command::Serve { config } => {
            let config_str = std::fs::read_to_string(&config).unwrap_or_else(|e| {
                eprintln!("bits: cannot read {}: {}", config.display(), e);
                process::exit(1);
            });

            crate::Bits::from_config(&config_str)
                .unwrap_or_else(|e| {
                    eprintln!("bits: invalid config: {}", e);
                    process::exit(1);
                })
                .serve()
                .await
                .unwrap_or_else(|e| {
                    eprintln!("bits: server error: {}", e);
                    process::exit(1);
                });
        }
    }
}
