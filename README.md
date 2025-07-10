# BITS - Brokering and Intelligent Task Switching

BITS is a distributed sharded queue/brokering system designed for processing meteorological data jobs through configurable pipelines. It's specifically designed for ECMWF's data distribution needs across European HPC infrastructure.

## Features

- **Configurable Routing**: YAML-based configuration for defining job processing pipelines
- **Multiple Queue Types**: FIFO, Priority, and Fair scheduling queues
- **Hybrid Workers**: Support for both internal and external worker pools
- **Transport Agnostic**: Designed to work with HTTP initially, extensible to other protocols
- **MARS Integration**: Built-in support for ECMWF MARS meteorological data keys
- **EuroHPC Ready**: Designed for deployment across LUMI, Leonardo, and Mare Nostrum

## Architecture

### Core Components

1. **BitsEngine**: Main orchestrator that manages the entire system
2. **Router**: Executes routing logic based on configured rules
3. **QueueManager**: Manages all queues and their associated workers
4. **Actions**: Pluggable action system for processing jobs

### Action Types

- **`filter`**: Conditional routing based on job data (MARS keys)
- **`check`**: Rule-based guards for access control
- **`via`**: Processing through queues (fast async or queued)
- **`switch`**: Nested routing with multiple branches
- **`destination`**: Final routing to workers or external systems

### Queue Types

- **`fifo`**: First-in, first-out processing
- **`priority`**: Priority-based scheduling using job metadata
- **`fair`**: Round-robin scheduling between different job types

## Configuration

Configuration is defined in YAML format with two main sections:

### Queues

```yaml
queues:
  forecast_processor:
    type: "fifo"
    capacity: 500
    # workers: 4  # Optional: internal workers
    
  analysis_processor:
    type: "priority"
    capacity: 800
    # No workers = external workers poll this queue
```

### Routes

```yaml
routes:
  operational_forecast:
    - type: "filter"
      class: "od"           # MARS class
      stream: "oper"        # MARS stream
      type: "fc"           # MARS type
    - type: "check"
      rule: "has_forecast_access"
    - type: "via"
      queue: "forecast_processor"
    - type: "destination"
      queue: "bulk_dissemination"
```

## Usage

### Basic Usage

```rust
use bits::{BitsConfig, BitsEngine, Job};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration
    let config = BitsConfig::from_file("config.yaml")?;
    
    // Create and start engine
    let engine = BitsEngine::new(config);
    engine.start().await?;
    
    // Create a meteorological data job
    let job = Job::new(json!({
        "class": "od",
        "stream": "oper",
        "type": "fc",
        "param": "2t",
        "levtype": "sfc",
        "date": "2024-01-15"
    }));
    
    // Process the job
    let result = engine.accept_job(job).await;
    
    // Handle result
    match result {
        bits::JobResult::Completed(data) => {
            println!("Job completed: {}", String::from_utf8_lossy(&data));
        }
        bits::JobResult::Forwarded => {
            println!("Job forwarded to external system");
        }
        bits::JobResult::Error(error) => {
            println!("Job failed: {}", error);
        }
        _ => {}
    }
    
    engine.shutdown().await?;
    Ok(())
}
```

### Running the Example

```bash
cargo run --example basic_usage
```

## MARS Integration

BITS natively supports ECMWF MARS (Meteorological Archival and Retrieval System) keys for filtering and routing meteorological data:

- **`class`**: Data class (od, ea, c3, etc.)
- **`stream`**: Data stream (oper, enfo, etc.)
- **`type`**: Data type (fc, an, pf, etc.)
- **`param`**: Parameters (2t, 10u, 10v, tp, msl, etc.)
- **`levtype`**: Level type (sfc, pl, ml, etc.)
- **`date`**, **`time`**, **`step`**: Temporal specifications

## EuroHPC Integration

BITS is designed to work across European HPC infrastructure:

- **LUMI** (Finland): Surface data processing
- **Leonardo** (Italy): Pressure level data
- **Mare Nostrum** (Spain): Climate reanalysis

Example configuration for HPC routing:

```yaml
routes:
  copernicus_climate:
    - type: "filter"
      class: "c3"
    - type: "switch"
      routes:
        surface_data:
          - type: "filter"
            levtype: "sfc"
          - type: "destination"
            url: "https://lumi.csc.fi/ecmwf/surface"
            method: "POST"
        pressure_levels:
          - type: "filter"
            levtype: "pl"
          - type: "destination"
            url: "https://leonardo.cineca.it/ecmwf/pressure"
            method: "POST"
```

## Development Status

This is the initial implementation with the following features:

✅ **Completed:**
- Core routing engine
- YAML configuration parsing
- Multiple queue types (FIFO, Priority, Fair)
- Job filtering and matching
- Basic worker pool management
- MARS key support

🚧 **In Progress:**
- HTTP server for job submission
- External worker API
- Rule checking system
- HTTP client for external destinations

📋 **Planned:**
- Hot configuration reloading
- Advanced monitoring and metrics
- Load balancing and sharding
- Persistent job state
- Advanced rule engine (Python integration)

## Testing

Run the test suite:

```bash
cargo test
```

## Dependencies

- **Tokio**: Async runtime
- **Serde**: Serialization (JSON/YAML)
- **UUID**: Job identification
- **Bytes**: Efficient byte handling
- **Async-trait**: Async trait support

## License

[License information to be added]

## Contributing

[Contributing guidelines to be added] 