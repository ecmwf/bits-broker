# BITS Configuration Examples for ECMWF Meteorological Data

This document shows examples of configuring BITS using both YAML and Rust fluent API for ECMWF meteorological data distribution across EuroHPC infrastructure.

## YAML Configuration

### Basic Example - Weather Data Distribution

```yaml
queues:
  auth_service:
    type: "fifo"
    capacity: 1000
    workers: 4
    
  forecast_processor:
    type: "fifo"
    capacity: 500
    # External workers poll this queue from LUMI
    
  analysis_processor:
    type: "priority"
    capacity: 800
    # External workers from Leonardo
    
  bulk_dissemination:
    type: "fair"
    capacity: 2000

routes:
  operational_forecast:
    - filter: {class: "od", stream: "oper", type: "fc"}
    - check: "has_forecast_access"
    - via: "auth_service"
    - switch:
        high_res_forecast:
          - filter: {levtype: "sfc", param: ["2t", "10u", "10v", "tp"]}
          - filter: {step: [0, 6, 12, 18, 24]}
          - destination: "lumi_workers"
        pressure_levels:
          - filter: {levtype: "pl", levelist: [1000, 850, 500, 250]}
          - filter: {param: ["t", "u", "v", "z", "r"]}
          - destination: "leonardo_workers"
        model_levels:
          - filter: {levtype: "ml"}
          - via: "forecast_processor"
          - destination: "mare_nostrum_workers"
  
  analysis_data:
    - filter: {class: "od", stream: "oper", type: "an"}
    - check: "has_analysis_access"
    - via: "auth_service"
    - switch:
        surface_analysis:
          - filter: {levtype: "sfc"}
          - filter: {param: ["msl", "2t", "2d", "10u", "10v"]}
          - destination: "lumi_workers"
        upper_air:
          - filter: {levtype: "pl"}
          - filter: {levelist: [1000, 925, 850, 700, 500, 400, 300, 250, 200, 150, 100]}
          - via: "analysis_processor"
          - destination: "leonardo_workers"
  
  ensemble_forecast:
    - filter: {class: "od", stream: "enfo", type: "pf"}
    - check: "has_ensemble_access"
    - filter: {number: [1, 2, 3, 4, 5]}  # First 5 ensemble members
    - via: "forecast_processor"
    - switch:
        probabilistic_surface:
          - filter: {levtype: "sfc", param: ["tp", "2t"]}
          - destination: "bulk_dissemination"
        probabilistic_upper:
          - filter: {levtype: "pl", levelist: [500, 850]}
          - destination: "mare_nostrum_workers"
  
  reanalysis_data:
    - filter: {class: "ea", stream: "oper"}  # ERA5
    - check: "has_reanalysis_access"
    - switch:
        hourly_surface:
          - filter: {levtype: "sfc"}
          - filter: {param: ["2t", "tp", "sp", "tcw"]}
          - destination: "bulk_dissemination"
        monthly_means:
          - filter: {type: "mnth"}
          - destination: "leonardo_workers"
        pressure_levels:
          - filter: {levtype: "pl"}
          - via: "analysis_processor"
          - destination: "lumi_workers"
  
  climate_data:
    - filter: {class: "c3", stream: "oper"}  # Copernicus Climate Change Service
    - check: "has_climate_access"
    - destination: {url: "https://climate-api.ecmwf.int/process"}
  
  member_state_data:
    - filter: {class: "od", stream: "oper"}
    - check: "has_member_state_access"
    - filter: {area: "europe"}  # European domain
    - switch:
        national_forecasts:
          - filter: {type: "fc", step: [0, 6, 12, 18, 24, 30, 36, 42, 48]}
          - destination: "bulk_dissemination"
        nowcast_data:
          - filter: {type: "fc", step: [0, 3, 6]}
          - filter: {levtype: "sfc"}
          - destination: "lumi_workers"
  
  default_route:
    - destination: "bulk_dissemination"
```

### Complex Example - Multi-stage Processing

```yaml
queues:
  auth_service:
    type: "fifo"
    capacity: 1000
    workers: 4
    
  grib_processor:
    type: "fifo"
    capacity: 500
    
  quality_control:
    type: "priority"
    capacity: 200
    workers: 2
    
  format_converter:
    type: "fifo"
    capacity: 800
    
  lumi_hpc:
    type: "fair"
    capacity: 1500
    
  leonardo_hpc:
    type: "priority"
    capacity: 1200
    
  mare_nostrum_hpc:
    type: "fair"
    capacity: 1000

routes:
  operational_processing:
    - filter: {class: "od", stream: "oper"}
    - via: "auth_service"
    - switch:
        high_priority_forecast:
          - filter: {type: "fc", step: [0, 6, 12]}
          - filter: {levtype: "sfc", param: ["2t", "10u", "10v", "msl"]}
          - check: "urgent_processing"
          - via: "quality_control"
          - switch:
              lumi_processing:
                - filter: {area: "60/0/30/30"}  # Europe
                - destination: "lumi_hpc"
              leonardo_processing:
                - filter: {area: "45/-10/35/20"}  # Mediterranean
                - destination: "leonardo_hpc"
              mare_nostrum_processing:
                - filter: {area: "50/-15/25/5"}  # Iberian Peninsula
                - destination: "mare_nostrum_hpc"
        
        ensemble_processing:
          - filter: {stream: "enfo", type: "pf"}
          - filter: {number: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}
          - via: "grib_processor"
          - switch:
              statistical_processing:
                - filter: {param: ["tp", "2t", "10si"]}
                - via: "format_converter"
                - destination: "leonardo_hpc"
              probability_processing:
                - filter: {param: ["cape", "cin"]}
                - destination: "mare_nostrum_hpc"
        
        analysis_processing:
          - filter: {type: "an"}
          - switch:
              surface_analysis:
                - filter: {levtype: "sfc"}
                - via: "quality_control"
                - destination: "lumi_hpc"
              upper_air_analysis:
                - filter: {levtype: "pl"}
                - filter: {levelist: [1000, 925, 850, 700, 500, 400, 300, 250, 200, 150, 100, 70, 50, 30, 20, 10]}
                - via: "grib_processor"
                - destination: "leonardo_hpc"
              model_level_analysis:
                - filter: {levtype: "ml"}
                - filter: {levelist: [1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 91, 100, 110, 120, 130, 137]}
                - destination: "mare_nostrum_hpc"
  
  reanalysis_processing:
    - filter: {class: "ea"}  # ERA5
    - check: "has_reanalysis_access"
    - switch:
        hourly_processing:
          - filter: {type: "an"}
          - via: "grib_processor"
          - destination: "bulk_processing"
        monthly_processing:
          - filter: {type: "mnth"}
          - via: "format_converter"
          - destination: "leonardo_hpc"
  
  external_distribution:
    - filter: {class: "od", stream: "oper"}
    - check: "has_external_access"
    - destination: {url: "https://external-met-service.gov/ingest", method: "POST"}
  
  archive_route:
    - destination: {url: "https://archive.ecmwf.int/store", method: "PUT"}
```

## Rust Fluent API

### Basic Usage

```rust
use std::collections::HashMap;
use serde_json::json;

// Build the configuration programmatically
let config = routes![
    "operational_forecast" => route()
        .filter(json!({"class": "od", "stream": "oper", "type": "fc"}))
        .check("has_forecast_access")
        .via("auth_service")
        .switch(routes![
            "high_res_forecast" => route()
                .filter(json!({"levtype": "sfc", "param": ["2t", "10u", "10v", "tp"]}))
                .filter(json!({"step": [0, 6, 12, 18, 24]}))
                .destination("lumi_workers"),
            "pressure_levels" => route()
                .filter(json!({"levtype": "pl", "levelist": [1000, 850, 500, 250]}))
                .filter(json!({"param": ["t", "u", "v", "z", "r"]}))
                .destination("leonardo_workers"),
            "model_levels" => route()
                .filter(json!({"levtype": "ml"}))
                .via("forecast_processor")
                .destination("mare_nostrum_workers")
        ]),
    
    "analysis_data" => route()
        .filter(json!({"class": "od", "stream": "oper", "type": "an"}))
        .check("has_analysis_access")
        .via("auth_service")
        .switch(routes![
            "surface_analysis" => route()
                .filter(json!({"levtype": "sfc"}))
                .filter(json!({"param": ["msl", "2t", "2d", "10u", "10v"]}))
                .destination("lumi_workers"),
            "upper_air" => route()
                .filter(json!({"levtype": "pl"}))
                .filter(json!({"levelist": [1000, 925, 850, 700, 500, 400, 300, 250, 200, 150, 100]}))
                .via("analysis_processor")
                .destination("leonardo_workers")
        ]),
    
    "ensemble_forecast" => route()
        .filter(json!({"class": "od", "stream": "enfo", "type": "pf"}))
        .check("has_ensemble_access")
        .filter(json!({"number": [1, 2, 3, 4, 5]}))
        .via("forecast_processor")
        .switch(routes![
            "probabilistic_surface" => route()
                .filter(json!({"levtype": "sfc", "param": ["tp", "2t"]}))
                .destination("bulk_dissemination"),
            "probabilistic_upper" => route()
                .filter(json!({"levtype": "pl", "levelist": [500, 850]}))
                .destination("mare_nostrum_workers")
        ]),
    
    "default_route" => route()
        .destination("bulk_dissemination")
];

// Create queue configurations
let queues = HashMap::from([
    ("auth_service".to_string(), QueueConfig {
        queue_type: "fifo".to_string(),
        capacity: 1000,
        workers: Some(4),
    }),
    ("forecast_processor".to_string(), QueueConfig {
        queue_type: "fifo".to_string(),
        capacity: 500,
        workers: None, // External workers from LUMI
    }),
    ("analysis_processor".to_string(), QueueConfig {
        queue_type: "priority".to_string(),
        capacity: 800,
        workers: None, // External workers from Leonardo
    }),
    ("bulk_dissemination".to_string(), QueueConfig {
        queue_type: "fair".to_string(),
        capacity: 2000,
        workers: None,
    }),
]);

// Build the complete configuration
let bits_config = BitsConfig {
    queues,
    routes: config,
};
```

### Advanced Usage with EuroHPC Integration

```rust
// Complex routing for different HPC systems
let hpc_routing = routes![
    "lumi_processing" => route()
        .filter(json!({"class": "od", "stream": "oper", "type": "fc"}))
        .filter(json!({"levtype": "sfc", "param": ["2t", "10u", "10v"]}))
        .filter(json!({"area": "60/0/30/30"})) // Europe
        .check("has_lumi_access")
        .via("grib_processor")
        .destination("lumi_hpc"),
    
    "leonardo_processing" => route()
        .filter(json!({"class": "od", "stream": "oper"}))
        .filter(json!({"levtype": "pl", "levelist": [1000, 850, 500, 250]}))
        .filter(json!({"area": "45/-10/35/20"})) // Mediterranean
        .check("has_leonardo_access")
        .via("quality_control")
        .destination("leonardo_hpc"),
    
    "mare_nostrum_processing" => route()
        .filter(json!({"class": "od", "stream": "enfo", "type": "pf"}))
        .filter(json!({"number": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}))
        .filter(json!({"area": "50/-15/25/5"})) // Iberian Peninsula
        .check("has_mare_nostrum_access")
        .via("ensemble_processor")
        .destination("mare_nostrum_hpc"),
    
    "external_distribution" => route()
        .filter(json!({"class": "od", "stream": "oper"}))
        .check("has_external_access")
        .destination(DestinationTarget::Http {
            url: "https://external-met-service.gov/ingest".to_string(),
            method: Some("POST".to_string()),
            headers: Some(HashMap::from([
                ("Content-Type".to_string(), "application/grib".to_string()),
                ("X-Source".to_string(), "ECMWF".to_string()),
            ])),
        }),
    
    "archive_route" => route()
        .destination(DestinationTarget::Http {
            url: "https://archive.ecmwf.int/store".to_string(),
            method: Some("PUT".to_string()),
            headers: Some(HashMap::from([
                ("Content-Type".to_string(), "application/grib".to_string()),
                ("X-Archive-Policy".to_string(), "long-term".to_string()),
            ])),
        })
];
```

## Job Flow Examples

### Example 1: High-Resolution Forecast Processing

```
1. Job arrives: {class: "od", stream: "oper", type: "fc", levtype: "sfc", param: "2t", step: 6}
2. Matches "operational_forecast" route
3. Passes "has_forecast_access" check
4. Processed by "auth_service" queue (4 internal workers)
5. Enters nested switch, matches "high_res_forecast"
6. Sent to "lumi_workers" queue (external workers on LUMI HPC)
7. LUMI worker processes the 2-meter temperature forecast
8. Returns result to client
```

### Example 2: Ensemble Member Processing

```
1. Job arrives: {class: "od", stream: "enfo", type: "pf", number: 3, levtype: "sfc", param: "tp"}
2. Matches "ensemble_forecast" route
3. Passes "has_ensemble_access" check
4. Passes number filter (member 3 is in [1,2,3,4,5])
5. Processed by "forecast_processor" queue (external workers)
6. Enters nested switch, matches "probabilistic_surface"
7. Sent to "bulk_dissemination" queue for wide distribution
8. Returns processed ensemble precipitation data
```

### Example 3: Analysis Data for Upper Air

```
1. Job arrives: {class: "od", stream: "oper", type: "an", levtype: "pl", levelist: 500, param: "t"}
2. Matches "analysis_data" route
3. Passes "has_analysis_access" check
4. Processed by "auth_service" queue
5. Enters nested switch, matches "upper_air"
6. Processed by "analysis_processor" queue (priority queue)
7. Sent to "leonardo_workers" queue (external workers on Leonardo HPC)
8. Returns 500 hPa temperature analysis
```

This configuration demonstrates BITS handling real meteorological data flows across ECMWF's infrastructure, routing different types of weather data (forecasts, analyses, ensemble members) to appropriate EuroHPC computing resources based on data characteristics and processing requirements. 