mod rationer;
mod fjall_rationer;

use fjall_rationer::{FjallRationer};
use rationer::{Rationer, Pressure, Resources};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting Fjall Rationer...");
    
    // Create a new Fjall rationer
    let mut rationer = FjallRationer::new(
        "/tmp/fjall_rationer_db", 
        3,
        "client1".to_string()
    )?;
    
    // Set some pressure
    let mut pressure = Pressure::new();
    pressure.insert("gpu".to_string(), 0.5);
    pressure.insert("cpu".to_string(), 0.3);
    rationer.set_pressure(&pressure);
    
    // Send heartbeat
    rationer.heartbeat();
    
    println!("Fjall Rationer initialized successfully!");
    Ok(())
}
