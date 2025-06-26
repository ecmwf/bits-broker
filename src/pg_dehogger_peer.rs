use std::{cmp::min, collections::HashMap, sync::Mutex};

use crate::dehogger::{DehoggerPeer, Pressure, Resources};
use postgres::Client;

pub struct PgDehoggerPeer {
    db: Client,
    peer_id: String,
    pressure: Pressure,
    target_allocation: Resources,
    actual_allocation: Resources,
    usage: Resources,
    usage_lock: Mutex<()>,
}

impl DehoggerPeer for PgDehoggerPeer {
    fn set_pressure(&mut self, pressure: &Pressure) -> Result<(),()> {
        self.set_pressure(pressure)
    }

    fn allocate(&mut self, resources: &Resources) -> Result<(),()> {
        self.allocate(resources)
    }

    fn free(&mut self, resources: &Resources) {
        self.free(resources)
    }

    fn sync(&mut self) -> Result<(),()> {
        self.sync()
    }
}


impl PgDehoggerPeer {

    pub fn new(db: Client, peer_id: String) -> Self {
        Self {
            db,
            peer_id,
            pressure: HashMap::new(),
            target_allocation: HashMap::new(),
            actual_allocation: HashMap::new(),
            usage: HashMap::new(),
            usage_lock: Mutex::new(()),
        }
    }

    fn sync(&mut self) -> Result<(),()> {
        self.update_heartbeat().unwrap();
        self.push_usage(&self.usage).unwrap();
        self.try_reduce_allocation().unwrap();
        Ok(())
    }

    fn set_pressure(&mut self, pressure: &Pressure) -> Result<(),()> {
        if self.pressure != *pressure {
            self.pressure = pressure.clone();
            self.push_pressure().unwrap();
        }
        Ok(())
    }

    fn allocate(&mut self, request: &Resources) -> Result<bool,()> {
        
        // Only one thread can increase usage
        let _lock = self.usage_lock.lock().unwrap();
        for (resource, amount) in request {
            
            let allocation = match self.actual_allocation.get(resource) {
                Some(a) => a,
                None => {
                    println!("Resource {} not found in peer's allocation", resource);
                    return Err(());
                }
            };

            let target_allocation = match self.target_allocation.get(resource) {
                Some(t) => t,
                None => {
                    println!("Resource {} not found in peer's target allocation", resource);
                    return Err(());
                }
            };

            // We cannot allocate usage if we are above the current allocation, or if 
            // we are trying to reduce to a lower target allocation.
            let allocation_limit = min(target_allocation, allocation);
            let usage = self.usage.get(resource).unwrap_or(&0);
            if amount + *usage > *allocation_limit {
                return Ok(false);
            }
        }

        // atomically increase the usage for each resource
        for (resource, amount) in request {
            self.usage.entry(resource.clone()).and_modify(|usage| *usage += amount);
        }
        Ok(true)
    }

    fn free(&mut self, resources: &Resources) {
        
        // atomically decrease the usage for each resource
        for (resource, amount) in resources {
            self.usage.entry(resource.clone()).and_modify(|usage| *usage -= amount);
        }

    }

    fn update_heartbeat(&mut self) -> Result<(),()> {
        self.db.execute(
            "INSERT INTO peers (peer_id, last_heartbeat) VALUES ($1, NOW())
            ON CONFLICT (peer_id) DO UPDATE SET last_heartbeat = NOW()",
            &[&self.peer_id]
        ).map_err(|e| {
            eprintln!("Error creating peer: {}", e);
            ()
        })?;
        Ok(())
    }

    fn try_reduce_allocation(&mut self) -> Result<(),()> {

        // Acquire the usage lock, because threads must not increase usage while we are trying to reduce the allocation
        let _lock = self.usage_lock.lock().unwrap();

        for (resource, amount) in self.usage.clone() {
            let target_allocation = self.target_allocation.get(&resource).unwrap_or(&0);
            let allocation = self.actual_allocation.get(&resource).unwrap_or(&0);

            // If we are already below the target allocation, do nothing.
            if allocation < target_allocation {
                continue;
            }

            let desired_reduction = allocation - target_allocation;
            let possible_reduction = allocation - amount;
            let reduction = min(desired_reduction, possible_reduction);

            // Atomically reduce the allocation
            self.actual_allocation.entry(resource.clone()).and_modify(|a| *a -= reduction);
        };

        Ok(())


        
        // if the target_allocation is lower than our current allocation, we should try to reduce it.

        // If the total usage is less than the total allocation, we can reduce the allocation


    }


    fn push_pressure(&mut self) -> Result<(),()> {
        
        
        // Always update the pressure
        let pressure_json = serde_json::to_value(&self.local_state.pressure).unwrap();
        self.db.execute(
            "INSERT INTO pressures (peer_id, pressure) VALUES ($1, $2::jsonb)
             ON CONFLICT (peer_id) DO UPDATE SET pressure = EXCLUDED.pressure",
            &[&self.local_state.peer_id, &pressure_json]
        ).map_err(|e| {
            eprintln!("Error pushing pressure: {}", e);
            ()
        })?;
        
        // Try to update the latest pressure_changed_timestamp
        let new_time = chrono::Utc::now();
        
        let rows_updated = self.db.execute(
            "UPDATE global_state SET last_pressure_update_time = $1 WHERE last_pressure_update_time < $1",
            &[&new_time]
        ).map_err(|e| {
            eprintln!("Something went wrong pushing pressure: {}", e);
            ()
        })?;
        
        if rows_updated > 0 {
            println!("Updated global pressure timestamp to: {:?}", new_time);
        } else {
            println!("Another peer has already pushed a newer pressure timestamp");
        }
        
        Ok(())
    }

    fn push_usage(&mut self, resources: &Resources) -> Result<(),()> {

        let usage_json = serde_json::to_value(&self.usage).unwrap();
        self.db.execute(
            "INSERT INTO allocations (peer_id, usage) VALUES ($1, $2::jsonb)
             ON CONFLICT (peer_id) DO UPDATE SET usage = EXCLUDED.usage",
            &[&self.peer_id, &usage_json]
        ).map_err(|e| {
            eprintln!("Error pushing usage: {}", e);
            ()
        })?;
        Ok(())
    }
}