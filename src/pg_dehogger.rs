use std::{cmp::max, collections::HashMap, sync::Mutex};

use chrono::{DateTime, Utc};
use postgres::{Client, NoTls};
use uuid::Uuid;
use crate::dehogger::{Dehogger, Pressure, Resources};

// ========================================
// === PostgreSQL Dehogger Implementation ===
// ========================================

// We need to be able to periodically write our presssure to the database.
// This should also include TTL of the peer. Maybe this is all peer state.

// Then we need to keep track of allocations probably in another table.

// ========================================
// === Data Structures ===
// ========================================

pub struct PgDehogger {
    db: Client,
    local_state: PgDehoggerPeerState,
    ttl: u64,
}

#[derive(Debug)]
struct PgDehoggerPeerState {
    peer_id: String,
    pressure: Pressure,
    target_allocation: Resources,
    actual_allocation: Resources,
    usage: Resources,
}

// ========================================
// === Peer State Implementation ===
// ========================================

impl Default for PgDehoggerPeerState {
    fn default() -> Self {
        Self {
            peer_id: Uuid::new_v4().to_string(),
            pressure: Pressure::default(),
            target_allocation: Resources::default(),
            actual_allocation: Resources::default(),
            usage: Resources::default(),
        }
    }
}

// ========================================
// === Dehogger Implementation ===
// ========================================

impl PgDehogger {
    pub fn new(db: Client, ttl: u64) -> Self {
        let mut s = Self { 
            db, 
            local_state: PgDehoggerPeerState::default(),
            ttl,
        };
        s.create_tables().unwrap();
        s.heartbeat().unwrap();
        s
    }

    fn create_tables(&mut self) -> Result<(),()> {
        self.db.batch_execute(
            "CREATE TABLE IF NOT EXISTS peers (
                peer_id TEXT PRIMARY KEY,
                last_heartbeat TIMESTAMP WITH TIME ZONE NOT NULL
            );
            
            CREATE TABLE IF NOT EXISTS pressures (
                peer_id TEXT,
                pressure JSONB NOT NULL,
                PRIMARY KEY (peer_id),
                FOREIGN KEY (peer_id) REFERENCES peers(peer_id) ON DELETE CASCADE
            );
            
            CREATE TABLE IF NOT EXISTS allocations (
                peer_id TEXT,
                target_allocation JSONB NOT NULL,
                actual_allocation JSONB NOT NULL,
                usage JSONB NOT NULL,
                PRIMARY KEY (peer_id),
                FOREIGN KEY (peer_id) REFERENCES peers(peer_id) ON DELETE CASCADE
            );
            
            CREATE TABLE IF NOT EXISTS global_state (
                id INTEGER PRIMARY KEY DEFAULT 1,
                last_allocation_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                last_renegotiation_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                last_pressure_update_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                last_usage_update_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                total_resources JSONB NOT NULL
            );
            INSERT INTO global_state (id, last_renegotiation_time, last_pressure_update_time, last_usage_update_time, total_resources) VALUES (1, NOW(), NOW(), NOW(), '{}') ON CONFLICT (id) DO NOTHING;
            "
        ).map_err(|e| {
            eprintln!("Error creating tables: {}", e);
            ()
        })?;

        Ok(())
    }


    pub fn set_total_resources(&mut self, total_resources: &Resources) -> Result<(),()> {

        let total_resources_json = serde_json::to_value(total_resources).unwrap();
        self.db.execute(
            "UPDATE global_state SET total_resources = $1",
            &[&total_resources_json]
        ).map_err(|e| {
            eprintln!("Error setting total resources: {}", e);
            ()
        })?;
        Ok(())
    }

    /// Sync is designed to be called periodically from a thread. It will check if any renogitation is needed and try to
    /// claim or free resources from the central pool.
    fn sync(&mut self) -> Result<(),()> {

        // Create this peer in the peers table, and update the last_heartbeat.
        self.heartbeat()?;

        // Check if remote pressures have changed, and if so trigger a renogiation.
        self.renegotiate()?;

        // Push our current usage state to the database.
        self.push_usage_state()?;

        // Check if our target has changed, and if we can transfer resources to/from the central pool.
        self.reallocation()?;

        Ok(())

    }


    fn renegotiate(&mut self) -> Result<(),()> {

        println!("=== Renegotiating ===");
        
        // Check the global last_pressure_update_time
        let row = self.db.query_one("SELECT last_pressure_update_time FROM global_state", &[]).unwrap();
        let mut last_pressure_update: DateTime<Utc> = row.get(0);

        println!("Last pressure update:     {:?}", last_pressure_update);

        let row = self.db.query_one("SELECT last_renegotiation_time FROM global_state", &[]).unwrap();
        let last_renegotiation_time: DateTime<Utc> = row.get(0);
        println!("Last renegotiation time:  {:?}", last_renegotiation_time);

        // Check if any peers have expired, by checking all of their last_heartbeats. Delete any older than TTL.
        let peers_deleted = self.db.execute(
            &format!("DELETE FROM peers WHERE last_heartbeat < NOW() - INTERVAL '{} seconds'", self.ttl),
            &[]
        ).map_err(|e| {
            eprintln!("Error deleting expired peers: {}", e);
            ()
        })?;

        println!("Peers deleted: {:?}", peers_deleted);

        // Update global pressure timestamp if peers were deleted
        if peers_deleted > 0 {

            let new_time = chrono::Utc::now();
            let rows_updated = self.db.execute(
                "UPDATE global_state SET last_pressure_update_time = $1 WHERE last_pressure_update_time < $1",
                &[&new_time]
            ).map_err(|e| {
                eprintln!("Error updating pressure timestamp: {}", e);
                ()
            })?;
            
            if rows_updated > 0 {
                println!("Updated global pressure timestamp to: {:?}", new_time);
                last_pressure_update = new_time; // update our local copy
            }
        }

        // We need to renogiate if the last pressure update is newer than the last renegotiation, or if any peers have expired.
        if last_pressure_update > last_renegotiation_time || peers_deleted > 0 {
            println!("Renegotiating");
        } else {
            println!("No renegotiation needed");
            return Ok(());
        }

        // Do renegotiation

        // Get all pressures from the pressures table.
        let rows = self.db.query("SELECT peer_id, pressure FROM pressures", &[]).unwrap();
        let pressures: Vec<(String, Pressure)> = rows.iter().map(|row| {
            let peer_id: String = row.get(0);
            let pressure: Pressure = serde_json::from_value(row.get(1)).unwrap();
            (peer_id, pressure)
        }).collect();
        println!("Pressures: {:?}", pressures);

        let mut total_pressure = Pressure::new();
        for (_, pressure) in pressures.iter() {
            for (resource, pressure) in pressure.iter() {
                total_pressure.insert(resource.clone(), total_pressure.get(resource).unwrap_or(&0.0) + pressure);
            }
        }
        println!("Total pressures: {:?}", total_pressure);

        // Get the total resources from the global_state table.
        let row = self.db.query_one("SELECT total_resources FROM global_state", &[]).unwrap();
        let total_resources: Resources = serde_json::from_value(row.get(0)).unwrap();
        println!("Total resources: {:?}", total_resources);


        // Build the target_allocations for each peer.
        // TODO: Need to fix rounding error which will overallocate.
        let target_allocations = pressures.iter().map(|(peer_id, pressure)| {
            let mut target_allocation = Resources::new();
            for (resource, pressure) in pressure.iter() {
                let total_pressure_val = *total_pressure.get(resource).unwrap();
                let total_resources_val = *total_resources.get(resource).unwrap();
                // Avoid division by zero
                let allocation = if total_pressure_val > 0.0 {
                    ((*pressure / total_pressure_val) * (total_resources_val as f64)).round() as i64
                } else {
                    0
                };
                target_allocation.insert(resource.clone(), allocation);
            }
            println!("Target allocation for {}: {:?}", peer_id, target_allocation);
            (peer_id.clone(), target_allocation)
        }).collect::<Vec<(String, Resources)>>();

        // Update database in a transaction
        let mut transaction = self.db.transaction().unwrap();
        
        // Check if renegotiation is still needed (another process might have updated it)
        let row = transaction.query_one("SELECT last_renegotiation_time FROM global_state", &[]).unwrap();
        let last_renegotiation_time: DateTime<Utc> = row.get(0);

        // Issue in here in casee of pressure being deleted, the last_updated pressure timestamp doesn't change.
        
        if last_renegotiation_time >= last_pressure_update {
            // Another process already renegotiated, cancel this transaction
            transaction.rollback().unwrap();
            println!("Renegotiation already done by another process at {:?}, last pressure update at {:?}", last_renegotiation_time, last_pressure_update);
        } else {
            // Update all target allocations
            for (peer_id, target_allocation) in target_allocations {
                let target_json = serde_json::to_value(&target_allocation).unwrap();
                transaction.execute(
                    "INSERT INTO allocations (peer_id, target_allocation, actual_allocation, usage) VALUES ($1, $2, '{}', '{}')
                     ON CONFLICT (peer_id) DO UPDATE SET target_allocation = $2",
                    &[&peer_id, &target_json]
                ).unwrap();
            }
    
            // Update the last_renegotiation_time to the last_updated pressure time.
            transaction.execute(
                "UPDATE global_state SET last_renegotiation_time = $1",
                &[&last_pressure_update]
            ).unwrap();
            
            // Commit the transaction
            transaction.commit().unwrap();
            println!("Renegotiation done");
        }
        

        // === Reallocation ===
        Ok(())
    }


    fn push_usage_state(&mut self) -> Result<(),()> {
        // set our current local state target allocations (get by peer_id)
        // any of these self-queries can fail if we got disconnected from the database and wiped out because of TTL.
        let row = self.db.query_one("SELECT target_allocation, actual_allocation FROM allocations WHERE peer_id = $1", &[&self.local_state.peer_id]).unwrap();
        self.local_state.target_allocation = serde_json::from_value(row.get(0)).unwrap();
        self.local_state.actual_allocation = serde_json::from_value(row.get(1)).unwrap();

        // from this moment, our usage cannot increase beyond the new target allocation

        // push our current usage, which is guaranteed to be <= the new target allocation OR <= old usage
        // the usage table is sometimes locked by the reallocation process, so this can block momentarily.
        // but we don't prevent the peer from allocating resources in this time.
        self.push_usage().unwrap();

        // Each peer is responsible for reducing their own allocation
        for (resource, usage) in self.local_state.usage.iter() {
            let target_allocation = self.local_state.target_allocation.get(resource).unwrap();
            if usage > target_allocation {
                // note this isn't a race condition because usage cannot increase if we are above the target allocation.
                // the usage can only have decreased since the comparison in the line above this comment.
                let new_allocation = max(*usage, *target_allocation);
                self.local_state.actual_allocation.insert(resource.clone(), new_allocation);
            }
        }

        // here we are pushing an empty dict
        
        self.db.execute(
            "UPDATE allocations SET actual_allocation = $1 WHERE peer_id = $2",
            &[&serde_json::to_value(&self.local_state.actual_allocation).unwrap(), &self.local_state.peer_id]
        ).unwrap();

        // Update the global timestamp only if the new time is newer than current
        self.db.execute(
            "UPDATE global_state SET last_usage_update_time = NOW() WHERE last_usage_update_time < NOW()",
            &[]
        ).unwrap();

        Ok(())
    }

    fn reallocation(&mut self) -> Result<(),()> {

        println!("=== Reallocating ===");

        // Get the last usage update time, this is the timestamp we are working with.
        let last_usage_update_time = self.db.query_one("SELECT last_usage_update_time FROM global_state", &[]).unwrap();
        let last_usage_update_time: DateTime<Utc> = last_usage_update_time.get(0);
        println!("Last usage update time: {:?}", last_usage_update_time);

        // === Group Allocation ===

        // The group allocation process is for increasing the allocation of the peers that are under-allocated.


        // Get the current allocations
        let actual_allocations = self.db.query("SELECT peer_id, actual_allocation FROM allocations", &[]).unwrap();
        let actual_allocations: HashMap<String, Resources> = actual_allocations.iter().map(|row| {
            let peer_id: String = row.get(0);
            let allocation: Resources = serde_json::from_value(row.get(1)).unwrap();
            (peer_id, allocation)
        }).collect();
        println!("Actual allocations: {:?}", actual_allocations);

        // Get the target allocations
        let target_allocations = self.db.query("SELECT peer_id, target_allocation FROM allocations", &[]).unwrap();
        let target_allocations: HashMap<String, Resources> = target_allocations.iter().map(|row| {
            let peer_id: String = row.get(0);
            let allocation: Resources = serde_json::from_value(row.get(1)).unwrap();
            (peer_id, allocation)
        }).collect();
        println!("Target allocations: {:?}", target_allocations);


        // Calculate which peers are under-allocated and by how much
        let mut allocation_deficits = HashMap::<String, Resources>::new();
        for (peer_id, allocation) in &actual_allocations {
            let target_allocation = target_allocations.get(&peer_id.clone()).unwrap();
            let mut deficit = Resources::new();
            for (resource, target_allocation) in target_allocation.iter() {
                let actual_allocation = allocation.get(resource).unwrap_or(&0);
                deficit.insert(resource.clone(), *target_allocation - *actual_allocation);
            }
            allocation_deficits.insert(peer_id.clone(), deficit);
        }
        println!("Allocation deficits: {:?}", allocation_deficits);

        // Need to calculate the proportional deficit for each peer for each resource
        // calculate the total deficit for each resource
        let mut total_resource_deficits = HashMap::<String, i64>::new();
        for (_, deficit) in &allocation_deficits {
            for (resource, deficit_amount) in deficit {
                if *deficit_amount > 0 {
                    *total_resource_deficits.entry(resource.clone()).or_insert(0) += deficit_amount;
                }
            }
        }
        println!("Total resource deficits: {:?}", total_resource_deficits);

        // Get the total resources
        let total_resources = self.db.query_one("SELECT total_resources FROM global_state", &[]).unwrap();
        let total_resources: Resources = serde_json::from_value(total_resources.get(0)).unwrap();
        println!("Total resources: {:?}", total_resources);

        // Calculate the total free resources (total - actual allocations)
        let mut total_free_resources = Resources::new();
        for (resource, total_resource) in total_resources.iter() {
            let mut total_free_resource = *total_resource;
            for (_, allocation) in actual_allocations.iter() {
                total_free_resource -= allocation.get(resource).unwrap_or(&0);
            }
            total_free_resources.insert(resource.clone(), total_free_resource);
        }
        println!("Total free resources: {:?}", total_free_resources);

        // Calculate the proportional deficit for each peer, multiplied by the free resources
        // only count positive deficits
        let proportional_change = allocation_deficits.iter().map(|(peer_id, deficit)| {
            let mut proportional_change = Resources::new();
            for (resource, deficit_amount) in deficit {
                let total_deficit = *total_resource_deficits.get(resource).unwrap_or(&0);
                if *deficit_amount > 0 && total_deficit > 0 {
                    let proportion: f64 = (*deficit_amount as f64) / (total_deficit as f64);
                    let free_resources = *total_free_resources.get(resource).unwrap_or(&0) as f64;
                    proportional_change.insert(resource.clone(), (proportion * free_resources) as i64);
                } else {
                    proportional_change.insert(resource.clone(), 0);
                }
            }
            (peer_id.clone(), proportional_change)
        }).collect::<HashMap<String, Resources>>();
        println!("Proportional change: {:?}", proportional_change);

        // New allocations which is current allocation + proportional change
        let mut new_allocations = HashMap::<String, Resources>::new();
        for (peer_id, proportional_change) in proportional_change {
            let current_allocation = actual_allocations.get(&peer_id).unwrap();
            let mut new_allocation = current_allocation.clone();
            for (resource, change) in proportional_change {
                new_allocation.insert(resource.clone(), current_allocation.get(&resource).unwrap_or(&0) + change);
            }
            new_allocations.insert(peer_id, new_allocation);
        }
        println!("New allocations: {:?}", new_allocations);

        // do a transaction, if the stored last_usage_time >= last_allocation_time, then we can update the allocations
        let mut transaction = self.db.transaction().unwrap();
        let row = transaction.query_one("SELECT last_allocation_time FROM global_state", &[]).unwrap();
        let last_allocation_time: DateTime<Utc> = row.get(0);
        if last_usage_update_time <= last_allocation_time {
            println!("Another process already updated the allocations, cancel this transaction");
            transaction.rollback().unwrap();
        } else {

            // Update the allocations, insert if not exists
            for (peer_id, new_allocation) in new_allocations {
                let new_allocation_json = serde_json::to_value(&new_allocation).unwrap();
                transaction.execute(
                    "INSERT INTO allocations (peer_id, actual_allocation, target_allocation, usage) VALUES ($1, $2, '{}', '{}')
                     ON CONFLICT (peer_id) DO UPDATE SET actual_allocation = $2",
                    &[&peer_id, &new_allocation_json]
                ).unwrap();
            }

            // Update the last_allocation_time
            transaction.execute(
                "UPDATE global_state SET last_allocation_time = $1",
                &[&last_usage_update_time]
            ).unwrap();
            
            // Commit the transaction   
            transaction.commit().unwrap();
            println!("Allocations updated");
        }

        Ok(())
    }

    fn push_usage(&mut self) -> Result<(),()> {
        let usage_json = serde_json::to_value(&self.local_state.usage).unwrap();
        self.db.execute(
            "INSERT INTO allocations (peer_id, usage, actual_allocation, target_allocation) VALUES ($1, $2::jsonb, '{}', '{}')
            ON CONFLICT (peer_id) DO UPDATE SET usage = EXCLUDED.usage",
            &[&self.local_state.peer_id, &usage_json]
        ).map_err(|e| {
            eprintln!("Error pushing usage: {}", e);
            ()
        })?;

        // Update global timestamp only if the new time is newer than current
        let new_time = chrono::Utc::now();
        let rows_updated = self.db.execute(
            "UPDATE global_state SET last_usage_update_time = $1 WHERE last_usage_update_time < $1",
            &[&new_time]
        ).map_err(|e| {
            eprintln!("Error updating usage timestamp: {}", e);
            ()
        })?;

        if rows_updated > 0 {
            println!("Updated global usage timestamp to: {:?}", new_time);
        }
        Ok(())
    }

    fn heartbeat(&mut self) -> Result<(),()> {
        self.db.execute(
            "INSERT INTO peers (peer_id, last_heartbeat) VALUES ($1, NOW())
            ON CONFLICT (peer_id) DO UPDATE SET last_heartbeat = NOW()",
            &[&self.local_state.peer_id]
        ).map_err(|e| {
            eprintln!("Error creating peer: {}", e);
            ()
        })?;
        println!("Created/updated peer: {}", self.local_state.peer_id);
        Ok(())
    }


    fn push_pressure(&mut self) -> Result<(),()> {
        let pressure_json = serde_json::to_value(&self.local_state.pressure).unwrap();
        
        // Always update the pressure
        self.db.execute(
            "INSERT INTO pressures (peer_id, pressure) VALUES ($1, $2::jsonb)
             ON CONFLICT (peer_id) DO UPDATE SET pressure = EXCLUDED.pressure",
            &[&self.local_state.peer_id, &pressure_json]
        ).map_err(|e| {
            eprintln!("Error pushing pressure: {}", e);
            ()
        })?;
        
        // Use atomic UPDATE for compare-and-store of global timestamp
        let new_time = chrono::Utc::now();
        
        // Update global timestamp only if the new time is newer than current
        let rows_updated = self.db.execute(
            "UPDATE global_state SET last_pressure_update_time = $1 WHERE last_pressure_update_time < $1",
            &[&new_time]
        ).map_err(|e| {
            eprintln!("Error updating pressure timestamp: {}", e);
            ()
        })?;
        
        if rows_updated > 0 {
            println!("Updated global pressure timestamp to: {:?}", new_time);
        }
        
        Ok(())
    }
}

impl Dehogger for PgDehogger {
    fn set_pressure(&mut self, pressure: &Pressure) -> Result<(),()> {
        if self.local_state.pressure != *pressure {
            self.local_state.pressure = pressure.clone();
            self.push_pressure().unwrap();
        }
        Ok(())
    }

    fn allocate(&mut self, resources: &Resources) -> Result<(),()> {

        todo!()
    }

    fn free(&mut self, resources: &Resources) {
        todo!()
    }

    fn sync(&mut self) -> Result<(),()> {
        self.sync()?;
        Ok(())
    }
}

// ========================================
// === Tests ===
// ========================================

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use testcontainers_modules::{postgres, testcontainers::runners::SyncRunner};

    #[test]
    fn test_pg_dehogger() {

        let container = postgres::Postgres::default().start().unwrap();

        let connection_string = format!(
            "postgresql://postgres:postgres@{}:{}/postgres",
            container.get_host().unwrap(),
            container.get_host_port_ipv4(5432).unwrap()
        );

        
        // Connect to the database
        let db = Client::connect(&connection_string, NoTls).unwrap();
        println!("Successfully connected to PostgreSQL container");
        
        let mut dehogger = PgDehogger::new(db, 1);

        let mut total_resources = Resources::new();
        total_resources.insert("cpu".to_string(), 100);
        total_resources.insert("memory".to_string(), 100);
        dehogger.set_total_resources(&total_resources).unwrap();

        println!("The time now is: {:?}", chrono::Utc::now().naive_utc());

        let pressure = HashMap::from([("cpu".to_string(), 0.5), ("memory".to_string(), 0.5)]);

        dehogger.set_pressure(&pressure).unwrap();

        // renogiate should show that no renegotiation is needed
        dehogger.renegotiate().unwrap();

        // Sleep causes this client to time out
        std::thread::sleep(std::time::Duration::from_millis(1200));

        // now renogiate should show that a peer is deleted
        // dehogger.renegotiate().unwrap();
        
        // dehogger.sync().unwrap();
        // dehogger.sync().unwrap();
        
    }

    #[test]
    fn test_two_peers() {
        let container = postgres::Postgres::default().start().unwrap();
        let connection_string = format!(
            "postgresql://postgres:postgres@{}:{}/postgres",
            container.get_host().unwrap(),
            container.get_host_port_ipv4(5432).unwrap()
        );

        let db1 = Client::connect(&connection_string, NoTls).unwrap();
        let mut dehogger = PgDehogger::new(db1, 1);

        let mut total_resources = Resources::new();
        total_resources.insert("cpu".to_string(), 100);
        total_resources.insert("memory".to_string(), 100);
        dehogger.set_total_resources(&total_resources).unwrap();

        let db2 = Client::connect(&connection_string, NoTls).unwrap();
        let mut dehogger2 = PgDehogger::new(db2, 1);

        let pressure1 = HashMap::from([("cpu".to_string(), 0.5), ("memory".to_string(), 0.5)]);
        let pressure2 = HashMap::from([("cpu".to_string(), 3.0), ("memory".to_string(), 0.5)]);
        dehogger.set_pressure(&pressure1).unwrap();
        dehogger2.set_pressure(&pressure2).unwrap();

        dehogger.sync().unwrap();
        dehogger2.sync().unwrap();
        dehogger.sync().unwrap();

        // allow one peer to expire
        // std::thread::sleep(std::time::Duration::from_millis(600));
        // dehogger.sync().unwrap();
        // std::thread::sleep(std::time::Duration::from_millis(600));
        // dehogger.sync().unwrap(); // peer 1 should now have all the resources
    }
}