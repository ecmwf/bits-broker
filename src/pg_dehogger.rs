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

#[derive(Debug, Clone)]
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
                last_heartbeat TIMESTAMP WITH TIME ZONE NOT NULL,
                status TEXT DEFAULT 'active' CHECK (status IN ('active', 'inactive'))
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
                total_resources JSONB NOT NULL
            );
            INSERT INTO global_state (id, last_renegotiation_time, last_pressure_update_time, total_resources) VALUES (1, NOW(), NOW(), '{}') ON CONFLICT (id) DO NOTHING;
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

        // Renogiation takes the current list of pressures with a timestamp, computes stuff, then tries to push to allocations table

        // Check if our target has changed, and if we can transfer resources to/from the central pool.

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
            return Ok(());
        }
        
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
        Ok(())
    }

    fn free(&mut self, resources: &Resources) {
        
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
        dehogger.renegotiate().unwrap();
        
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

        let pressure = HashMap::from([("cpu".to_string(), 0.5), ("memory".to_string(), 0.5)]);
        dehogger.set_pressure(&pressure).unwrap();
        dehogger2.set_pressure(&pressure).unwrap();

        dehogger.sync().unwrap();
        dehogger2.sync().unwrap();

        // allow one peer to expire
        std::thread::sleep(std::time::Duration::from_millis(600));
        dehogger.sync().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(600));
        dehogger.sync().unwrap(); // peer 1 should now have all the resources
    }
}