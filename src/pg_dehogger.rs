use postgres::{Client, NoTls};
use uuid::Uuid;
use crate::dehogger::{Pressure, Resources};

// ========================================
// === PostgreSQL Dehogger Implementation ===
// ========================================

// We need to be able to periodically write our presssure to the database.
// This should also include TTL of the peer. Maybe this is all peer state.

// Then we need to keep track of allocations probably in another table.

// ========================================
// === Data Structures ===
// ========================================

struct PgDehogger {
    db: Client,
    local_state: PgDehoggerPeerState,
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
    pub fn new(db: Client) -> Self {
        let mut s = Self { 
            db, 
            local_state: PgDehoggerPeerState::default()
        };
        s.create_tables().unwrap();
        s.heartbeat().unwrap();
        s
    }

    fn create_tables(&mut self) -> Result<(),()> {
        self.db.batch_execute(
            "CREATE TABLE IF NOT EXISTS peers (
                peer_id TEXT PRIMARY KEY,
                last_heartbeat TIMESTAMP NOT NULL,
                status TEXT DEFAULT 'active' CHECK (status IN ('active', 'inactive'))
            );
            
            CREATE TABLE IF NOT EXISTS pressures (
                peer_id TEXT,
                pressure JSONB NOT NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
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
                last_allocation_time TIMESTAMP NOT NULL DEFAULT NOW(),
                last_renegotiation_time TIMESTAMP NOT NULL DEFAULT NOW()
            );
            "
        ).map_err(|e| {
            eprintln!("Error creating tables: {}", e);
            ()
        })

        
    }

    /// Sync is designed to be called periodically from a thread. It will check if any renogitation is needed and try to
    /// claim or free resources from the central pool.
    pub fn sync(&mut self) -> Result<(),()> {

        // Create this peer in the peers table, and update the last_heartbeat.
        self.heartbeat()?;

        // Check if remote pressures have changed, and if so trigger a renogiation.

        // Renogiation takes the current list of pressures with a timestamp, computes stuff, then tries to push to allocations table

        // Check if our target has changed, and if we can transfer resources to/from the central pool.

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

    pub fn set_pressure(&mut self, pressure: &Pressure) -> Result<(),()> {
        if self.local_state.pressure != *pressure {
            self.local_state.pressure = pressure.clone();
            self.push_pressure().unwrap();
        }
        Ok(())
    }

    fn push_pressure(&mut self) -> Result<(),()> {
        let pressure_json = serde_json::to_value(&self.local_state.pressure).unwrap();
        self.db.execute(
            "INSERT INTO pressures (peer_id, pressure) VALUES ($1, $2::jsonb)
            ON CONFLICT (peer_id) DO UPDATE SET pressure = EXCLUDED.pressure",
            &[&self.local_state.peer_id, &pressure_json]
        ).map_err(|e| {
            eprintln!("Error pushing pressure: {}", e);
            ()
        })?;
        println!("Pushed pressure: {}", pressure_json);
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
        let mut dehogger = PgDehogger::new(db);

        let pressure = HashMap::from([("cpu".to_string(), 0.5), ("memory".to_string(), 0.5)]);

        dehogger.set_pressure(&pressure).unwrap();
        
        // // Now you can test with a real PostgreSQL instance
        // // The container will automatically be cleaned up when `node` goes out of scope
        println!("Successfully connected to PostgreSQL container");
    }
}