use std::{cmp::min, collections::HashMap, sync::Mutex};

use crate::dehogger::{DehoggerPeer, DehoggerPeerError, Pressure, Resources};
use postgres::Client;

pub struct PgDehoggerPeer {
    db: Client,
    peer_id: String,
    pressure: Pressure,
    target_reservation: Resources,
    reservation: Resources,
    allocation: Resources,
    allocation_lock: Mutex<()>,
}

impl DehoggerPeer for PgDehoggerPeer {
    fn set_pressure(&mut self, pressure: &Pressure) -> Result<(), DehoggerPeerError> {
        self.set_pressure(pressure)
    }

    fn allocate(&mut self, resources: &Resources) -> Result<bool, DehoggerPeerError> {
        self.allocate(resources)
    }

    fn free(&mut self, resources: &Resources) {
        self.free(resources)
    }

    fn sync(&mut self) -> Result<(), DehoggerPeerError> {
        self.sync()
    }
}


impl PgDehoggerPeer {

    pub fn new(db: Client, peer_id: String) -> Result<Self, DehoggerPeerError> {
        let mut s = Self {
            db,
            peer_id,
            pressure: HashMap::new(),
            target_reservation: HashMap::new(),
            reservation: HashMap::new(),
            allocation: HashMap::new(),
            allocation_lock: Mutex::new(()),
        };
        s.sync()?;
        Ok(s)
    }

    fn sync(&mut self) -> Result<(), DehoggerPeerError> {
        // tell the server we are still alive
        self.update_heartbeat()?;
        
        // this updates our target, which tells us if we need to reduce or increase our reservation
        self.get_target_reservation()?;

        // this tells us if the server has allocated more resources to us,
        // this function can only cause our reservation to INCREASE
        self.get_reservation()?;

        // now we tell the server if we can reduce our reservation (because our allocation has decreased)
        self.try_reduce_reservation()?;

        // finally tell the server our current allocation, which is just used for monitoring
        self.push_allocation()?;
        Ok(())
    }

    fn update_heartbeat(&mut self) -> Result<(), DehoggerPeerError> {
        self.db.execute(
            "INSERT INTO peers (peer_id, last_heartbeat) VALUES ($1, NOW())
            ON CONFLICT (peer_id) DO UPDATE SET last_heartbeat = NOW()",
            &[&self.peer_id]
        ).map_err(|e| {
            DehoggerPeerError::FatalError(format!("Could not create peer in database: {}", e.to_string()))
        })?;
        Ok(())
    }

    fn get_target_reservation(&mut self) -> Result<(), DehoggerPeerError> {
        let rows = self.db.query("SELECT target_reservation FROM reservations WHERE peer_id = $1", &[&self.peer_id]);
        match rows {
            Ok(rows) => {
                if rows.len() == 0 {
                    self.target_reservation = HashMap::new();
                } else if rows.len() > 1 {
                    return Err(DehoggerPeerError::FatalError(format!("Multiple target reservations found for peer {}", self.peer_id)))
                } else if let Some(row) = rows.get(0) {
                    let target_reservation: Resources = serde_json::from_value(row.get(0)).unwrap();
                    self.target_reservation = target_reservation.clone();
                }
                Ok(())
            }
            Err(e) => {
                return Err(DehoggerPeerError::FatalError(format!("Error getting target reservation: {}", e.to_string())))
            }
        }
    }

    fn get_reservation(&mut self) -> Result<(), DehoggerPeerError> {
        // This function can only cause our reservation to INCREASE.
        let rows = self.db.query("SELECT reservation FROM reservations WHERE peer_id = $1", &[&self.peer_id]);
        match rows {
            Ok(rows) => {
                if rows.len() == 0 {
                    // Do nothing, local peer state holds the truth
                } else if rows.len() > 1 {
                    return Err(DehoggerPeerError::FatalError(format!("Multiple reservations found for peer {}", self.peer_id)))
                } else if let Some(row) = rows.get(0) {
                    let reserved: Resources = serde_json::from_value(row.get(0)).unwrap();
                    for (resource, amount) in reserved {
                        if &amount < self.reservation.get(&resource).unwrap_or(&0) {
                            return Err(DehoggerPeerError::FatalError(format!(
                                "Server is trying to reduce our reservation below our current reservation: {}: {} < {}",
                                resource,
                                amount,
                                self.reservation.get(&resource).unwrap_or(&0)
                            )))
                        }
                        self.reservation.insert(resource.clone(), amount);
                    }
                }
                Ok(())
            }
            Err(e) => {
                return Err(DehoggerPeerError::FatalError(format!("Error getting reservation: {}", e.to_string())))
            }
        }
    }
    
    fn set_pressure(&mut self, pressure: &Pressure) -> Result<(), DehoggerPeerError> {
        if self.pressure != *pressure {
            self.pressure = pressure.clone();
            self.push_pressure()?;
        }
        Ok(())
    }

    fn allocate(&mut self, request: &Resources) -> Result<bool, DehoggerPeerError> {
        
        // Only one thread can increase allocation
        let _lock = self.allocation_lock.lock().unwrap();
        for (resource, amount) in request {
            
            let reservation = match self.reservation.get(resource) {
                Some(a) => a,
                None => {
                    // TODO: handle this better. should keep track of which types of resources are possibly available
                    // e.g. a peer should not be able to ask for a resource that will never exist
                    return Err(DehoggerPeerError::FatalError(format!("Resource {} not found in peer's reservation", resource)));
                }
            };

            let target_reservation = match self.target_reservation.get(resource) {
                Some(t) => t,
                None => {
                    // TODO: handle this better. should keep track of which types of resources are possibly available
                    // e.g. a peer should not be able to ask for a resource that will never exist
                    return Err(DehoggerPeerError::FatalError(format!("Resource {} not found in peer's target reservation", resource)));
                }
            };

            // We cannot allocate if we are above the current reservation, or if 
            // we are trying to reduce to a lower target reservation.
            let allocation_limit = min(target_reservation, reservation);
            let allocation = self.allocation.get(resource).unwrap_or(&0);
            if amount + *allocation > *allocation_limit {
                return Ok(false);
            }
        }

        // atomically increase the allocation for each resource
        for (resource, amount) in request {
            self.allocation.entry(resource.clone()).and_modify(|allocation| *allocation += amount);
        }
        Ok(true)
    }

    fn free(&mut self, resources: &Resources) {
        
        // atomically decrease the allocation for each resource
        for (resource, amount) in resources {
            self.allocation.entry(resource.clone()).and_modify(|allocation| *allocation -= amount);
        }

    }


    fn try_reduce_reservation(&mut self) -> Result<(), DehoggerPeerError> {

        // Acquire the allocation lock, because threads must not increase allocation while we are trying to reduce the reservation
        let _lock = self.allocation_lock.lock().unwrap();

        for (resource, amount) in self.allocation.clone() {
            let target_reservation = self.target_reservation.get(&resource).unwrap_or(&0);
            let reservation = self.reservation.get(&resource).unwrap_or(&0);

            // If we are already below the target reservation, do nothing.
            if reservation < target_reservation {
                continue;
            }

            let desired_reduction = reservation - target_reservation;
            let possible_reduction = reservation - amount;
            let reduction = min(desired_reduction, possible_reduction);

            // Atomically reduce the reservation
            self.reservation.entry(resource.clone()).and_modify(|a| *a -= reduction);
        };

        Ok(())
    }


    fn push_pressure(&mut self) -> Result<(), DehoggerPeerError> {
        
        
        // Always update the pressure
        let pressure_json = serde_json::to_value(&self.pressure).unwrap();
        self.db.execute(
            "INSERT INTO pressures (peer_id, pressure) VALUES ($1, $2::jsonb)
             ON CONFLICT (peer_id) DO UPDATE SET pressure = EXCLUDED.pressure",
            &[&self.peer_id, &pressure_json]
        ).map_err(|e| {
            DehoggerPeerError::FatalError(format!("Error pushing pressure: {}", e.to_string()))
        })?;
        
        // Try to update the latest pressure_changed_timestamp
        let new_time = chrono::Utc::now();
        
        let rows_updated = self.db.execute(
            "UPDATE global_state SET last_pressure_update_time = $1 WHERE last_pressure_update_time < $1",
            &[&new_time]
        ).map_err(|e| {
            DehoggerPeerError::FatalError(format!("Error updating global pressure timestamp: {}", e.to_string()))
        })?;
        
        if rows_updated > 0 {
            println!("Updated global pressure timestamp to: {:?}", new_time);
        } else {
            println!("Another peer has already pushed a newer pressure timestamp, this is fine.");
        }
        
        Ok(())
    }

    fn push_allocation(&mut self) -> Result<(), DehoggerPeerError> {

        let allocation_json = serde_json::to_value(&self.allocation).unwrap();
        self.db.execute(
            "INSERT INTO reservations (peer_id, allocation) VALUES ($1, $2::jsonb)
             ON CONFLICT (peer_id) DO UPDATE SET allocation = EXCLUDED.allocation",
            &[&self.peer_id, &allocation_json]
        ).map_err(|e| {
            DehoggerPeerError::FatalError(format!("Error pushing allocation: {}", e.to_string()))
        })?;
        Ok(())
    }
}