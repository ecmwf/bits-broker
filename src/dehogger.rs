use std::collections::HashMap;

pub type Pressure = HashMap<String, f64>;
pub type Resources = HashMap<String, i64>;


pub trait DehoggerPeer {

    /// Set the pressure on the peer. This is used to balance resources between peers. This is used to balance resources between peers.
    fn set_pressure(&mut self, pressure: &Pressure) -> Result<(),()>;

    /// Try to allocate from the allocated resources.
    fn allocate(&mut self, request: &Resources) -> Result<(),()>;

    /// Free the allocated resources.
    fn free(&mut self, resources: &Resources);

    fn sync(&mut self) -> Result<(),()>;
}

pub trait DehoggerManager {

    fn set_total_resources(&mut self, total_resources: &Resources) -> Result<(),()>;
    fn sync(&mut self) -> Result<(),()>;

}

