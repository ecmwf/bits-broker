pub mod switch;

use crate::actions::Action;

/// An ordered pipeline branch with a human-readable name.
#[derive(Debug)]
pub struct Route {
    /// Route name used in diagnostics and configuration.
    pub name: String,
    /// Actions executed in order when this route is selected.
    pub actions: Vec<Action>,
}

impl Route {
    /// Creates a route from a name and ordered action list.
    pub fn new(name: String, actions: Vec<Action>) -> Self {
        Self { name, actions }
    }
}
