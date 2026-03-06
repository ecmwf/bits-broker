pub mod registry;
pub mod switch;

use crate::actions::Action;

#[derive(Debug)]
pub struct Route {
    pub name: String,
    pub actions: Vec<Action>,
}

impl Route {
    pub fn new(name: String, actions: Vec<Action>) -> Self {
        Self { name, actions }
    }
}
