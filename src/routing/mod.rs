pub mod registry;
pub mod switch;

use crate::actions::Action;

#[derive(Debug)]
pub struct Pipeline {
    pub name: String,
    pub actions: Vec<Action>,
}

impl Pipeline {
    pub fn new(name: String, actions: Vec<Action>) -> Self {
        Self { name, actions }
    }
}
