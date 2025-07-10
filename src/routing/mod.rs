pub mod actions;
pub mod switch;

use actions::Action;

pub struct Route {
    name: String,
    actions: Vec<Action>,
}

impl Route {
    pub fn new(name: String, actions: Vec<Action>) -> Self {
        Self { name, actions }
    }
}
