//!
//! Messaging are about publishing and subscribing messages. `Publisher` is the struct to utilize to deliver message to broker.
//!
use crate::message::Message;

struct Publisher {
    group: String,
}

impl Publisher {
    pub fn new(group: &str) -> Self {
        Publisher {
            group: group.to_owned(),
        }
    }

    pub fn publish(&mut self, message: &Message) {}
}
