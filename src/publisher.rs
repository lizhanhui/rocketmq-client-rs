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
