use bytes;
use std::collections::HashMap;
use std::vec::Vec;

pub struct Message {
    /// In the publisher-subscriber model, a topic is an addresses where messages are delivered to and subscribed from.
    pub topic: String,

    pub tag: String,

    pub keys: Vec<String>,

    /// User defined attributes in form of key-value paris.
    /// Attributes are indexed by backend broker servers, thus, may be utilized to query.
    pub attributes: HashMap<String, String>,

    /// System properties
    ///
    /// System properties include key-value pairs to modify how messages are delivered to subscribers. For example, publishers
    /// may publish a timed message, which should be invisible to subscribers before specified time point.
    pub(crate) properties: HashMap<String, String>,

    pub body: bytes::Bytes,
}
