//!
//! This module defines RouteManager to dynamically fetch and refresh routes for each topic in use.
//!
use crate::error::ClientError;
use crate::protocol;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};

/// RouteManager maintains route entries for each topic.
pub(crate) struct RouteManager {
    /// Endpoints remain constant after construction.
    endpoints: Arc<RwLock<Vec<SocketAddr>>>,

    /// Topic routes are supposed to be refreshed after configured interval.
    topic_routes: Arc<Mutex<HashMap<String, Arc<protocol::TopicRouteData>>>>,
}

impl RouteManager {
    pub(crate) fn new(addrs: &str) -> Result<Self, ClientError> {
        let endpoints: Vec<_> = addrs
            .split(';')
            .flat_map(|addr| match addr.parse::<SocketAddr>() {
                Ok(socket_addr) => Some(socket_addr),
                Err(e) => {
                    eprintln!(
                        "Failed to parse name server address {}. Cause: {}",
                        addr,
                        e.to_string()
                    );
                    None
                }
            })
            .collect();
        Ok(Self {
            endpoints: Arc::new(RwLock::new(endpoints)),
            topic_routes: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub(crate) fn route(
        &mut self,
        topic: &str,
    ) -> Result<Option<Arc<protocol::TopicRouteData>>, ClientError> {
        {
            let guard = match self.topic_routes.lock() {
                Ok(map) => map,
                Err(e) => {
                    eprintln!("Lock is poisoned. Cause: {}", e.to_string());
                    return Err(ClientError::Unknown);
                }
            };

            match guard.get(topic) {
                Some(value) => return Ok(Some(Arc::clone(value))),
                None => {}
            };
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::RouteManager;

    #[test]
    fn test_route_manager_new() -> Result<(), Box<dyn std::error::Error>> {
        let addrs = "8.8.8.8:80;4.4.4.4.3:80";
        let manager = RouteManager::new(addrs)?;
        Ok(())
    }
}
