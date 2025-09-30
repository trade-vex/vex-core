use crate::server::duologue::Duologue;
use crate::utils::{PortAllocator, SessionAllocator, send_message, send_message_with_retries};
use common::cmd::OrderCommand;
use dashmap::DashMap;
use disruptor::{MultiConsumerBarrier, MultiProducer};
use rusteron_client::{Aeron, AeronPublication};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, error, info};
use vex_config::CoreNetworkingConfig;

use super::ServerError;

/// Manages gateway connections and session lifecycle
///
/// Handles all gateway operations including handshake processing,
/// session allocation, resource management, and cleanup operations.
pub struct GatewayManager {
    /// Active gateway sessions mapped by session ID
    gateway_sessions: DashMap<i32, Duologue>,
    /// Gateway addresses mapped by session ID
    gateway_session_addresses: DashMap<i32, String>,
    /// Connection count per address for rate limiting
    address_connection_count: DashMap<String, AtomicU64>,
    /// Aeron messaging instance
    aeron: Arc<Aeron>,
    /// Core configuration
    config: CoreNetworkingConfig,
    /// Port allocator for gateway sessions
    port_allocator: PortAllocator,
    /// Session ID allocator
    session_allocator: SessionAllocator,
    /// Producer that sends commands to the disruptor ring
    producer: MultiProducer<OrderCommand, MultiConsumerBarrier>,
}

impl GatewayManager {
    /// Creates a new gateway manager
    pub fn new(
        config: CoreNetworkingConfig,
        aeron: Arc<Aeron>,
        producer: MultiProducer<OrderCommand, MultiConsumerBarrier>,
    ) -> Result<Self, ServerError> {
        Ok(Self {
            gateway_session_addresses: DashMap::new(),
            gateway_sessions: DashMap::new(),
            address_connection_count: DashMap::new(),
            aeron,
            port_allocator: PortAllocator::new(
                config.base_gateway_port,
                config.max_gateways.into(),
            )
            .map_err(|e| ServerError::ResourceAllocationError(e.to_string()))?,
            session_allocator: SessionAllocator::new(
                config.reserved_session_id_low,
                config.reserved_session_id_high,
            )
            .map_err(|e| ServerError::ResourceAllocationError(e.to_string()))?,
            config,
            producer,
        })
    }

    /// Returns is the gateway manager is empty
    pub fn is_empty(&self) -> bool {
        self.gateway_sessions.is_empty()
    }

    /// Checks if a gateway is currently connected
    pub fn is_gateway_connected(&self, gateway_id: &str) -> bool {
        self.gateway_sessions
            .iter()
            .any(|session| session.gateway_id == gateway_id)
    }

    /// Processes gateway handshake message
    ///
    /// Handles the complete handshake flow including message parsing,
    /// authentication, resource allocation, and session creation.
    pub fn process_handshake_message(
        &self,
        publication: &AeronPublication,
        session_id: i32,
        buffer: &[u8],
    ) -> Result<(), ServerError> {
        let message = std::str::from_utf8(buffer)
            .map_err(|e| ServerError::GatewayMessageError(format!("Invalid UTF-8: {e}")))?;

        debug!(
            "Processing handshake from session 0x{:x}: {}",
            session_id, message
        );

        // Parse "HELLO gateway_id encryption_key"
        let mut parts = message.split_whitespace();

        let hello = parts
            .next()
            .ok_or_else(|| ServerError::GatewayMessageError("Empty message".to_string()))?;
        if hello != "HELLO" {
            let error_msg = format!("{session_id} unknown REJECT Malformed HELLO message");
            send_message(publication, error_msg.as_bytes())?;
            return Err(ServerError::GatewayMessageError(
                "Malformed HELLO message".to_string(),
            ));
        }

        let gateway_id = parts
            .next()
            .ok_or_else(|| ServerError::GatewayMessageError("Missing gateway ID".to_string()))?;

        let encryption_key_str = parts.next().ok_or_else(|| {
            ServerError::GatewayMessageError("Missing encryption key".to_string())
        })?;

        let encryption_key = encryption_key_str.parse::<i32>().map_err(|e| {
            ServerError::GatewayMessageError(format!("Invalid encryption key: {e}"))
        })?;

        // Validate gateway ID
        if gateway_id.is_empty() {
            let error_msg = format!("{session_id} {gateway_id} REJECT Empty gateway ID");
            send_message(publication, error_msg.as_bytes())?;
            return Err(ServerError::GatewayMessageError(
                "Empty gateway ID".to_string(),
            ));
        }

        // Check various limits and constraints
        self.check_capacity_limits(publication, session_id, gateway_id)?;
        let gateway_address = match self.get_gateway_address(session_id) {
            Ok(address) => address,
            Err(e) => {
                let error_msg = format!("{session_id} {gateway_id} REJECT {e}");
                send_message(publication, error_msg.as_bytes())?;
                return Err(ServerError::GatewayMessageError(e.to_string()));
            }
        };
        self.check_address_limits(publication, session_id, gateway_id, &gateway_address)?;
        self.check_duplicate_connection(publication, session_id, gateway_id)?;

        // Authenticate if enabled
        if self.config.enable_authentication
            && let Err(e) = self.authenticate_gateway(gateway_id, &encryption_key.to_string())
        {
            let error_msg = format!("{session_id} {gateway_id} REJECT Authentication failed");
            send_message(publication, error_msg.as_bytes())?;
            return Err(e);
        }

        // Allocate resources and create session
        let (dedicated_session, ports) =
            self.allocate_gateway_session(session_id, gateway_id, &gateway_address)?;
        let encrypted_session = encryption_key ^ dedicated_session;

        // Send success response
        let accept_msg = format!(
            "{} {} ACCEPT {} {} {}",
            session_id, gateway_id, ports[0], ports[1], encrypted_session
        );
        match send_message_with_retries(publication, accept_msg.as_bytes()) {
            Ok(_) => (),
            Err(e) => {
                self.remove_gateway_session(session_id)?;
                return Err(ServerError::GatewayMessageError(format!(
                    "Failed to send ACCEPT message: {e}"
                )));
            }
        }
        info!(
            "Gateway '{}' connected successfully. Session: 0x{:x}, ports: {}, {}",
            gateway_id, dedicated_session, ports[0], ports[1]
        );

        Ok(())
    }

    /// Polls all active gateway sessions
    pub fn poll(&self) -> Result<(), ServerError> {
        let mut sessions_to_remove = Vec::new();

        for mut x in self.gateway_sessions.iter_mut() {
            let (initial_session_id, gateway_session) = x.pair_mut();

            if gateway_session.is_expired() || gateway_session.is_closed() {
                sessions_to_remove.push(*initial_session_id);
                continue;
            }

            if let Err(e) = gateway_session.poll() {
                error!(
                    "Error polling gateway session 0x{:x}: {}",
                    initial_session_id, e
                );
                sessions_to_remove.push(*initial_session_id);
            }
        }

        // Clean up terminated sessions
        for session_id in sessions_to_remove {
            self.remove_gateway_session(session_id)?;
        }

        Ok(())
    }

    /// Cleans up expired gateways
    pub fn cleanup_expired_gateways(&self) -> Result<usize, ServerError> {
        let expired_sessions: Vec<i32> = self
            .gateway_sessions
            .iter()
            .filter_map(|entry| {
                if entry.value().is_expired() {
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect();
        let count = expired_sessions.len();
        for session_id in expired_sessions {
            self.remove_gateway_session(session_id)?;
        }

        Ok(count)
    }

    /// Shuts down all gateway connections
    pub fn shutdown_all_gateways(&self) -> Result<(), ServerError> {
        let session_ids: Vec<i32> = self
            .gateway_sessions
            .iter()
            .map(|entry| *entry.key())
            .collect();

        for session_id in session_ids {
            self.remove_gateway_session(session_id)?;
        }

        info!("All gateway sessions shut down");
        Ok(())
    }

    /// Associates gateway address with session
    pub fn set_gateway_address(&self, session_id: i32, address: String) {
        self.gateway_session_addresses.insert(session_id, address);
    }

    /// Removes gateway address association
    pub fn remove_gateway_address(&self, session_id: i32) {
        self.gateway_session_addresses.remove(&session_id);
    }

    /// Returns Number of active gateways
    pub fn active_gateways_count(&self) -> usize {
        self.gateway_sessions.len()
    }

    // Private implementation methods

    fn check_capacity_limits(
        &self,
        publication: &AeronPublication,
        session_id: i32,
        gateway_id: &str,
    ) -> Result<(), ServerError> {
        if self.gateway_sessions.len() >= self.config.max_gateways as usize {
            let error_msg = format!("{session_id} {gateway_id} REJECT Core capacity exceeded");
            send_message(publication, error_msg.as_bytes())?;
            return Err(ServerError::CapacityExceededError(
                "Too many gateways connected".to_string(),
            ));
        }
        Ok(())
    }

    fn get_gateway_address(&self, session_id: i32) -> Result<String, ServerError> {
        self.gateway_session_addresses
            .get(&session_id)
            .map(|addr| addr.clone())
            .ok_or_else(|| {
                ServerError::GatewayMessageError("Gateway address not found".to_string())
            })
    }

    fn check_address_limits(
        &self,
        publication: &AeronPublication,
        session_id: i32,
        gateway_id: &str,
        gateway_address: &str,
    ) -> Result<(), ServerError> {
        if let Some(count_entry) = self.address_connection_count.get(gateway_address)
            && count_entry.load(Ordering::Relaxed) >= self.config.max_connections_per_address as u64
        {
            let error_msg =
                format!("{session_id} {gateway_id} REJECT Too many connections from address");
            send_message(publication, error_msg.as_bytes())?;
            return Err(ServerError::CapacityExceededError(
                "Too many connections from this address".to_string(),
            ));
        }

        Ok(())
    }

    fn check_duplicate_connection(
        &self,
        publication: &AeronPublication,
        session_id: i32,
        gateway_id: &str,
    ) -> Result<(), ServerError> {
        if self.is_gateway_connected(gateway_id) {
            let error_msg = format!("{session_id} {gateway_id} REJECT Gateway already connected");
            send_message(publication, error_msg.as_bytes())?;
            return Err(ServerError::GatewayMessageError(
                "Gateway already connected".to_string(),
            ));
        }
        Ok(())
    }

    fn allocate_gateway_session(
        &self,
        initial_session_id: i32,
        gateway_id: &str,
        gateway_address: &str,
    ) -> Result<(i32, [u16; 2]), ServerError> {
        // Update connection count
        let counter = self
            .address_connection_count
            .entry(gateway_address.to_string())
            .or_insert(AtomicU64::new(0));
        counter.fetch_add(1, Ordering::Relaxed);

        // Allocate resources
        let ports = match self.port_allocator.allocate(2) {
            Ok(p) => p,
            Err(e) => {
                counter.fetch_sub(1, Ordering::Relaxed);
                return Err(ServerError::ResourceAllocationError(e.to_string()));
            }
        };
        let dedicated_session = match self.session_allocator.allocate() {
            Ok(s) => s,
            Err(e) => {
                self.port_allocator.free(ports[0]);
                self.port_allocator.free(ports[1]);
                counter.fetch_sub(1, Ordering::Relaxed);
                return Err(ServerError::ResourceAllocationError(e.to_string()));
            }
        };

        // gateway session
        let gateway_session = match Duologue::new(
            &self.aeron,
            &self.config.local_address,
            gateway_id,
            gateway_address,
            ports[0],
            ports[1],
            dedicated_session,
            self.producer.clone(),
        ) {
            Ok(session) => session,
            Err(e) => {
                self.port_allocator.free(ports[0]);
                self.port_allocator.free(ports[1]);
                self.session_allocator.free(dedicated_session);
                counter.fetch_sub(1, Ordering::Relaxed);
                return Err(ServerError::ResourceAllocationError(format!(
                    "Failed to create Duologue: {e}"
                )));
            }
        };

        // Store session
        self.gateway_sessions
            .insert(initial_session_id, gateway_session);
        self.gateway_session_addresses
            .insert(initial_session_id, gateway_address.to_string());

        debug!(
            "Allocated session 0x{:x} for gateway '{}' with ports {}, {}",
            dedicated_session, gateway_id, ports[0], ports[1]
        );
        Ok((dedicated_session, [ports[0], ports[1]]))
    }

    fn authenticate_gateway(
        &self,
        gateway_id: &str,
        _credentials: &str,
    ) -> Result<(), ServerError> {
        // TODO: Implement proper authentication
        if gateway_id.len() < 3 || !gateway_id.starts_with("gateway-") {
            return Err(ServerError::AuthenticationError(
                "Invalid gateway ID format".to_string(),
            ));
        }
        info!("Gateway '{}' authenticated", gateway_id);
        Ok(())
    }

    fn remove_gateway_session(&self, session_id: i32) -> Result<(), ServerError> {
        if let Some((session_id, mut gateway_session)) = self.gateway_sessions.remove(&session_id) {
            info!(
                "Removing gateway session 0x{:x} for '{}'",
                session_id, gateway_session.gateway_id
            );

            gateway_session.close()?;

            // Free resources
            self.port_allocator.free(gateway_session.port_data);
            self.port_allocator.free(gateway_session.port_control);
            self.session_allocator.free(gateway_session.session_id);
            // Update connection count
            if let Some((_id, address)) = self.gateway_session_addresses.remove(&session_id)
                && let Some(count) = self.address_connection_count.get_mut(&address)
            {
                let _ = count.fetch_sub(1, Ordering::Relaxed);
                if count.load(Ordering::Relaxed) == 0 {
                    self.address_connection_count.remove(&address);
                }
            }
        }

        Ok(())
    }
}
