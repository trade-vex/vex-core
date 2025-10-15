use crate::server::duologue::{
    DUOLOGUE_STREAM_ID, Duologue, DuologueImageAvailable, DuologueImageUnavailable,
};
use crate::server::gateway_publications::Publications;
use crate::utils::{
    PortAllocator, SessionAllocator, new_publication_with_mdc_and_session,
    new_subsciption_with_handlers_and_session, send_message, send_message_with_retries,
};
use common::{MAX_GATEWAYS, OrderCommand};
use disruptor::{MultiProducer, SingleConsumerBarrier};
use rusteron_archive::{Aeron, AeronPublication, Handler};
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::sync::{Arc, RwLock};
use tracing::{debug, error, info, warn};
use vex_config::CoreNetworkingConfig;

use super::ServerError;

pub struct Session {
    slots: [Option<GatewaySlot>; MAX_GATEWAYS],
}

pub struct GatewaySlot {
    duologue: Duologue,
    port_data: u16,
    port_control: u16,
    session_id: i32,
}

impl Session {
    pub fn new() -> Self {
        Self {
            slots: [(); MAX_GATEWAYS].map(|_| None),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &Duologue> {
        self.slots
            .iter()
            .filter_map(|s| s.as_ref().map(|slot| &slot.duologue))
    }

    pub fn insert(&mut self, gateway_id: u8, session_id: i32, duologue: Duologue, ports: &[u16]) {
        if (gateway_id as usize) >= MAX_GATEWAYS {
            return;
        }
        self.slots[gateway_id as usize] = Some(GatewaySlot {
            duologue,
            session_id,
            port_data: ports[0],
            port_control: ports[1],
        });
    }

    pub fn remove(&mut self, gateway_id: u8) -> Option<Duologue> {
        let gateway_id = gateway_id as usize;
        if gateway_id < MAX_GATEWAYS {
            self.slots[gateway_id].take().map(|slot| slot.duologue)
        } else {
            None
        }
    }

    pub fn is_gateway_connected(&self, gateway_id: u8) -> bool {
        if (gateway_id as usize) >= MAX_GATEWAYS {
            return false;
        }
        self.slots[gateway_id as usize].is_some()
    }

    /// Get all ports currently in use (both data and control ports)
    pub fn get_ports_in_use(&self) -> Vec<u16> {
        let mut ports = Vec::new();
        for slot in self.slots.iter().flatten() {
            if slot.port_data != 0 {
                ports.push(slot.port_data);
            }
            if slot.port_control != 0 {
                ports.push(slot.port_control);
            }
        }
        ports
    }

    /// Get all session IDs currently in use
    pub fn get_sessions_in_use(&self) -> Vec<i32> {
        let mut sessions = Vec::new();
        for slot in self.slots.iter().flatten() {
            sessions.push(slot.session_id);
        }
        sessions
    }
}

/// Manages gateway connections and session lifecycle
///
/// Handles all gateway operations including handshake processing,
/// session allocation, resource management, and cleanup operations.
pub struct GatewayManager {
    /// Active gateway sessions mapped by gateway id
    gateway_sessions: RwLock<Session>,
    /// Aeron messaging instance
    aeron: Aeron,
    /// Core configuration
    config: CoreNetworkingConfig,
    /// Port allocator for gateway sessions
    port_allocator: PortAllocator,
    /// Session ID allocator
    session_allocator: SessionAllocator,
    /// Producer that sends commands to the disruptor ring
    producer: MultiProducer<OrderCommand, SingleConsumerBarrier>,
    /// Aeron publications for each gateway
    publications: Arc<Publications>,
    /// Channel for receiving cleanup requests from image unavailable callbacks
    cleanup_rx: Receiver<u8>,
    /// Channel sender cloned for each callback
    cleanup_tx: Sender<u8>,
}

impl GatewayManager {
    /// Creates a new gateway manager
    pub fn new(
        config: CoreNetworkingConfig,
        aeron: Aeron,
        producer: MultiProducer<OrderCommand, SingleConsumerBarrier>,
        publications: Arc<Publications>,
    ) -> Result<Self, ServerError> {
        let (cleanup_tx, cleanup_rx) = channel();

        Ok(Self {
            gateway_sessions: RwLock::new(Session::new()),
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
            publications,
            cleanup_rx,
            cleanup_tx,
        })
    }

    /// Checks if a gateway is currently connected
    pub fn is_gateway_connected(&self, gateway_id: u8) -> bool {
        match self.gateway_sessions.read() {
            Ok(guard) => guard.is_gateway_connected(gateway_id),
            Err(e) => {
                error!(
                    target: "gateway_manager",
                    action = "lock_poisoned",
                    context = "is_gateway_connected",
                    error = %e
                );
                false // Assume not connected if lock is poisoned
            }
        }
    }

    /// This callback will be invoked when DuologueImageUnavailable is triggered
    fn create_cleanup_callback(&self) -> Arc<dyn Fn(u8) + Send + Sync> {
        let tx = self.cleanup_tx.clone();

        Arc::new(move |gateway_id: u8| {
            debug!(
                target: "gateway_manager",
                action = "cleanup_requested",
                gateway_id
            );

            // Send cleanup request through channel
            if let Err(e) = tx.send(gateway_id) {
                error!(
                    target: "gateway_manager",
                    action = "cleanup_channel_error",
                    gateway_id,
                    error = %e
                );
            }
        })
    }

    /// Processes pending cleanup requests from image unavailable callbacks
    fn process_cleanup_requests(&self) {
        loop {
            match self.cleanup_rx.try_recv() {
                Ok(gateway_id) => {
                    debug!(
                        target: "gateway_manager",
                        action = "cleanup_process",
                        gateway_id
                    );
                    if let Err(e) = self.remove_gateway_session(gateway_id) {
                        // Log but don't fail - session might already be removed
                        warn!(
                            target: "gateway_manager",
                            action = "cleanup_remove_failed",
                            gateway_id,
                            error = %e
                        );
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    error!(
                        target: "gateway_manager",
                        action = "cleanup_channel_disconnected"
                    );
                    break;
                }
            }
        }
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

        let gateway_id_str = parts
            .next()
            .ok_or_else(|| ServerError::GatewayMessageError("Missing gateway ID".to_string()))?;

        let gateway_id = {
            const PREFIX: &str = "gateway-";
            if !gateway_id_str.starts_with(PREFIX) {
                let error_msg = format!(
                    "{session_id} {gateway_id_str} REJECT Invalid gateway ID format, expected 'gateway-{{id}}'"
                );
                send_message(publication, error_msg.as_bytes())?;
                return Err(ServerError::GatewayMessageError(
                    "Invalid gateway ID format".to_string(),
                ));
            }
            match gateway_id_str[PREFIX.len()..].parse::<u8>() {
                Ok(id) if id <= 15 => id,
                Ok(id) => {
                    let error_msg = format!(
                        "{session_id} gateway-{id} REJECT Gateway ID out of range, expected 0-15"
                    );
                    send_message(publication, error_msg.as_bytes())?;
                    return Err(ServerError::GatewayMessageError(
                        "Gateway ID out of range".to_string(),
                    ));
                }
                Err(_) => {
                    let error_msg = format!(
                        "{session_id} {gateway_id_str} REJECT Invalid gateway ID, expected numeric ID"
                    );
                    send_message(publication, error_msg.as_bytes())?;
                    return Err(ServerError::GatewayMessageError(
                        "Invalid gateway ID".to_string(),
                    ));
                }
            }
        };

        let encryption_key_str = parts.next().ok_or_else(|| {
            ServerError::GatewayMessageError("Missing encryption key".to_string())
        })?;

        let encryption_key = encryption_key_str.parse::<i32>().map_err(|e| {
            ServerError::GatewayMessageError(format!("Invalid encryption key: {e}"))
        })?;

        self.check_duplicate_connection(publication, session_id, gateway_id)?;

        // Authenticate if enabled
        if self.config.enable_authentication
            && let Err(e) = self.authenticate_gateway(gateway_id, &encryption_key.to_string())
        {
            let error_msg =
                format!("{session_id} gateway-{gateway_id} REJECT Authentication failed");
            send_message(publication, error_msg.as_bytes())?;
            return Err(e);
        }

        let (dedicated_session, ports) = self.allocate_gateway_session(gateway_id)?;
        let encrypted_session = encryption_key ^ dedicated_session;

        // Send success response
        let accept_msg = format!(
            "{} gateway-{} ACCEPT {} {} {}",
            session_id, gateway_id, ports[0], ports[1], encrypted_session
        );
        match send_message_with_retries(publication, accept_msg.as_bytes()) {
            Ok(_) => (),
            Err(e) => {
                self.remove_gateway_session(gateway_id)?;
                return Err(ServerError::GatewayMessageError(format!(
                    "Failed to send ACCEPT message: {e}"
                )));
            }
        }
        info!(
            target: "gateway_manager",
            action = "gateway_connected",
            gateway_id,
            session = format_args!("{:#x}", dedicated_session),
            data_port = ports[0],
            control_port = ports[1]
        );

        Ok(())
    }

    /// Polls all active gateway sessions
    pub fn poll(&self) -> Result<(), ServerError> {
        self.process_cleanup_requests();

        // polls all active gateway sessions
        let guard = self.gateway_sessions.read().unwrap();
        for subscription in guard.iter() {
            if let Err(e) = subscription.poll() {
                error!(
                    target: "gateway_manager",
                    action = "poll_failed",
                    gateway_id = subscription.gateway_id,
                    error = %e
                );
            }
        }
        Ok(())
    }

    /// Shuts down all gateway connections
    pub fn shutdown_all_gateways(&self) -> Result<(), ServerError> {
        let gateways_ids: Vec<u8> = self
            .gateway_sessions
            .read()
            .expect("Gateway sessions lock poisoned during shutdown")
            .iter()
            .map(|duologue| duologue.gateway_id)
            .collect();

        for gateway_id in gateways_ids {
            self.remove_gateway_session(gateway_id)?;
        }

        info!(
            target: "gateway_manager",
            action = "shutdown_complete"
        );
        Ok(())
    }

    fn check_duplicate_connection(
        &self,
        publication: &AeronPublication,
        session_id: i32,
        gateway_id: u8,
    ) -> Result<(), ServerError> {
        // Check if gateway_id is within valid range (implicit capacity check)
        if gateway_id as usize >= MAX_GATEWAYS {
            let error_msg = format!(
                "{session_id} gateway-{gateway_id} REJECT Invalid gateway ID (must be 0-{})",
                MAX_GATEWAYS - 1
            );
            send_message(publication, error_msg.as_bytes())?;
            return Err(ServerError::GatewayMessageError(format!(
                "Gateway ID {} out of range (max: {})",
                gateway_id,
                MAX_GATEWAYS - 1
            )));
        }

        // Check if this gateway is already connected
        if self.is_gateway_connected(gateway_id) {
            let error_msg =
                format!("{session_id} gateway-{gateway_id} REJECT Gateway already connected");
            send_message(publication, error_msg.as_bytes())?;
            return Err(ServerError::GatewayMessageError(
                "Gateway already connected".to_string(),
            ));
        }
        Ok(())
    }

    /// Internal allocation method that creates a session with cleanup callback
    /// Called during handshake to create session with image unavailable handling
    fn allocate_gateway_session(&self, gateway_id: u8) -> Result<(i32, [u16; 2]), ServerError> {
        // Get currently used ports and sessions from the Session struct
        let mut guard = self.gateway_sessions.write().unwrap();
        let ports_in_use = guard.get_ports_in_use();
        let sessions_in_use = guard.get_sessions_in_use();
        // drop(guard); // Release lock before allocating

        // Allocate resources - now passing in the currently used values
        let ports = match self.port_allocator.allocate(2, &ports_in_use) {
            Ok(p) => p,
            Err(e) => {
                return Err(ServerError::ResourceAllocationError(e.to_string()));
            }
        };
        let dedicated_session = match self.session_allocator.allocate(&sessions_in_use) {
            Ok(s) => s,
            Err(e) => {
                return Err(ServerError::ResourceAllocationError(e.to_string()));
            }
        };

        let publication = match new_publication_with_mdc_and_session(
            &self.aeron,
            &self.config.local_address,
            ports[1],
            DUOLOGUE_STREAM_ID,
            dedicated_session,
        ) {
            Ok(publication) => publication,
            Err(e) => {
                return Err(ServerError::ResourceAllocationError(format!(
                    "Failed to create publication: {e}"
                )));
            }
        };

        let on_image_available_handler = Handler::leak(DuologueImageAvailable {
            expected_session_id: dedicated_session,
            gateway_id,
        });

        // This will be invoked when Aeron detects the image is unavailable (connection lost)
        let cleanup_callback = Some(self.create_cleanup_callback());

        let on_image_unavailable_handler = Handler::leak(DuologueImageUnavailable {
            session_id: dedicated_session,
            gateway_id,
            cleanup_callback,
        });

        let subscription = match new_subsciption_with_handlers_and_session(
            &self.aeron,
            &self.config.local_address,
            ports[0],
            DUOLOGUE_STREAM_ID,
            dedicated_session,
            Some(&on_image_available_handler),
            Some(&on_image_unavailable_handler),
        ) {
            Ok(subscription) => subscription,
            Err(e) => {
                return Err(ServerError::ResourceAllocationError(format!(
                    "Failed to create subscription: {e}"
                )));
            }
        };

        // gateway session
        let gateway_session = Duologue::new(
            subscription,
            on_image_available_handler,
            on_image_unavailable_handler,
            gateway_id,
            self.producer.clone(),
        );

        self.publications.set(gateway_id, Arc::new(publication));

        // Store session
        guard.insert(gateway_id, dedicated_session, gateway_session, &ports);

        debug!(
            target: "gateway_manager",
            action = "session_allocated",
            gateway_id,
            session = format_args!("{:#x}", dedicated_session),
            data_port = ports[0],
            control_port = ports[1]
        );

        Ok((dedicated_session, [ports[0], ports[1]]))
    }

    fn authenticate_gateway(&self, gateway_id: u8, _credentials: &str) -> Result<(), ServerError> {
        // TODO: Implement proper authentication
        info!(
            target: "gateway_manager",
            action = "gateway_authenticated",
            gateway_id
        );
        Ok(())
    }

    /// Removes a gateway session and frees all associated resources
    pub fn remove_gateway_session(&self, gateway_id: u8) -> Result<(), ServerError> {
        let mut session = match self.gateway_sessions.write().unwrap().remove(gateway_id) {
            Some(duologue) => duologue,
            None => {
                return Err(ServerError::GatewayMessageError(format!(
                    "No active session for gateway-{gateway_id}"
                )));
            }
        };

        // remove publication
        self.publications.remove(gateway_id);

        // close subscription
        if let Err(e) = session.close() {
            error!(
                target: "gateway_manager",
                action = "session_close_failed",
                gateway_id,
                error = ?e
            );
        }

        Ok(())
    }
}
