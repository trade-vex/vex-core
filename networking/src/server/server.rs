use std::sync::Arc;
use std::{collections::HashMap, sync::RwLock};
use std::time::SystemTime;

use rusteron_client::{
    AeronCError, AeronFragmentHandlerCallback,
    AeronHeader, AeronImage, AeronPublication, AeronSubscription,
    Handler, AeronAvailableImageCallback, AeronUnavailableImageCallback,
};
use rusteron_media_driver::AeronIdleStrategy;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::task::{spawn};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{debug, error, info, instrument};

use crate::server::aeron::AeronActor;
use crate::server::{aeron::AeronCommand, config::CoreConfig};
use crate::server::duologue::{Duologue, DUOLOGUE_STREAM_ID, DuologueImageAvailable, DuologueImageUnavailable};
use crate::utils::{
    create_mdc_control_uri, create_mdc_control_with_session_uri, create_udp_endpoint_uri, create_udp_endpoint_with_session_uri, send_message, PortAllocator, SessionAllocator
};

/// Stream IDs for different communication channels
const ALL_GATEWAYS_STREAM_ID: i32 = 1001;

/// Represents a gateway's handshake request
#[derive(Debug, Clone)]
pub struct GatewayHandshakeRequest {
    pub gateway_id: String,
    pub session_id: i32,
    pub encryption_key: i32,
    pub source_address: String,
    pub timestamp: SystemTime,
}

/// Custom error types for VEX Core
#[derive(Error, Debug)]
pub enum ServerError {
    #[error("Media driver initialization failed: {0}")]
    MediaDriverError(String),
    #[error("Aeron connection failed: {0}")]
    AeronConnectionError(#[from] AeronCError),
    #[error("Subscription setup failed: {0}")]
    SubscriptionError(String),
    #[error("Publication creation failed: {0}")]
    PublicationError(String),
    #[error("Invalid gateway message: {0}")]
    InvalidGatewayMessage(String),
    #[error("URI parsing error: {0}")]
    UriParseError(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Invalid input: {0}")]
    InvalidInput(#[from] std::ffi::NulError),
    #[error("Port allocation error: {0}")]
    PortAllocationError(String),
    #[error("Session allocation error: {0}")]
    SessionAllocationError(String),
    #[error("Gateway authentication failed: {0}")]
    AuthenticationError(String),
    #[error("Core capacity exceeded: {0}")]
    CapacityExceededError(String),
    #[error("Gateway not found: {0}")]
    GatewayNotFoundError(String),
    #[error("Configuration error: {0}")]
    ConfigurationError(String),
    #[error("Error sending GatewayManagerCommand Tokio Error: {0}")]
    SendCommandError(#[from] SendError<GatewayManagerCommand>),
    #[error("Error sending AeronCommand Tokio Error: {0}")]
    SendAeronCommandError(#[from] SendError<AeronCommand>),
    #[error("Error receiving AeronCommand Tokio Error: {0}")]
    ReceiveAeronCommandError(#[from] oneshot::error::RecvError),
}


/// Enhanced VEX Core server for handling gateway connections
pub struct VexCoreServer {
    /// Core configuration
    config: CoreConfig,
    /// Gateway state management
    gateways: GatewayManager,
}

impl VexCoreServer {
    /// Creates a new VEX Core instance
    pub fn new(config: CoreConfig) -> Result<Self, ServerError> {
        // Validate configuration
        if config.max_gateways == 0 {
            return Err(ServerError::ConfigurationError("Max gateways must be greater than 0".to_string()));
        }
        if config.core_id.is_empty() {
            return Err(ServerError::ConfigurationError("Core ID cannot be empty".to_string()));
        }
        info!("VEX Core '{}' initialized", config.core_id);
        Ok(Self {
            gateways: GatewayManager::new(config.clone())?,
            config,
        })
    }

    /// Starts the VEX Core server
    #[instrument(skip(self))]
    pub async fn start(&self) -> Result<(), ServerError> {
        info!("Starting VEX Core '{}'", self.config.core_id);

        // let aeron_actor = AeronActor::new(self.config.clone())?;
        let (aeron_tx, aeron_rx) = mpsc::channel(50);
        let config = self.config.clone();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let aeron_actor = AeronActor::new(config).unwrap();
                if let Err(e) = aeron_actor.run(aeron_rx).await {
                    error!("AeronActor failed: {}", e);
                }
            });
        });
        let (gateway_manager_tx, gateway_manager_rx) = mpsc::channel(100);
        // Spawn the GatewayManager Actor in a dedicated thread
        let gateways = self.gateways.clone();

        // Create handshake message handler
        let mut handshake_handler = HandshakeMessageHandler::new(
            gateway_manager_tx.clone(),
        );
        let aeron_tx_s = aeron_tx.clone();
        let _gateway_handle = spawn(async move {
            info!("GatewayManager: Task starting");
            match gateways.run(gateway_manager_rx, gateway_manager_tx, aeron_tx_s).await {
                Ok(_) => info!("GatewayManager: Task completed successfully"),
                Err(e) => error!("GatewayManager: Task failed with error: {}", e),
            }
        });
        // Create image handlers for gateway connection management
        let image_available_handler = GatewayImageAvailableHandler {
            gateway_session_addresses: Arc::clone(&self.gateways.gateway_session_addresses),
        };
        let image_unavailable_handler = GatewayImageUnavailableHandler {
            gateway_session_addresses: Arc::clone(&self.gateways.gateway_session_addresses),
        };

        // Create subscription for receiving gateway 
        let (sub_tx, sub_rx) = oneshot::channel();
        let channel = create_udp_endpoint_uri(
            &self.config.local_address,
            self.config.initial_port,
        );
        aeron_tx.send(AeronCommand::CreateAllGatewaySubscription {channel, stream_id: ALL_GATEWAYS_STREAM_ID, reply: sub_tx, image_available_handler, image_unavailable_handler }).await?;
        let subscription = sub_rx.await??;
        info!("VEX Core '{}' started successfully", self.config.core_id);

        // Main event loop
        tokio::task::spawn_blocking(move || {
            loop {
                // Process incoming handshake messages
                subscription.poll(Some(&Handler::leak(&mut handshake_handler)), 10).unwrap();
                // Idle strategy
                AeronIdleStrategy::busy_spinning_idle(std::ptr::null_mut(), 0);
                }
            });
        info!("VEX Core '{}' started successfully. Polling loop is running.", self.config.core_id);

        // The `start` function can now complete, or you can await other futures,
        // like a shutdown signal. For instance, to keep the server alive:
        tokio::signal::ctrl_c().await.expect("failed to listen for ctrl-c");
        info!("Shutdown signal received.");
        Ok(())
    }

    /// Gets core configuration
    pub fn config(&self) -> &CoreConfig {
        &self.config
    }

    /// Gets the number of connected gateways
    pub fn connected_gateway_count(&self) -> usize {
        self.gateways.gateway_sessions.len()
    }

    /// Checks if a gateway is connected
    pub fn is_gateway_connected(&self, gateway_id: &str) -> bool {
        self.gateways.is_gateway_connected(gateway_id)
    }

    /// Gracefully shuts down the core
    pub fn shutdown(&mut self) -> Result<(), ServerError> {
        info!("Shutting down VEX Core '{}'", self.config.core_id);
        
        // Close all gateway sessions
        self.gateways.shutdown_all_gateways()?;
        
        info!("VEX Core '{}' shut down successfully", self.config.core_id);
        Ok(())
    }
}

/// Commands that are sent to the GatewayManager Actor
#[derive(Debug, Clone)]
pub enum GatewayManagerCommand {
    ProcessHandshake {
        /// Session ID of the All Gateways Channel
        session_id: i32,
        /// Message from the gateway
        message: String,
    },
    DisconnectGateway {
        /// Dedicated session ID
        session_id: i32
    },
    ShutdownAllGateways,
}

/// Manages all gateway connections and sessions
#[derive(Clone)]
struct GatewayManager {
    /// Map of session ID to gateway address
    gateway_session_addresses: Arc<RwLock<HashMap<i32, String>>>,
    /// Gateway sessions to Gateway IDs
    gateway_sessions: HashMap<i32, Duologue>,
    /// Core configuration
    config: CoreConfig,
    /// Message buffer for sending responses
    buffer: [u8; 2048],
    /// Count of connections per address
    address_connection_count: HashMap<String, u16>,
    /// Port allocator for gateway sessions
    port_allocator: PortAllocator,
    /// Session ID allocator
    session_allocator: SessionAllocator,
}

impl GatewayManager {
    fn new(config: CoreConfig) -> Result<Self, ServerError> {
        Ok(Self {
            gateway_session_addresses: Arc::new(RwLock::new(HashMap::new())),
            gateway_sessions: HashMap::new(),
            port_allocator: PortAllocator::new(
                config.base_gateway_port,
                config.max_gateways.into(),
            ).map_err(|e| ServerError::PortAllocationError(e.to_string()))?,
            session_allocator: SessionAllocator::new(
                config.reserved_session_id_low,
                config.reserved_session_id_high,
            ).map_err(|e| ServerError::SessionAllocationError(e.to_string()))?,
            config,
            buffer: [0u8; 2048],
            address_connection_count: HashMap::new(),
        })
    }

    /// GatewayManager Actor
    async fn run(mut self, mut gateway_manager_rx: Receiver<GatewayManagerCommand>, gateway_manager_tx: Sender<GatewayManagerCommand>, aeron_tx: Sender<AeronCommand>) -> Result<(), ServerError> {
        info!("GatewayManager Actor started");
        let (pub_tx, pub_rx) = oneshot::channel();
        let _ = aeron_tx.send(AeronCommand::CreatePublication { channel: create_mdc_control_uri(&self.config.local_address, self.config.initial_control_port), stream_id: ALL_GATEWAYS_STREAM_ID, reply: pub_tx }).await?;
        info!("GatewayManager: Waiting for CreatePublication reply...");
        // Add detailed error handling here
        match pub_rx.await {
            Ok(Ok(publication)) => {
                info!("GatewayManager created all gateways publication successfully");
                while let Some(command) = gateway_manager_rx.recv().await {
                    match command {
                        GatewayManagerCommand::ProcessHandshake { session_id, message } => {
                            info!("Processing handshake from session 0x{:x}: {}", session_id, message);
                            self.process_handshake_message(
                                session_id,
                                &message,
                                gateway_manager_tx.clone(),
                                &publication,
                                aeron_tx.clone(),
                            ).await?;
                        }
                        GatewayManagerCommand::DisconnectGateway { session_id } => {
                            info!("Disconnecting gateway session 0x{:x}", session_id);
                            self.remove_gateway_session(session_id)?;
                        }
                        GatewayManagerCommand::ShutdownAllGateways => {
                            info!("Shutting down all gateway sessions");
                            self.shutdown_all_gateways()?;
                        } 
                    }
                }
                info!("GatewayManager: Message loop ended - no more commands");
                Ok(())
            }
            Ok(Err(aeron_error)) => {
                error!("GatewayManager failed to create publication: AeronCError: {}", aeron_error);
                return Err(ServerError::AeronConnectionError(aeron_error));
            }
            Err(recv_error) => {
                error!("GatewayManager failed to receive publication reply: {}", recv_error);
                return Err(ServerError::ReceiveAeronCommandError(recv_error));
            }
        }
    }

    /// Processes initial handshake message from a gateway
    async fn process_handshake_message(
        &mut self,
        session_id: i32,
        message: &str,
        tx: Sender<GatewayManagerCommand>,
        publication: &AeronPublication,
        aeron_tx: Sender<AeronCommand>,
    ) -> Result<(), ServerError> {
        debug!("Processing handshake from session 0x{:x}: {}", session_id, message);

        // Parse handshake message: "HELLO gateway_id encryption_key"
        let parts: Vec<&str> = message.split_whitespace().collect();
        if parts.len() != 3 || parts[0] != "HELLO" {
            let error_msg = format!("{} {} REJECT Malformed HELLO message", session_id, "unknown");
            send_message(&publication, &mut self.buffer, &error_msg)?;
            return Err(ServerError::InvalidGatewayMessage("Malformed HELLO message".to_string()));
        }

        let gateway_id = parts[1];
        let encryption_key = parts[2].parse::<i32>()
            .map_err(|e| ServerError::InvalidGatewayMessage(format!("Invalid encryption key: {}", e)))?;

        // Validate gateway ID
        if gateway_id.is_empty() {
            let error_msg = format!("{} {} REJECT Empty gateway ID", session_id, gateway_id);
            send_message(&publication, &mut self.buffer, &error_msg)?;
            return Err(ServerError::InvalidGatewayMessage("Empty gateway ID".to_string()));
        }

        // Check if too many gateways are connected
        if self.gateway_sessions.len() >= self.config.max_gateways as usize {
            let error_msg = format!("{} {} REJECT Core capacity exceeded", session_id, gateway_id);
            send_message(&publication, &mut self.buffer, &error_msg)?;
            return Err(ServerError::CapacityExceededError("Too many gateways connected".to_string()));
        }

        // Check connection limits per address
        if let Some(gateway_address) = self.gateway_session_addresses.read().unwrap().get(&session_id) {
            let connection_count = self.address_connection_count.get(gateway_address).unwrap_or(&0);
            if *connection_count >= self.config.max_connections_per_address {
                let error_msg = format!("{} {} REJECT Too many connections from address", session_id, gateway_id);
                send_message(&publication, &mut self.buffer, &error_msg)?;
                return Err(ServerError::CapacityExceededError("Too many connections from this address".to_string()));
            }
        }

        // Check if gateway is already connected
        if self.is_gateway_connected(gateway_id) {
            let error_msg = format!("{} {} REJECT Gateway already connected", session_id, gateway_id);
            send_message(&publication, &mut self.buffer, &error_msg)?;
            return Err(ServerError::InvalidGatewayMessage("Gateway already connected".to_string()));
        }

        // Authenticate gateway if required
        if self.config.enable_authentication {
            if let Err(e) = self.authenticate_gateway(gateway_id, &encryption_key.to_string()) {
                let error_msg = format!("{} {} REJECT Authentication failed", session_id, gateway_id);
                send_message(&publication, &mut self.buffer, &error_msg)?;
                return Err(e);
            }
        }

        // Allocate dedicated session for this gateway
        let gateway_address = self.gateway_session_addresses.read().unwrap().get(&session_id)
            .ok_or_else(|| ServerError::InvalidGatewayMessage("Gateway address not found".to_string()))?
            .clone();

        let (dedicated_session, ports) = self.allocate_gateway_session(session_id, gateway_id, &gateway_address, tx, aeron_tx).await?;

        // Encrypt the dedicated session ID
        let encrypted_session = encryption_key ^ dedicated_session;

        // Send ACCEPT response
        let accept_msg = format!(
            "{} {} ACCEPT {} {} {}",
            session_id, gateway_id, ports[0], ports[1], encrypted_session
        );
        info!("Sending ACCEPT response to gateway: {}", accept_msg);
        send_message(&publication, &mut self.buffer, &accept_msg)?;

        info!(
            "Gateway '{}' handshake successful. Dedicated session: 0x{:x}, ports: {} and {}",
            gateway_id, dedicated_session, ports[0], ports[1]
        );

        Ok(())
    }

    /// Allocates a dedicated session for a gateway
    async fn allocate_gateway_session(
        &mut self,
        initial_session_id: i32,
        gateway_id: &str,
        gateway_address: &str,
        tx: Sender<GatewayManagerCommand>,
        aeron_tx: Sender<AeronCommand>,
    ) -> Result<(i32, [u16; 2]), ServerError> {
        // Increment connection count for this address
        let counter = self.address_connection_count.entry(gateway_address.to_string()).or_insert(0);
        *counter += 1;

        // Allocate two ports for the gateway session
        let ports = self.port_allocator.allocate(2)
            .map_err(|e| ServerError::PortAllocationError(e.to_string()))?;

        // Allocate a dedicated session ID
        let dedicated_session = self.session_allocator.allocate()
            .map_err(|e| ServerError::SessionAllocationError(e.to_string()))?;

        let (pub_tx, pub_rx) = oneshot::channel();
        aeron_tx.send(AeronCommand::CreatePublication { channel: create_mdc_control_with_session_uri(&self.config.local_address, ports[1], dedicated_session), stream_id: DUOLOGUE_STREAM_ID, reply: pub_tx }).await?;
        let publication = pub_rx.await??;

        let (sub_tx, sub_rx) = oneshot::channel();
        let image_available_handler = DuologueImageAvailable { owner: gateway_address.to_string() };
        let image_unavailable_handler = DuologueImageUnavailable { owner: gateway_address.to_string() };
        aeron_tx.send(AeronCommand::CreateGatewaySubscription { channel: create_udp_endpoint_with_session_uri(&self.config.local_address, ports[0], dedicated_session), stream_id: DUOLOGUE_STREAM_ID, reply: sub_tx, image_available_handler, image_unavailable_handler }).await?;
        let subscription = sub_rx.await??;

        // Create gateway session
        let duologue = Duologue::new(
            gateway_id,
            gateway_address,
            ports[0],
            ports[1],
            dedicated_session,
            publication,
            subscription,
        )?;

        // Spawn a task to handle the gateway session
        info!("{}: Gateway task started for session 0x{:x} from {}", duologue.gateway_id, dedicated_session, gateway_address);
        spawn(gateway_task(duologue, tx.clone()));

        // Store the gateway session
        // self.gateway_sessions.insert(initial_session_id, gateway_session);
        self.gateway_session_addresses.write().unwrap().insert(initial_session_id, gateway_address.to_string());

        debug!(
            "Allocated dedicated session 0x{:x} for gateway '{}' with ports {} and {}",
            dedicated_session, gateway_id, ports[0], ports[1]
        );

        Ok((dedicated_session, [ports[0], ports[1]]))
    }

    /// Authenticates a gateway (placeholder implementation)
    fn authenticate_gateway(&self, gateway_id: &str, _credentials: &str) -> Result<(), ServerError> {
        // TODO: Implement actual authentication logic
        // For now, just validate the gateway ID format
        if gateway_id.len() < 3 || !gateway_id.starts_with("gateway-") {
            return Err(ServerError::AuthenticationError("Invalid gateway ID format".to_string()));
        }
        
        info!("Gateway '{}' authenticated successfully", gateway_id);
        Ok(())
    }

    /// Checks if a gateway is already connected
    fn is_gateway_connected(&self, gateway_id: &str) -> bool {
        self.gateway_sessions.values().any(|session| session.gateway_id == gateway_id)
    }

    /// Removes a gateway session and cleans up resources
    fn remove_gateway_session(&mut self, session_id: i32) -> Result<(), ServerError> {
        if let Some(mut gateway_session) = self.gateway_sessions.remove(&session_id) {
            info!("Removing gateway session 0x{:x} for gateway '{}'", session_id, gateway_session.gateway_id);

            // Close the session
            gateway_session.close()?;

            // Free allocated ports
            self.port_allocator.free(gateway_session.port_data);
            self.port_allocator.free(gateway_session.port_control);

            // Decrement connection count for the address
            if let Some(address) = self.gateway_session_addresses.write().unwrap().remove(&session_id) {
                if let Some(count) = self.address_connection_count.get_mut(&address) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        self.address_connection_count.remove(&address);
                    }
                }
            }
        }

        Ok(())
    }

    /// Shuts down all gateways
    fn shutdown_all_gateways(&mut self) -> Result<(), ServerError> {
        let session_ids: Vec<i32> = self.gateway_sessions.keys().cloned().collect();
        
        for session_id in session_ids {
            self.remove_gateway_session(session_id)?;
        }

        info!("All gateway sessions shut down");
        Ok(())
    }
}

async fn gateway_task(mut duologue: Duologue, manager: Sender<GatewayManagerCommand>) -> Result<(), ServerError> {
    let id = &duologue.gateway_id;
    let dedicated_session_id = duologue.session_id;
    loop {

        if duologue.is_closed() {
            info!("{}: Session 0x{:x} closed", id, dedicated_session_id);
            break;
        }

        if duologue.is_expired() {
            info!("{}: Session 0x{:x} expired", id, dedicated_session_id);
            break;
        }

        duologue.poll()?;

        AeronIdleStrategy::busy_spinning_idle(std::ptr::null_mut(), 0);
    }
    info!("{}: Gateway task stopped for session 0x{:x}", id, dedicated_session_id);
    duologue.close()?;
    manager.send(GatewayManagerCommand::DisconnectGateway { session_id: dedicated_session_id }).await?;
    Ok(())
}
/// Handler for processing initial handshake messages from gateways
struct HandshakeMessageHandler {
    gateway_manager_tx: Sender<GatewayManagerCommand>,
}

impl HandshakeMessageHandler {
    fn new(
        gateway_manager_tx: Sender<GatewayManagerCommand>,
    ) -> Self {
        Self {
            gateway_manager_tx,
        }
    }
}

impl AeronFragmentHandlerCallback for &mut HandshakeMessageHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) {
        let message = String::from_utf8_lossy(buffer);
        let session_id = header.get_values().unwrap().frame.session_id;
        
        debug!("Received handshake message from session 0x{:x}: {}", session_id, message);

        // Use blocking_send instead of try_send
        let tx = self.gateway_manager_tx.clone();
        let command = GatewayManagerCommand::ProcessHandshake { 
            session_id, 
            message: message.to_string() 
        };
        
        // Spawn a task to send the command asynchronously
        match tx.try_send(command) {
            Ok(_) => {
                info!("Handshake message sent to GatewayManager Actor");
            }
            Err(e) => {
                error!("Error sending handshake message to GatewayManager Actor: {}", e);
            }
        }
    }
}

/// Handler for gateway image availability events
pub struct GatewayImageAvailableHandler {
    gateway_session_addresses: Arc<RwLock<HashMap<i32, String>>>,
}

impl AeronAvailableImageCallback for GatewayImageAvailableHandler {
    fn handle_aeron_on_available_image(
        &mut self,
        _subscription: AeronSubscription,
        image: AeronImage,
    ) {
        let session_id = image.get_constants().unwrap().session_id;
        let binding = image.get_constants().unwrap();
        let address = binding.source_identity();
        
        debug!("Gateway image available for session 0x{:x} from {}", session_id, address);
        
        let mut gateway_session_addresses = self.gateway_session_addresses.write().unwrap();
        gateway_session_addresses.insert(session_id, address.to_string());
    }
}

/// Handler for gateway image unavailability events
pub struct GatewayImageUnavailableHandler {
    gateway_session_addresses: Arc<RwLock<HashMap<i32, String>>>,
}

impl AeronUnavailableImageCallback for GatewayImageUnavailableHandler {
    fn handle_aeron_on_unavailable_image(
        &mut self,
        _subscription: AeronSubscription,
        image: AeronImage,
    ) {
        let session_id = image.get_constants().unwrap().session_id;
        let binding = image.get_constants().unwrap();
        let address = binding.source_identity();
        
        debug!("Gateway image unavailable for session 0x{:x} from {}", session_id, address);
        
        let mut gateway_session_addresses = self.gateway_session_addresses.write().unwrap();
        gateway_session_addresses.remove(&session_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_core_config_default() {
        let config = CoreConfig::default();
        assert_eq!(config.core_id, "vex-core-1");
        assert_eq!(config.max_gateways, 100);
        assert_eq!(config.initial_port, 40001);
        assert!(config.enable_authentication);
        assert!(config.enable_heartbeat);
    }
}