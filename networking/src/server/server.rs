use std::sync::Arc;
use std::{collections::HashMap, sync::RwLock};
use std::time::{Duration, Instant, SystemTime};

use rusteron_client::{
    Aeron, AeronCError, AeronContext, AeronFragmentHandlerCallback,
    AeronHeader, AeronImage, AeronPublication, AeronSubscription,
    Handler, AeronAvailableImageCallback, AeronUnavailableImageCallback,
};
use thiserror::Error;
use tracing::{debug, error, info, warn, instrument};

use crate::server::config::CoreConfig;
use crate::server::duologue::Duologue;
use crate::utils::{
    new_publication_with_mdc, new_subscription_with_handlers, send_message, 
    PortAllocator, SessionAllocator
};

// Stream IDs for different communication channels
const ALL_GATEWAYS_STREAM_ID: i32 = 1001;
// const GATEWAY_CORE_STREAM_ID: i32 = 1002;
// const HEARTBEAT_STREAM_ID: i32 = 1003;

// Timeouts and intervals
// const GATEWAY_TIMEOUT: Duration = Duration::from_secs(30);
// const HEARTBEAT_CHECK_INTERVAL: Duration = Duration::from_secs(10);
const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);



/// Represents a gateway's handshake request
#[derive(Debug, Clone)]
pub struct GatewayHandshakeRequest {
    pub gateway_id: String,
    pub session_id: i32,
    pub encryption_key: i32,
    pub source_address: String,
    pub timestamp: SystemTime,
}

/// Core server state and statistics
#[derive(Debug, Default, Clone)]
pub struct CoreStats {
    pub connected_gateways: u32,
    pub total_messages_received: u64,
    pub total_messages_sent: u64,
    pub handshakes_successful: u64,
    pub handshakes_rejected: u64,
    pub uptime_start: Option<SystemTime>,
    pub last_cleanup: Option<SystemTime>,
}

/// Custom error types for VEX Core
#[derive(Error, Debug)]
pub enum CoreError {
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
}

/// Enhanced VEX Core server for handling gateway connections
pub struct VexCore {
    /// Aeron instance for messaging
    aeron: Arc<Aeron>,
    /// Core configuration
    config: CoreConfig,
    /// Gateway state management
    gateways: Arc<RwLock<GatewayManager>>,
    /// Core statistics
    stats: Arc<RwLock<CoreStats>>,
    /// Last cleanup timestamp
    last_cleanup: Arc<RwLock<Instant>>,
}

impl VexCore {
    /// Creates a new VEX Core instance
    pub fn new(config: CoreConfig) -> Result<Self, CoreError> {
        // Validate configuration
        if config.max_gateways == 0 {
            return Err(CoreError::ConfigurationError("Max gateways must be greater than 0".to_string()));
        }
        if config.core_id.is_empty() {
            return Err(CoreError::ConfigurationError("Core ID cannot be empty".to_string()));
        }

        // Initialize Aeron context
        let ctx = AeronContext::new()?;
        let context_dir = std::ffi::CString::new(config.context_dir.clone())?;
        info!("VEX Core '{}' context_dir: {:?}", config.core_id, context_dir);
        ctx.set_dir(&context_dir)?;
        ctx.set_driver_timeout_ms(5_000)?;

        // Create Aeron instance
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;
        
        info!("VEX Core '{}' initialized successfully", config.core_id);

        let mut stats = CoreStats::default();
        stats.uptime_start = Some(SystemTime::now());

        let aeron = Arc::new(aeron);

        Ok(Self {
            aeron: Arc::clone(&aeron),
            gateways: Arc::new(RwLock::new(GatewayManager::new(config.clone(), aeron)?)),
            config,
            stats: Arc::new(RwLock::new(stats)),
            last_cleanup: Arc::new(RwLock::new(Instant::now())),
        })
    }

    /// Starts the VEX Core server
    #[instrument(skip(self))]
    pub fn start(&self) -> Result<(), CoreError> {
        info!("Starting VEX Core '{}'", self.config.core_id);
        
        // Create publication for sending responses to gateways
        let publication = new_publication_with_mdc(
            &self.aeron,
            &self.config.local_address,
            self.config.initial_control_port,
            ALL_GATEWAYS_STREAM_ID,
        )?;

        // Create image handlers for gateway connection management
        let image_available_handler = GatewayImageAvailableHandler {
            gateways: self.gateways.clone(),
        };
        let image_unavailable_handler = GatewayImageUnavailableHandler {
            gateways: self.gateways.clone(),
        };

        // Create subscription for receiving gateway handshakes
        let subscription = new_subscription_with_handlers(
            &self.aeron,
            &self.config.local_address,
            self.config.initial_port,
            ALL_GATEWAYS_STREAM_ID,
            image_available_handler,
            image_unavailable_handler,
        )?;

        // Create handshake message handler
        let mut handshake_handler = HandshakeMessageHandler::new(
            self.gateways.clone(),
            self.stats.clone(),
            publication,
        );

        info!("VEX Core '{}' started successfully", self.config.core_id);

        // Main event loop
        loop {
            // Process incoming handshake messages
            subscription.poll(Some(&Handler::leak(&mut handshake_handler)), 10)?;
            
            // Poll all active gateway sessions
            self.gateways.write().unwrap().poll()?;
            
            // Perform periodic cleanup
            self.periodic_cleanup()?;
            
            // Brief pause to prevent busy waiting
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Performs periodic cleanup of expired gateways and statistics
    fn periodic_cleanup(&self) -> Result<(), CoreError> {
        let now = Instant::now();
        let mut last_cleanup = self.last_cleanup.write().unwrap();
        
        if now.duration_since(*last_cleanup) >= CLEANUP_INTERVAL {
            info!("Performing periodic cleanup");
            
            // Clean up expired gateways
            let cleanup_count = self.gateways.write().unwrap().cleanup_expired_gateways()?;
            if cleanup_count > 0 {
                info!("Cleaned up {} expired gateways", cleanup_count);
            }
            
            // Update statistics
            {
                let mut stats = self.stats.write().unwrap();
                stats.last_cleanup = Some(SystemTime::now());
            }
            
            *last_cleanup = now;
        }
        
        Ok(())
    }

    /// Gets core statistics
    pub fn stats(&self) -> CoreStats {
        self.stats.read().unwrap().clone()
    }

    /// Gets core configuration
    pub fn config(&self) -> &CoreConfig {
        &self.config
    }

    /// Gets the number of connected gateways
    pub fn connected_gateway_count(&self) -> usize {
        self.gateways.read().unwrap().gateway_sessions.len()
    }

    /// Checks if a gateway is connected
    pub fn is_gateway_connected(&self, gateway_id: &str) -> bool {
        self.gateways.read().unwrap().is_gateway_connected(gateway_id)
    }

    /// Gracefully shuts down the core
    pub fn shutdown(&self) -> Result<(), CoreError> {
        info!("Shutting down VEX Core '{}'", self.config.core_id);
        
        // Close all gateway sessions
        self.gateways.write().unwrap().shutdown_all_gateways()?;
        
        info!("VEX Core '{}' shut down successfully", self.config.core_id);
        Ok(())
    }
}

/// Manages all gateway connections and sessions
struct GatewayManager {
    /// Map of session ID to gateway address
    gateway_session_addresses: HashMap<i32, String>,
    /// Map of session ID to gateway sessions
    gateway_sessions: HashMap<i32, Duologue>,
    /// Aeron instance reference
    aeron: Arc<Aeron>,
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
    fn new(config: CoreConfig, aeron: Arc<Aeron>) -> Result<Self, CoreError> {
        Ok(Self {
            gateway_session_addresses: HashMap::new(),
            gateway_sessions: HashMap::new(),
            aeron,
            port_allocator: PortAllocator::new(
                config.base_gateway_port,
                config.max_gateways.into(),
            ).map_err(|e| CoreError::PortAllocationError(e.to_string()))?,
            session_allocator: SessionAllocator::new(
                config.reserved_session_id_low,
                config.reserved_session_id_high,
            ).map_err(|e| CoreError::SessionAllocationError(e.to_string()))?,
            config,
            buffer: [0u8; 2048],
            address_connection_count: HashMap::new(),
        })
    }

    /// Processes initial handshake message from a gateway
    fn process_handshake_message(
        &mut self,
        publication: &AeronPublication,
        session_id: i32,
        message: &str,
    ) -> Result<(), CoreError> {
        debug!("Processing handshake from session 0x{:x}: {}", session_id, message);

        // Parse handshake message: "HELLO gateway_id encryption_key"
        let parts: Vec<&str> = message.split_whitespace().collect();
        if parts.len() != 3 || parts[0] != "HELLO" {
            let error_msg = format!("{} {} REJECT Malformed HELLO message", session_id, "unknown");
            send_message(publication, &mut self.buffer, &error_msg)?;
            return Err(CoreError::InvalidGatewayMessage("Malformed HELLO message".to_string()));
        }

        let gateway_id = parts[1];
        let encryption_key = parts[2].parse::<i32>()
            .map_err(|e| CoreError::InvalidGatewayMessage(format!("Invalid encryption key: {}", e)))?;

        // Validate gateway ID
        if gateway_id.is_empty() {
            let error_msg = format!("{} {} REJECT Empty gateway ID", session_id, gateway_id);
            send_message(publication, &mut self.buffer, &error_msg)?;
            return Err(CoreError::InvalidGatewayMessage("Empty gateway ID".to_string()));
        }

        // Check if too many gateways are connected
        if self.gateway_sessions.len() >= self.config.max_gateways as usize {
            let error_msg = format!("{} {} REJECT Core capacity exceeded", session_id, gateway_id);
            send_message(publication, &mut self.buffer, &error_msg)?;
            return Err(CoreError::CapacityExceededError("Too many gateways connected".to_string()));
        }

        // Check connection limits per address
        if let Some(gateway_address) = self.gateway_session_addresses.get(&session_id) {
            let connection_count = self.address_connection_count.get(gateway_address).unwrap_or(&0);
            if *connection_count >= self.config.max_connections_per_address {
                let error_msg = format!("{} {} REJECT Too many connections from address", session_id, gateway_id);
                send_message(publication, &mut self.buffer, &error_msg)?;
                return Err(CoreError::CapacityExceededError("Too many connections from this address".to_string()));
            }
        }

        // Check if gateway is already connected
        if self.is_gateway_connected(gateway_id) {
            let error_msg = format!("{} {} REJECT Gateway already connected", session_id, gateway_id);
            send_message(publication, &mut self.buffer, &error_msg)?;
            return Err(CoreError::InvalidGatewayMessage("Gateway already connected".to_string()));
        }

        // Authenticate gateway if required
        if self.config.enable_authentication {
            if let Err(e) = self.authenticate_gateway(gateway_id, &encryption_key.to_string()) {
                let error_msg = format!("{} {} REJECT Authentication failed", session_id, gateway_id);
                send_message(publication, &mut self.buffer, &error_msg)?;
                return Err(e);
            }
        }

        // Allocate dedicated session for this gateway
        let gateway_address = self.gateway_session_addresses.get(&session_id)
            .ok_or_else(|| CoreError::InvalidGatewayMessage("Gateway address not found".to_string()))?
            .clone();

        let (dedicated_session, ports) = self.allocate_gateway_session(session_id, gateway_id, &gateway_address)?;

        // Encrypt the dedicated session ID
        let encrypted_session = encryption_key ^ dedicated_session;

        // Send ACCEPT response
        let accept_msg = format!(
            "{} {} ACCEPT {} {} {}",
            session_id, gateway_id, ports[0], ports[1], encrypted_session
        );
        send_message(publication, &mut self.buffer, &accept_msg)?;

        info!(
            "Gateway '{}' handshake successful. Dedicated session: 0x{:x}, ports: {} and {}",
            gateway_id, dedicated_session, ports[0], ports[1]
        );

        Ok(())
    }

    /// Allocates a dedicated session for a gateway
    fn allocate_gateway_session(
        &mut self,
        initial_session_id: i32,
        gateway_id: &str,
        gateway_address: &str,
    ) -> Result<(i32, [u16; 2]), CoreError> {
        // Increment connection count for this address
        let counter = self.address_connection_count.entry(gateway_address.to_string()).or_insert(0);
        *counter += 1;

        // Allocate two ports for the gateway session
        let ports = self.port_allocator.allocate(2)
            .map_err(|e| CoreError::PortAllocationError(e.to_string()))?;

        // Allocate a dedicated session ID
        let dedicated_session = self.session_allocator.allocate()
            .map_err(|e| CoreError::SessionAllocationError(e.to_string()))?;

        // Create gateway session
        let gateway_session = Duologue::new(
            &self.aeron,
            &self.config.local_address,
            gateway_id,
            gateway_address,
            ports[0],
            ports[1],
            dedicated_session,
        )?;

        // Store the gateway session
        self.gateway_sessions.insert(initial_session_id, gateway_session);
        self.gateway_session_addresses.insert(initial_session_id, gateway_address.to_string());

        debug!(
            "Allocated dedicated session 0x{:x} for gateway '{}' with ports {} and {}",
            dedicated_session, gateway_id, ports[0], ports[1]
        );

        Ok((dedicated_session, [ports[0], ports[1]]))
    }

    /// Authenticates a gateway (placeholder implementation)
    fn authenticate_gateway(&self, gateway_id: &str, _credentials: &str) -> Result<(), CoreError> {
        // TODO: Implement actual authentication logic
        // For now, just validate the gateway ID format
        if gateway_id.len() < 3 || !gateway_id.starts_with("gateway-") {
            return Err(CoreError::AuthenticationError("Invalid gateway ID format".to_string()));
        }
        
        info!("Gateway '{}' authenticated successfully", gateway_id);
        Ok(())
    }

    /// Checks if a gateway is already connected
    fn is_gateway_connected(&self, gateway_id: &str) -> bool {
        self.gateway_sessions.values().any(|session| session.gateway_id == gateway_id)
    }

    /// Polls all active gateway sessions
    fn poll(&mut self) -> Result<(), CoreError> {
        let mut sessions_to_remove = Vec::new();

        for (initial_session_id, gateway_session) in self.gateway_sessions.iter_mut() {
            let mut should_remove = false;

            // Check if session is expired
            if gateway_session.is_expired() {
                warn!("Gateway session 0x{:x} expired", initial_session_id);
                should_remove = true;
            }

            // Check if session is closed
            if gateway_session.is_closed() {
                info!("Gateway session 0x{:x} closed", initial_session_id);
                should_remove = true;
            }

            if should_remove {
                sessions_to_remove.push(*initial_session_id);
                continue;
            }

            // Poll the session for messages
            if let Err(e) = gateway_session.poll() {
                error!("Error polling gateway session 0x{:x}: {}", initial_session_id, e);
                sessions_to_remove.push(*initial_session_id);
            }
        }

        // Remove expired/closed sessions
        for session_id in sessions_to_remove {
            self.remove_gateway_session(session_id)?;
        }

        Ok(())
    }

    /// Removes a gateway session and cleans up resources
    fn remove_gateway_session(&mut self, session_id: i32) -> Result<(), CoreError> {
        if let Some(mut gateway_session) = self.gateway_sessions.remove(&session_id) {
            info!("Removing gateway session 0x{:x} for gateway '{}'", session_id, gateway_session.gateway_id);

            // Close the session
            gateway_session.close()?;

            // Free allocated ports
            self.port_allocator.free(gateway_session.port_data);
            self.port_allocator.free(gateway_session.port_control);

            // Decrement connection count for the address
            if let Some(address) = self.gateway_session_addresses.remove(&session_id) {
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

    /// Cleans up expired gateways
    fn cleanup_expired_gateways(&mut self) -> Result<usize, CoreError> {
        let expired_sessions: Vec<i32> = self.gateway_sessions
            .iter()
            .filter(|(_, session)| session.is_expired())
            .map(|(session_id, _)| *session_id)
            .collect();

        let count = expired_sessions.len();
        for session_id in expired_sessions {
            self.remove_gateway_session(session_id)?;
        }

        Ok(count)
    }

    /// Shuts down all gateways
    fn shutdown_all_gateways(&mut self) -> Result<(), CoreError> {
        let session_ids: Vec<i32> = self.gateway_sessions.keys().cloned().collect();
        
        for session_id in session_ids {
            self.remove_gateway_session(session_id)?;
        }

        info!("All gateway sessions shut down");
        Ok(())
    }
}

/// Handler for processing initial handshake messages from gateways
struct HandshakeMessageHandler {
    gateways: Arc<RwLock<GatewayManager>>,
    stats: Arc<RwLock<CoreStats>>,
    publication: AeronPublication,
}

impl HandshakeMessageHandler {
    fn new(
        gateways: Arc<RwLock<GatewayManager>>,
        stats: Arc<RwLock<CoreStats>>,
        publication: AeronPublication,
    ) -> Self {
        Self {
            gateways,
            stats,
            publication,
        }
    }
}

impl AeronFragmentHandlerCallback for &mut HandshakeMessageHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) {
        let message = String::from_utf8_lossy(buffer);
        let session_id = header.get_values().unwrap().frame.session_id;
        
        debug!("Received handshake message from session 0x{:x}: {}", session_id, message);

        // Update statistics
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_messages_received += 1;
        }

        // Process the handshake message
        let mut gateways = self.gateways.write().unwrap();
        match gateways.process_handshake_message(&self.publication, session_id, &message) {
            Ok(_) => {
                let mut stats = self.stats.write().unwrap();
                stats.handshakes_successful += 1;
                stats.total_messages_sent += 1;
            }
            Err(e) => {
                error!("Error processing handshake message: {}", e);
                let mut stats = self.stats.write().unwrap();
                stats.handshakes_rejected += 1;
            }
        }
    }
}

/// Handler for gateway image availability events
struct GatewayImageAvailableHandler {
    gateways: Arc<RwLock<GatewayManager>>,
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
        
        let mut gateways = self.gateways.write().unwrap();
        gateways.gateway_session_addresses.insert(session_id, address.to_string());
    }
}

/// Handler for gateway image unavailability events
struct GatewayImageUnavailableHandler {
    gateways: Arc<RwLock<GatewayManager>>,
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
        
        let mut gateways = self.gateways.write().unwrap();
        gateways.gateway_session_addresses.remove(&session_id);
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