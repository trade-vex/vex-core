use crate::utils::{
    new_publication, new_publication_with_session, new_subscription_with_mdc,
    new_subscription_with_mdc_and_session,
};
use common::{OrderCommand, encode_order_command};
use rand;
use rusteron_client::{
    Aeron, AeronCError, AeronContext, AeronFragmentHandlerCallback, AeronHeader, AeronPublication,
    AeronReservedValueSupplierLogger, AeronSubscription, Handler,
};
use rusteron_media_driver::AeronIdleStrategy;
use std::ffi::CString;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::{debug, error, info, warn};
use vex_config::GatewayNetworkingConfig;

mod cmd_handler;

pub use cmd_handler::OrderCommandHandler;

// Constants for stream identification and timeouts
const ALL_GATEWAYS_STREAM_ID: i32 = 1001;
const GATEWAY_CORE_STREAM_ID: i32 = 1002;
// const HEARTBEAT_STREAM_ID: i32 = 1003;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
// const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const MESSAGE_RETRY_COUNT: usize = 5;
const MESSAGE_RETRY_DELAY: Duration = Duration::from_millis(100);

/// Represents the VEX Core's response to gateway handshake
#[derive(Debug, PartialEq, Clone)]
pub enum CoreResponse {
    /// Core accepted the gateway connection
    Accept {
        dedicated_port: u16,
        dedicated_control_port: u16,
        encrypted_session: i32,
        gateway_id: String,
    },
    /// Core rejected the gateway connection
    Reject { reason: String },
    /// Core is temporarily unavailable
    Unavailable { retry_after_seconds: u32 },
    /// Message should be ignored (wrong session, malformed, etc.)
    Ignore,
}

/// Gateway connection state
#[derive(Debug, Clone, PartialEq)]
pub enum GatewayState {
    Disconnected,
    Connecting,
    Connected,
    Error(String),
}

/// Parses a VEX Core response message
fn parse_core_response(
    message: &str,
    expected_session: i32,
    expected_gateway_id: &str,
) -> CoreResponse {
    let clean_message = message.trim_matches('\0').trim();
    let parts: Vec<&str> = clean_message.split_whitespace().collect();

    if parts.len() < 3 {
        warn!("Malformed core response: insufficient parts");
        return CoreResponse::Ignore;
    }

    // Parse session ID
    let session_id = match parts[0].parse::<i32>() {
        Ok(id) => id,
        Err(_) => {
            warn!("Invalid session ID in core response: {}", parts[0]);
            return CoreResponse::Ignore;
        }
    };

    if session_id != expected_session {
        warn!(
            "Session ID mismatch for expected gateway: {}. Expected: {}, Got: {}, Ignoring Because this is for Gateway: {}",
            expected_session, expected_gateway_id, session_id, parts[1]
        );
        return CoreResponse::Ignore;
    }

    // Parse gateway ID
    let gateway_id = parts[1];
    if gateway_id != expected_gateway_id {
        warn!(
            "Gateway ID mismatch. Expected: {}, Got: {}",
            expected_gateway_id, gateway_id
        );
        return CoreResponse::Ignore;
    }

    // Parse command
    match parts[2] {
        "ACCEPT" if parts.len() == 6 => {
            match (
                parts[3].parse::<u16>(),
                parts[4].parse::<u16>(),
                parts[5].parse::<i32>(),
            ) {
                (Ok(port), Ok(control_port), Ok(encrypted_session)) => CoreResponse::Accept {
                    dedicated_port: port,
                    dedicated_control_port: control_port,
                    encrypted_session,
                    gateway_id: gateway_id.to_string(),
                },
                _ => {
                    error!("Malformed ACCEPT message: invalid parameters");
                    CoreResponse::Ignore
                }
            }
        }
        "REJECT" if parts.len() >= 4 => {
            let reason = parts[3..].join(" ");
            CoreResponse::Reject { reason }
        }
        "UNAVAILABLE" if parts.len() == 4 => match parts[3].parse::<u32>() {
            Ok(retry_after) => CoreResponse::Unavailable {
                retry_after_seconds: retry_after,
            },
            _ => CoreResponse::Ignore,
        },
        _ => {
            warn!("Unknown or malformed core response command: {}", parts[2]);
            CoreResponse::Ignore
        }
    }
}

/// Shared state for fragment handlers
type SharedCoreResponse = Arc<Mutex<Option<CoreResponse>>>;

/// Fragment handler for parsing VEX Core handshake responses
struct HandshakeResponseHandler {
    response: SharedCoreResponse,
    expected_session: i32,
    expected_gateway_id: String,
}

impl AeronFragmentHandlerCallback for HandshakeResponseHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
        let message = String::from_utf8_lossy(buffer);
        debug!(
            "Received handshake response from core: {} (length: {})",
            message,
            buffer.len()
        );

        let parsed =
            parse_core_response(&message, self.expected_session, &self.expected_gateway_id);
        if parsed != CoreResponse::Ignore {
            info!("Valid core response received: {:?}", parsed);
            *self.response.lock().unwrap() = Some(parsed);
        }
    }
}

/// Custom error types for VEX Gateway
#[derive(Error, Debug)]
pub enum GatewayError {
    #[error("Aeron operation failed: {0}")]
    AeronError(#[from] AeronCError),
    #[error("Invalid CString: {0}")]
    NulError(#[from] std::ffi::NulError),
    #[error("Connection timed out: {0}")]
    Timeout(String),
    #[error("VEX Core returned an error: {0}")]
    CoreError(String),
    #[error("Failed to send message: {0}")]
    SendError(String),
    #[error("Protocol error: {0}")]
    ProtocolError(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("Gateway not connected to Server")]
    NotConnected,
    #[error("Invalid gateway state: expected {expected}, got {actual}")]
    InvalidState { expected: String, actual: String },
    #[error("Vex Core Not Started")]
    VexCoreNotStarted,
}

/// Enhanced VEX Gateway for connecting to VEX Core
pub struct VexGateway {
    /// Aeron instance for messaging
    aeron: Aeron,
    /// Gateway configuration
    config: GatewayNetworkingConfig,
    /// Current gateway state
    state: Arc<RwLock<GatewayState>>,
    /// Dedicated session ID for this gateway
    session_id: Option<i32>,
    /// One-time encryption key for session establishment
    encryption_key: Option<i32>,
    /// Publication for sending to core (stored after handshake)
    core_publication: Option<AeronPublication>,
    /// Shutdown flag
    shutdown: Arc<AtomicBool>,
}

impl VexGateway {
    /// Creates a new VEX Gateway instance
    pub fn new(config: GatewayNetworkingConfig) -> Result<Self, GatewayError> {
        // Validate configuration
        if config.gateway_id.is_empty() {
            return Err(GatewayError::ConfigError(
                "Gateway ID cannot be empty".to_string(),
            ));
        }
        if config.max_message_size == 0 {
            return Err(GatewayError::ConfigError(
                "Max message size must be greater than 0".to_string(),
            ));
        }

        // Initialize Aeron context
        let ctx = AeronContext::new()?;
        let context_dir = CString::new(config.context_dir.clone())?;
        ctx.set_dir(&context_dir)?;
        ctx.set_driver_timeout_ms(5_000)?;

        // Create Aeron instance
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        info!(
            "VEX Gateway '{}' initialized successfully",
            config.gateway_id
        );

        Ok(Self {
            aeron,
            config,
            state: Arc::new(RwLock::new(GatewayState::Disconnected)),
            session_id: None,
            encryption_key: None,
            core_publication: None,
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Starts the gateway and establishes connection to VEX Core
    pub fn start<AeronFragmentHandlerHandlerImpl>(
        &mut self,
        handler: AeronFragmentHandlerHandlerImpl,
    ) -> Result<(), GatewayError>
    where
        AeronFragmentHandlerHandlerImpl: AeronFragmentHandlerCallback + Send + 'static,
    {
        info!("Starting VEX Gateway '{}'", self.config.gateway_id);

        // Update state to connecting
        *self.state.write().unwrap() = GatewayState::Connecting;

        // Phase 1: Perform handshake with VEX Core
        let (dedicated_port, dedicated_control_port, session_id) = self.perform_handshake()?;

        info!(
            "Gateway '{}': Handshake successful. Port: {}, Control Port: {}, Session ID: {}",
            self.config.gateway_id, dedicated_port, dedicated_control_port, session_id
        );

        // Phase 2: Establish dedicated communication channel
        self.establish_dedicated_channel(
            dedicated_port,
            dedicated_control_port,
            session_id,
            handler,
        )?;

        // Update state to Connected
        *self.state.write().unwrap() = GatewayState::Connected;

        info!(
            "VEX Gateway '{}' successfully connected and authenticated",
            self.config.gateway_id
        );
        Ok(())
    }

    /// Performs initial handshake with VEX Core
    fn perform_handshake(&mut self) -> Result<(u16, u16, i32), GatewayError> {
        // Create publication and subscription for handshake
        let publication = new_publication(
            &self.aeron,
            &self.config.core_address,
            self.config.core_port,
            ALL_GATEWAYS_STREAM_ID,
        )?;

        let subscription = new_subscription_with_mdc(
            &self.aeron,
            &self.config.core_address,
            self.config.core_control_port,
            ALL_GATEWAYS_STREAM_ID,
        )?;

        // Wait for publication to be connected
        self.wait_for_publication_connection(&publication, "handshake")?;

        let session_id = publication.session_id();
        self.session_id = Some(session_id);

        // Generate encryption key for secure session establishment
        let encryption_key = rand::random::<i32>();
        self.encryption_key = Some(encryption_key);

        info!(
            "Gateway '{}': Connected to handshake channel with session ID: {}",
            self.config.gateway_id, session_id
        );

        // Send HELLO message with gateway identification
        let hello_msg = format!("HELLO {} {}", self.config.gateway_id, encryption_key);
        self.send_message_with_retries(&publication, &hello_msg)?;

        // Wait for VEX Core response
        let response = self.wait_for_core_response(&subscription, session_id)?;

        match response {
            CoreResponse::Accept {
                dedicated_port,
                dedicated_control_port,
                encrypted_session,
                ..
            } => {
                // Decrypt the session ID
                let decrypted_session = encrypted_session ^ encryption_key;
                Ok((dedicated_port, dedicated_control_port, decrypted_session))
            }
            CoreResponse::Reject { reason } => Err(GatewayError::CoreError(format!(
                "Connection rejected: {reason}"
            ))),
            CoreResponse::Unavailable {
                retry_after_seconds,
            } => Err(GatewayError::CoreError(format!(
                "Core unavailable, retry after {retry_after_seconds} seconds"
            ))),
            _ => Err(GatewayError::ProtocolError(
                "Unexpected core response".to_string(),
            )),
        }
    }

    /// Establishes dedicated communication channel with VEX Core
    fn establish_dedicated_channel<AeronFragmentHandlerHandlerImpl>(
        &mut self,
        port: u16,
        control_port: u16,
        session_id: i32,
        handler: AeronFragmentHandlerHandlerImpl,
    ) -> Result<(), GatewayError>
    where
        AeronFragmentHandlerHandlerImpl: AeronFragmentHandlerCallback + Send + 'static,
    {
        info!(
            "Gateway '{}': Establishing dedicated channel with session ID: {}",
            self.config.gateway_id, session_id
        );

        // Create subscription for receiving core messages
        let subscription = new_subscription_with_mdc_and_session(
            &self.aeron,
            &self.config.core_address,
            control_port,
            GATEWAY_CORE_STREAM_ID,
            session_id,
        )?;

        // Create publication for sending messages to core
        let publication = new_publication_with_session(
            &self.aeron,
            &self.config.core_address,
            port,
            GATEWAY_CORE_STREAM_ID,
            session_id,
        )?;

        // Store publication for further sending Order Command
        self.core_publication = Some(publication.clone());

        // Wait for connections
        self.wait_for_channel_connections(&publication, &subscription)?;

        info!(
            "Gateway '{}': Successfully established dedicated channel",
            self.config.gateway_id
        );

        // Start polling for messages in a separate thread
        self.start_message_polling(subscription, handler)
    }

    /// Starts polling for incoming messages in a separate thread
    fn start_message_polling<AeronFragmentHandlerHandlerImpl>(
        &self,
        subscription: AeronSubscription,
        handler: AeronFragmentHandlerHandlerImpl,
    ) -> Result<(), GatewayError>
    where
        AeronFragmentHandlerHandlerImpl: AeronFragmentHandlerCallback + Send + 'static,
    {
        // Start polling thread
        let gateway_id = self.config.gateway_id.clone();
        let shutdown = self.shutdown.clone();
        std::thread::spawn(move || {
            let mut handler = Handler::leak(handler);

            info!("Gateway '{}': Started message polling thread", gateway_id);
            while !shutdown.load(Ordering::SeqCst) {
                if let Err(e) = subscription.poll(Some(&handler), 10) {
                    error!("Gateway '{}': Error polling messages: {}", gateway_id, e);
                    break;
                }
                AeronIdleStrategy::busy_spinning_idle(std::ptr::null_mut(), 0);
            }
            handler.release();
        });

        Ok(())
    }

    /// Waits for VEX Core response during handshake
    fn wait_for_core_response(
        &self,
        subscription: &rusteron_client::AeronSubscription,
        session_id: i32,
    ) -> Result<CoreResponse, GatewayError> {
        let shared_response = Arc::new(Mutex::new(None));
        let fragment_handler = HandshakeResponseHandler {
            response: shared_response.clone(),
            expected_session: session_id,
            expected_gateway_id: self.config.gateway_id.clone(),
        };

        let mut handler = Handler::leak(fragment_handler);

        let start = Instant::now();
        while start.elapsed() < CONNECT_TIMEOUT {
            subscription.poll(Some(&handler), 10)?;
            if let Some(response) = shared_response.lock().unwrap().take() {
                return Ok(response);
            }
            // Sleeping breifly here. Larfer sleep as latency is not critical during handshake
            std::thread::sleep(Duration::from_millis(10));
        }
        handler.release();
        Err(GatewayError::Timeout(
            "Waiting for core handshake response".to_string(),
        ))
    }

    /// Waits for publication connection with timeout
    fn wait_for_publication_connection(
        &self,
        publication: &rusteron_client::AeronPublication,
        context: &str,
    ) -> Result<(), GatewayError> {
        let start = Instant::now();
        while !publication.is_connected() {
            if start.elapsed() > CONNECT_TIMEOUT {
                return Err(GatewayError::Timeout(format!(
                    "Connecting {context} publication timed out",
                )));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        Ok(())
    }

    /// Waits for both publication and subscription connections
    fn wait_for_channel_connections(
        &self,
        publication: &rusteron_client::AeronPublication,
        subscription: &rusteron_client::AeronSubscription,
    ) -> Result<(), GatewayError> {
        let start = Instant::now();
        while !publication.is_connected() || !subscription.is_connected() {
            if start.elapsed() > CONNECT_TIMEOUT {
                return Err(GatewayError::Timeout(
                    "Connecting to dedicated channel timed out".to_string(),
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        Ok(())
    }

    /// Sends a message with automatic retries
    fn send_message_with_retries(
        &mut self,
        publication: &rusteron_client::AeronPublication,
        text: &str,
    ) -> Result<(), GatewayError> {
        debug!(
            "Gateway '{}': Sending message: {}",
            self.config.gateway_id, text
        );

        let value = text.as_bytes();
        if value.len() > self.config.max_message_size {
            return Err(GatewayError::SendError(format!(
                "Message too long: {} bytes (max: {})",
                value.len(),
                self.config.max_message_size
            )));
        }

        // Retry sending with exponential backoff
        for attempt in 0..MESSAGE_RETRY_COUNT {
            let result = publication.offer::<AeronReservedValueSupplierLogger>(value, None);

            if result >= 0 {
                return Ok(());
            }

            // Wait before retrying with exponential backoff
            let delay = MESSAGE_RETRY_DELAY * (2_u32.pow(attempt as u32));
            std::thread::sleep(delay);
        }

        Err(GatewayError::SendError(format!(
            "Failed to send message after {MESSAGE_RETRY_COUNT} attempts",
        )))
    }

    /// Gets current gateway state
    pub fn state(&self) -> GatewayState {
        self.state.read().unwrap().clone()
    }

    /// Gets gateway configuration
    pub fn config(&self) -> &GatewayNetworkingConfig {
        &self.config
    }

    /// Checks if gateway is connected and authenticated
    pub fn is_connected(&self) -> bool {
        matches!(*self.state.read().unwrap(), GatewayState::Connected)
    }

    /// Gracefully shuts down the gateway
    pub async fn shutdown(&mut self) -> Result<(), GatewayError> {
        info!("Shutting down VEX Gateway '{}'", self.config.gateway_id);

        // Update state
        *self.state.write().unwrap() = GatewayState::Disconnected;

        // Set shutdown flag
        self.shutdown.store(true, Ordering::SeqCst);

        info!(
            "VEX Gateway '{}' shut down successfully",
            self.config.gateway_id
        );
        Ok(())
    }

    /// Sends an OrderCommand to the core
    pub fn send_order_command(&mut self, order_command: &OrderCommand) -> Result<(), GatewayError> {
        // Check if we're connected
        if !self.is_connected() {
            return Err(GatewayError::NotConnected);
        }

        let publication = self
            .core_publication
            .as_ref()
            .ok_or(GatewayError::VexCoreNotStarted)?;

        // Serialize OrderCommand
        let mut buffer = vec![0u8; self.config.max_message_size];
        encode_order_command(order_command.clone(), &mut buffer).map_err(|e| {
            GatewayError::ProtocolError(format!("Failed to encode OrderCommand: {e:?}"))
        })?;

        // Send the binary message directly
        debug!(
            "Gateway '{}': Sending OrderCommand: {:?}",
            self.config.gateway_id, order_command
        );

        // // Calculate actual encoded size (you may need to adjust this based on your encoding)
        // let encoded_size = std::cmp::min(buffer.len(), self.config.max_message_size);

        // Send using the buffer directly
        for attempt in 0..MESSAGE_RETRY_COUNT {
            let result = publication.offer::<AeronReservedValueSupplierLogger>(&buffer, None);

            if result >= 0 {
                return Ok(());
            }

            // Wait before retrying with exponential backoff
            let delay = MESSAGE_RETRY_DELAY * (2_u32.pow(attempt as u32));
            std::thread::sleep(delay);
        }

        Err(GatewayError::SendError(format!(
            "Failed to send OrderCommand after {MESSAGE_RETRY_COUNT} attempts",
        )))
    }

    /// get gateway ID
    pub fn gateway_id(&self) -> &str {
        &self.config.gateway_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_core_response_accept() {
        let response = parse_core_response(
            "12345 gateway-1 ACCEPT 40003 40004 98765",
            12345,
            "gateway-1",
        );
        assert_eq!(
            response,
            CoreResponse::Accept {
                dedicated_port: 40003,
                dedicated_control_port: 40004,
                encrypted_session: 98765,
                gateway_id: "gateway-1".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_core_response_reject() {
        let response = parse_core_response(
            "12345 gateway-1 REJECT Invalid credentials",
            12345,
            "gateway-1",
        );
        assert_eq!(
            response,
            CoreResponse::Reject {
                reason: "Invalid credentials".to_string()
            }
        );
    }

    #[test]
    fn test_parse_core_response_ignore_wrong_session() {
        let response = parse_core_response(
            "99999 gateway-1 ACCEPT 40003 40004 98765",
            12345,
            "gateway-1",
        );
        assert_eq!(response, CoreResponse::Ignore);
    }
}
