use std::ffi::CString;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rusteron_client::{
    Aeron, AeronCError, AeronContext, AeronFragmentAssembler, AeronFragmentHandlerCallback,
    AeronHeader, AeronPublication, AeronReservedValueSupplierLogger, AeronSubscription,
    Handler, AeronAvailableImageLogger, AeronUnavailableImageLogger,
};
use thiserror::Error;
use tracing::{debug, error, info};

const ECHO_STREAM_ID: i32 = 1002;

/// Fragment handler for parsing incoming messages
struct FragmentHandler;

impl AeronFragmentHandlerCallback for FragmentHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
        let message = String::from_utf8_lossy(buffer);
        debug!("client received message: {}", message);
    }
}

/// Error types for the EchoClient
#[derive(Error, Debug)]
pub enum EchoClientError {
    #[error("Aeron connection failed: {0}")]
    AeronConnectionError(#[from] AeronCError),

    #[error("Subscription setup failed: {0}")]
    SubscriptionError(String),

    #[error("Publication setup failed: {0}")]
    PublicationError(String),

    #[error("Invalid input: {0}")]
    InvalidInput(#[from] std::ffi::NulError),

    #[error("Message send failed: {0}")]
    SendError(String),

    #[error("Client not connected")]
    NotConnected,

    #[error("Empty message")]
    EmptyMessage,
}

/// A mindlessly simple Echo client
pub struct EchoClient {
    aeron: Arc<Aeron>,
    local_address: SocketAddr,
    remote_address: SocketAddr,
    buffer: [u8; 2048],
}

impl EchoClient {
    /// Create a new client
    ///
    /// # Arguments
    /// * `context_dir` - The directory used for the underlying media driver
    /// * `local_address` - The local address used by the client
    /// * `remote_address` - The address of the server to which the client will connect
    ///
    /// # Returns
    /// A new client instance
    pub fn create(
        context_dir: &str,
        local_address: SocketAddr,
        remote_address: SocketAddr,
    ) -> Result<Self, EchoClientError> {
        let ctx = AeronContext::new()?;
        let context_dir = CString::new(context_dir)?;
        ctx.set_dir(&context_dir)?;
        ctx.set_driver_timeout_ms(1_000)?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;
        info!("client started");

        let buffer = [0u8; 2048];

        Ok(Self {
            aeron: Arc::new(aeron),
            local_address,
            remote_address,
            buffer,
        })
    }

    /// Run the client
    pub fn run(&mut self) -> Result<(), EchoClientError> {
        let subscription = self.setup_subscription()?;
        let publication = self.setup_publication()?;
        
        self.run_loop(subscription, publication)
    }

    /// Main client loop
    fn run_loop(
        &mut self,
        subscription: AeronSubscription,
        publication: AeronPublication,
    ) -> Result<(), EchoClientError> {
        // Try repeatedly to send an initial HELLO message
        loop {
            if publication.is_connected() {
                let hello_msg = format!("HELLO {}", self.local_address.port());
                if self.send_message(&publication, &hello_msg)? {
                    debug!("Successfully sent HELLO message");
                    break;
                }
            }
            thread::sleep(Duration::from_millis(1000));
        }

        // Send an infinite stream of random unsigned integers
        let fragment_handler = FragmentHandler;
        let fragment_handler = AeronFragmentAssembler::new(
            Some(&Handler::leak(fragment_handler)),
        )?;
        let fragment_handler = Handler::leak(fragment_handler);

        let mut counter = 0u32;
        loop {
            if publication.is_connected() {
                let message = format!("{}", counter);
                if let Err(e) = self.send_message(&publication, &message) {
                    error!("Failed to send message: {}", e);
                }
                counter = counter.wrapping_add(1);
            }

            if subscription.is_connected() {
                subscription.poll(Some(&fragment_handler), 10)?;
            }

            thread::sleep(Duration::from_millis(1000));
        }
    }

    /// Send a message via the publication
    fn send_message(
        &mut self,
        publication: &AeronPublication,
        text: &str,
    ) -> Result<bool, EchoClientError> {
        if text.is_empty() {
            return Err(EchoClientError::EmptyMessage);
        }

        debug!("client: sending message: {}", text);

        let value = text.as_bytes();
        if value.len() > self.buffer.len() {
            return Err(EchoClientError::SendError("Message too long".to_string()));
        }

        self.buffer[..value.len()].copy_from_slice(value);

        for _ in 0..5 {
            let result = publication.offer::<AeronReservedValueSupplierLogger>(
                &self.buffer[..value.len()],
                None,
            );
            if result >= 0 {
                return Ok(true);
            }
            thread::sleep(Duration::from_millis(100));
        }

        error!("could not send message after 5 attempts");
        Ok(false)
    }

    /// Setup the publication for sending messages
    fn setup_publication(&self) -> Result<AeronPublication, EchoClientError> {
        let pub_uri = CString::new(format!(
            "aeron:udp?endpoint={}",
            self.remote_address
        ))?;

        debug!("publication URI: {:?}", pub_uri);

        let publication = self
            .aeron
            .add_publication(&pub_uri, ECHO_STREAM_ID, Duration::from_secs(1))
            .map_err(|e| EchoClientError::PublicationError(e.to_string()))?;

        Ok(publication)
    }

    /// Setup the subscription for receiving messages
    fn setup_subscription(&self) -> Result<AeronSubscription, EchoClientError> {
        let sub_uri = CString::new(format!(
            "aeron:udp?endpoint={}",
            self.local_address
        ))?;

        debug!("subscription URI: {:?}", sub_uri);

        let available_logger = AeronAvailableImageLogger {};
        let available_handler = Handler::leak(available_logger);
        let unavailable_logger = AeronUnavailableImageLogger {};
        let unavailable_handler = Handler::leak(unavailable_logger);

        let subscription = self
            .aeron
            .add_subscription(
                &sub_uri,
                ECHO_STREAM_ID,
                Some(&available_handler),
                Some(&unavailable_handler),
                Duration::from_secs(1),
            )?;
        Ok(subscription)
    }
}

// Legacy compatibility - keeping the old types for backward compatibility
pub type VexClient = EchoClient;
pub type ClientError = EchoClientError;

impl VexClient {
    pub fn new(
        context_dir: &str,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
    ) -> Result<Self, ClientError> {
        // Note: stream_id parameter is ignored, we use ECHO_STREAM_ID constant
        Self::create(context_dir, local_addr, remote_addr)
    }
}