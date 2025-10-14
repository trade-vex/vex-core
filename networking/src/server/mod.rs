//! VEX Core Server Implementation
//!
//! This module provides the main server implementation for the VEX Core,
//! which manages gateway connections and handles high-throughput messaging
//! using the Aeron transport protocol.
//!
//! ## Key Features
//! - High-performance gateway connection management
//! - Concurrent session handling with lock-free data structures
//! - Automatic resource cleanup and connection lifecycle management
//! - Configurable connection limits and authentication
//!
//! ## Rchitecture
//! The server is built around three main components:
//! - `VexCoreServer`: Main server orchestrating connections and cleanup
//! - `GatewayManager`: Handles individual gateway sessions and handshakes
//! - Message handlers: Process Aeron image and fragment events
//!
//! ## Usage
//! ```ignore, rust,no_run
//! use networking::server::{VexCoreServer};
//! use vex_config::CoreNetworkingConfig;
//!
//! let config = CoreNetworkingConfig::test_defaults();
//! let mut server = VexCoreServer::new(config).unwrap();
//! server.start().unwrap(); // Runs indefinitely
//! ```

mod cmd_handler;
mod duologue;
mod gateway_handler;
mod gateway_manager;
mod gateway_publications;

use crate::server::gateway_handler::{
    GatewayImageAvailableHandler, GatewayImageUnavailableHandler, HandshakeMessageHandler,
};
use crate::server::gateway_manager::GatewayManager;
use crate::utils::{new_publication_with_mdc, new_subscription_with_handlers};
use common::OrderCommand;
use disruptor::{MultiProducer, SingleConsumerBarrier};
use rusteron_client::{Aeron, AeronCError, AeronContext, Handler};
use rusteron_media_driver::AeronIdleStrategy;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use thiserror::Error;
use tracing::{error, info, instrument};
use vex_config::CoreNetworkingConfig;

pub use gateway_publications::GatewayPublications;

/// Stream ID for gateway communication
const ALL_GATEWAYS_STREAM_ID: i32 = 1001;

/// Error types for VEX Core server operations
#[derive(Error, Debug)]
pub enum ServerError {
    #[error("Aeron connection failed: {0}")]
    AeronConnectionError(#[from] AeronCError),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Invalid input: {0}")]
    InvalidInput(#[from] std::ffi::NulError),
    #[error("Resource allocation error: {0}")]
    ResourceAllocationError(String),
    #[error("Gateway message error: {0}")]
    GatewayMessageError(String),
    #[error("Authentication failed: {0}")]
    AuthenticationError(String),
    #[error("Capacity exceeded: {0}")]
    CapacityExceededError(String),
    #[error("Configuration error: {0}")]
    ConfigurationError(String),
}

/// Enhanced VEX Core server for handling gateway connections
pub struct VexCoreServer {
    /// Aeron instance for messaging
    aeron: Arc<Aeron>,
    /// Core configuration
    config: CoreNetworkingConfig,
    /// Gateway state management (lock-free)
    gateways: Arc<GatewayManager>,
    /// shutdown flag
    shutdown: AtomicBool,
    /// Image available handler
    image_available_handler: Option<Handler<GatewayImageAvailableHandler>>,
    /// Image unavailable handler
    image_unavailable_handler: Option<Handler<GatewayImageUnavailableHandler>>,
}

impl VexCoreServer {
    /// Creates a new VEX Core instance
    pub fn new(
        config: CoreNetworkingConfig,
        producer: MultiProducer<OrderCommand, SingleConsumerBarrier>,
        publications: Arc<GatewayPublications>,
    ) -> Result<Self, ServerError> {
        // Validate configuration
        Self::validate_config(&config)?;

        // Initialize Aeron context
        let aeron = Self::initialize_aeron(&config)?;

        info!("VEX Core '{}' initialized successfully", config.core_id);

        #[allow(clippy::arc_with_non_send_sync)]
        let aeron = Arc::new(aeron);

        Ok(Self {
            aeron: Arc::clone(&aeron),
            #[allow(clippy::arc_with_non_send_sync)]
            gateways: Arc::new(GatewayManager::new(
                config.clone(),
                aeron,
                producer,
                publications,
            )?),
            config,
            shutdown: AtomicBool::new(false),
            image_available_handler: None,
            image_unavailable_handler: None,
        })
    }

    /// Starts the VEX Core server
    #[instrument(skip(self))]
    pub fn start(&mut self) -> Result<(), ServerError> {
        let image_available_handler = Handler::leak(GatewayImageAvailableHandler);
        let image_unavailable_handler = Handler::leak(GatewayImageUnavailableHandler);

        let (subscription, handshake_handler) =
            self.setup_networking(&image_available_handler, &image_unavailable_handler)?;

        self.image_available_handler = Some(image_available_handler);
        self.image_unavailable_handler = Some(image_unavailable_handler);

        info!("VEX Core '{}' started successfully", self.config.core_id);

        let mut handler = Handler::leak(handshake_handler);
        // Main event loop
        while !self.shutdown.load(Ordering::SeqCst) {
            // incoming handshake messages
            if let Err(e) = subscription.poll(Some(&handler), 10) {
                error!("Error polling subscription: {}", e);
            }

            // poll order command from gateways
            if let Err(e) = self.gateways.poll() {
                error!("Error polling gateways: {}", e);
            }

            AeronIdleStrategy::busy_spinning_idle(std::ptr::null_mut(), 0);
        }
        handler.release();
        Ok(())
    }

    /// Gets core configuration
    pub fn config(&self) -> &CoreNetworkingConfig {
        &self.config
    }

    /// Checks if a gateway is connected (lock-free)
    pub fn is_gateway_connected(&self, gateway_id: u8) -> bool {
        self.gateways.is_gateway_connected(gateway_id)
    }

    /// Gracefully shuts down the core server
    ///
    /// Closes all active gateway connections and cleans up resources.
    ///
    /// # Returns
    /// * `Result<(), ServerError>` - Success or shutdown error
    pub fn shutdown(&mut self) -> Result<(), ServerError> {
        info!("Shutting down VEX Core '{}'", self.config.core_id);
        self.gateways.shutdown_all_gateways()?;
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(mut handler) = self.image_available_handler.take() {
            handler.release();
        }
        if let Some(mut handler) = self.image_unavailable_handler.take() {
            handler.release();
        }
        info!("VEX Core '{}' shut down successfully", self.config.core_id);
        Ok(())
    }

    // Private helper methods

    /// Validates the core configuration
    fn validate_config(config: &CoreNetworkingConfig) -> Result<(), ServerError> {
        if config.max_gateways == 0 {
            return Err(ServerError::ConfigurationError(
                "Max gateways must be greater than 0".to_string(),
            ));
        }
        if config.core_id.is_empty() {
            return Err(ServerError::ConfigurationError(
                "Core ID cannot be empty".to_string(),
            ));
        }
        Ok(())
    }

    /// Initializes the Aeron messaging system
    fn initialize_aeron(config: &CoreNetworkingConfig) -> Result<Aeron, ServerError> {
        let ctx = AeronContext::new()?;
        let context_dir = std::ffi::CString::new(config.context_dir.clone())?;

        info!(
            "VEX Core '{}' context_dir: {:?}",
            config.core_id, context_dir
        );

        ctx.set_dir(&context_dir)?;
        ctx.set_driver_timeout_ms(5_000)?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        Ok(aeron)
    }

    /// Sets up networking components for gateway communication
    fn setup_networking(
        &self,
        image_available_handler: &Handler<GatewayImageAvailableHandler>,
        image_unavailable_handler: &Handler<GatewayImageUnavailableHandler>,
    ) -> Result<(rusteron_client::AeronSubscription, HandshakeMessageHandler), ServerError> {
        // Create publication for responses
        let publication = new_publication_with_mdc(
            &self.aeron,
            &self.config.local_address,
            self.config.initial_control_port,
            ALL_GATEWAYS_STREAM_ID,
        )?;

        // Create subscription for handshakes
        let subscription = new_subscription_with_handlers(
            &self.aeron,
            &self.config.local_address,
            self.config.initial_port,
            ALL_GATEWAYS_STREAM_ID,
            Some(image_available_handler),
            Some(image_unavailable_handler),
        )?;

        // Create handshake handler
        let handshake_handler =
            HandshakeMessageHandler::new(Arc::clone(&self.gateways), publication);

        Ok((subscription, handshake_handler))
    }

    /// Number of connected gateways
    pub fn connected_gateway_count(&self) -> usize {
        self.gateways.active_gateways_count()
    }

    /// Checks if there are no connected gateways
    pub fn is_empty(&self) -> bool {
        self.gateways.is_empty()
    }
}
