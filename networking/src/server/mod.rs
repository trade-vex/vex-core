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
//! ```rust,no_run
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

use crate::server::gateway_handler::{
    GatewayImageAvailableHandler, GatewayImageUnavailableHandler, HandshakeMessageHandler,
};
use crate::server::gateway_manager::GatewayManager;
use crate::utils::{new_publication_with_mdc, new_subscription_with_handlers};
use common::cmd::OrderCommand;
use crossbeam::utils::CachePadded;
use disruptor::{MultiConsumerBarrier, MultiProducer};
use rusteron_client::{Aeron, AeronCError, AeronContext, Handler};
use rusteron_media_driver::AeronIdleStrategy;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::{error, info, instrument};
use vex_config::CoreNetworkingConfig;

/// Stream ID for gateway communication
const ALL_GATEWAYS_STREAM_ID: i32 = 1001;

/// Cleanup interval for expired gateways
const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

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
    aeron: Rc<Aeron>,
    /// Core configuration
    config: CoreNetworkingConfig,
    /// Gateway state management (lock-free)
    gateways: Rc<GatewayManager>,
    /// Last cleanup timestamp (atomic)
    last_cleanup_nanos: CachePadded<AtomicU64>,
    /// shutdown flag
    shutdown: AtomicBool,
}

impl VexCoreServer {
    /// Creates a new VEX Core instance
    pub fn new(
        config: CoreNetworkingConfig,
        producer: MultiProducer<OrderCommand, MultiConsumerBarrier>,
    ) -> Result<Self, ServerError> {
        // Validate configuration
        Self::validate_config(&config)?;

        // Initialize Aeron context
        let aeron = Self::initialize_aeron(&config)?;

        info!("VEX Core '{}' initialized successfully", config.core_id);

        let aeron = Rc::new(aeron);

        let now_nanos = Instant::now().elapsed().as_nanos() as u64;

        Ok(Self {
            aeron: Rc::clone(&aeron),
            gateways: Rc::new(GatewayManager::new(config.clone(), aeron, producer)?),
            config,
            last_cleanup_nanos: CachePadded::new(AtomicU64::new(now_nanos)),
            shutdown: AtomicBool::new(false),
        })
    }

    /// Starts the VEX Core server
    #[instrument(skip(self))]
    pub fn start(&mut self) -> Result<(), ServerError> {
        // Create publication for sending responses to gateways
        let (subscription, handshake_handler) = self.setup_networking()?;

        info!("VEX Core '{}' started successfully", self.config.core_id);

        let mut handler = Handler::leak(handshake_handler);
        // Main event loop
        while !self.shutdown.load(Ordering::SeqCst) {
            // Process incoming handshake messages
            subscription.poll(Some(&handler), 10)?;

            // Poll all active gateway sessions (lock-free)
            if let Err(e) = self.gateways.poll() {
                error!("Error polling gateways: {}", e);
            }

            // Perform periodic cleanup
            self.periodic_cleanup()?;

            AeronIdleStrategy::busy_spinning_idle(std::ptr::null_mut(), 0);
        }
        handler.release();
        Ok(())
    }

    /// Performs periodic cleanup of expired gateways (lock-free)
    fn periodic_cleanup(&mut self) -> Result<(), ServerError> {
        let now_nanos = Instant::now().elapsed().as_nanos() as u64;
        let last_cleanup_nanos = self.last_cleanup_nanos.load(Ordering::Relaxed);
        let cleanup_interval_nanos = CLEANUP_INTERVAL.as_nanos() as u64;

        if now_nanos.saturating_sub(last_cleanup_nanos) >= cleanup_interval_nanos {
            // Try to update cleanup timestamp atomically
            if self
                .last_cleanup_nanos
                .compare_exchange_weak(
                    last_cleanup_nanos,
                    now_nanos,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                info!("Performing periodic cleanup");

                // Clean up expired gateways (lock-free)
                match self.gateways.cleanup_expired_gateways() {
                    Ok(cleanup_count) => {
                        if cleanup_count > 0 {
                            info!("Cleaned up {} expired gateways", cleanup_count);
                        }
                    }
                    Err(e) => {
                        error!("Error during gateway cleanup: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Gets core configuration
    pub fn config(&self) -> &CoreNetworkingConfig {
        &self.config
    }

    /// Checks if a gateway is connected (lock-free)
    pub fn is_gateway_connected(&self, gateway_id: &str) -> bool {
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
    ) -> Result<(rusteron_client::AeronSubscription, HandshakeMessageHandler), ServerError> {
        // Create publication for responses
        let publication = new_publication_with_mdc(
            &self.aeron,
            &self.config.local_address,
            self.config.initial_control_port,
            ALL_GATEWAYS_STREAM_ID,
        )?;

        // Create image handlers
        let image_available_handler = GatewayImageAvailableHandler::new(Rc::clone(&self.gateways));
        let image_unavailable_handler =
            GatewayImageUnavailableHandler::new(Rc::clone(&self.gateways));

        // Create subscription for handshakes
        let subscription = new_subscription_with_handlers(
            &self.aeron,
            &self.config.local_address,
            self.config.initial_port,
            ALL_GATEWAYS_STREAM_ID,
            image_available_handler,
            image_unavailable_handler,
        )?;

        // Create handshake handler
        let handshake_handler =
            HandshakeMessageHandler::new(Rc::clone(&self.gateways), publication);

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
