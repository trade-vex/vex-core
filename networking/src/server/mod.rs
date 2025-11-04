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
mod replay;

use crate::server::cmd_handler::ReplayFragmentHandler;
use crate::server::gateway_handler::{
    GatewayImageAvailableHandler, GatewayImageUnavailableHandler, HandshakeMessageHandler,
};
use crate::server::gateway_manager::GatewayManager;
use crate::server::replay::{ActiveRecordingReader, ExtendedRecordingDescriptor, RecorderDescriptorReader};
use crate::utils::{new_publication_with_mdc, new_subscription_with_handlers};
use common::{FRAMESIZE, OrderCommand};
use disruptor::{MultiProducer, SingleConsumerBarrier};
use rusteron_archive::{
    Aeron, AeronArchiveAsyncConnect, AeronArchiveReplayParams, AeronAvailableImageLogger,
    AeronCError, AeronContext, AeronNotificationLogger, AeronSubscription,
    AeronUnavailableImageLogger, Handler, IntoCString, SourceLocation,
};
use rusteron_archive::{AeronArchive, AeronArchiveContext};
use rusteron_media_driver::AeronIdleStrategy;
use std::i32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, error, info};
use vex_config::CoreNetworkingConfig;

pub use gateway_publications::Publications;

/// Stream ID for gateway communication
const ALL_GATEWAYS_STREAM_ID: i32 = 1001;

/// Recording stream ID for Aeron Archive
const RECORDING_STREAM_ID: i32 = 2001;
/// Replay Stram ID for Aeron Archive
pub const REPLAY_STREAM_ID: i32 = 2002;
/// Channel for Aeron Recording also known as Aeron Control Channel
const RECORDING_CHANNEL: &str = "aeron:ipc";

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
    /// Core configuration
    config: CoreNetworkingConfig,
    /// Gateway state management (lock-free)
    gateways: Arc<GatewayManager>,
    /// Shared shutdown flag
    shutdown: Arc<AtomicBool>,
    /// Image available handler
    image_available_handler: Handler<GatewayImageAvailableHandler>,
    /// Image unavailable handler
    image_unavailable_handler: Handler<GatewayImageUnavailableHandler>,
    /// Handshake message handler
    handshake_handler: Handler<HandshakeMessageHandler>,
    /// Subscription for handshake messages
    subscription: AeronSubscription,
    /// Archive Client
    archive: AeronArchive,
    /// Subscription ID for recording
    subscription_id: i64,
}

impl VexCoreServer {
    /// Creates a new VEX Core instance
    pub fn new(
        config: CoreNetworkingConfig,
        producer: MultiProducer<OrderCommand, SingleConsumerBarrier>,
        publications: Arc<Publications>,
        replay: bool,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Self, ServerError> {
        // Validate configuration
        Self::validate_config(&config)?;

        // Initialize Aeron context
        let aeron = Self::initialize_aeron(&config)?;

        // Initialize Aeron Archive
        let archive = Self::initialize_archive(&config, &aeron)?;

        // Replay
        let recording = if replay {
            Self::start_replay(&aeron, &archive, producer.clone(), Arc::clone(&shutdown))?
        } else {
            None
        };

        // Start recording
        let (subscription_id, channel) = Self::start_recording(&archive, recording)?;

        // Publisher for Recording the incoming messages
        let archive_publication = aeron.add_publication(
            &channel.into_c_string(),
            RECORDING_STREAM_ID,
            Duration::from_secs(1),
        )?;

        // wait for publication to be connected
        while !archive_publication.is_connected() {
            std::thread::sleep(Duration::from_millis(100));
            info!(
                target: "core_server",
                action = "archive_publication_wait",
                core_id = %config.core_id
            );
        }

        publications.set_archive_publication(archive_publication);

        info!(
            target: "core_server",
            action = "initialized",
            archive_recording = true,
            core_id = %config.core_id
        );

        let image_available_handler = Handler::leak(GatewayImageAvailableHandler);
        let image_unavailable_handler = Handler::leak(GatewayImageUnavailableHandler);

        let publication = new_publication_with_mdc(
            &aeron,
            &config.local_address,
            config.initial_control_port,
            ALL_GATEWAYS_STREAM_ID,
        )?;

        // Create subscription for handshakes
        let subscription = new_subscription_with_handlers(
            &aeron,
            &config.local_address,
            config.initial_port,
            ALL_GATEWAYS_STREAM_ID,
            Some(&image_available_handler),
            Some(&image_unavailable_handler),
        )?;

        let gateways = Arc::new(GatewayManager::new(
            config.clone(),
            aeron,
            producer,
            publications,
        )?);

        // Create handshake handler
        let handshake_handler = HandshakeMessageHandler::new(Arc::clone(&gateways), publication);

        Ok(Self {
            gateways,
            config,
            subscription,
            handshake_handler: Handler::leak(handshake_handler),
            shutdown,
            image_available_handler,
            image_unavailable_handler,
            subscription_id,
            archive,
        })
    }

    /// Starts the VEX Core server
    pub fn start(&mut self) -> Result<(), ServerError> {
        info!(
            target: "core_server",
            action = "started",
            core_id = %self.config.core_id
        );

        // Main Message Polling Loop
        // 1. Listens for new handhakes
        // 2. Listens for new orders
        loop {
            if self.shutdown.load(Ordering::Acquire) {
                return self.shutdown();
            }

            if let Err(e) = self.subscription.poll(Some(&self.handshake_handler), 10) {
                error!(
                    target: "core_server",
                    action = "poll_subscription_failed",
                    core_id = %self.config.core_id,
                    error = %e
                );
            }

            if let Err(e) = self.gateways.poll() {
                error!(
                    target: "core_server",
                    action = "poll_gateways_failed",
                    core_id = %self.config.core_id,
                    error = %e
                );
            }

            AeronIdleStrategy::busy_spinning_idle(std::ptr::null_mut(), 0);
        }
    }

    /// Gracefully shuts down the core server
    ///
    /// Closes all active gateway connections and cleans up resources.
    /// This method should only be called when the shutdown flag is already set.
    ///
    /// # Returns
    /// * `Result<(), ServerError>` - Success or shutdown error
    pub fn shutdown(&mut self) -> Result<(), ServerError> {
        info!(
            target: "core_server",
            action = "shutdown_requested",
            core_id = %self.config.core_id
        );

        self.gateways.shutdown_all_gateways()?;
        self.subscription.close::<AeronNotificationLogger>(None)?;
        self.image_available_handler.release();
        self.image_unavailable_handler.release();
        self.handshake_handler.release();
        self.archive
            .stop_recording_subscription(self.subscription_id)?;
        self.archive.close()?;

        info!(
            target: "core_server",
            action = "shutdown_complete",
            core_id = %self.config.core_id
        );
        Ok(())
    }

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

    fn initialize_archive(
        config: &CoreNetworkingConfig,
        aeron: &Aeron,
    ) -> Result<AeronArchive, ServerError> {
        let archive_ctx = AeronArchiveContext::new_with_no_credentials_supplier(
            &aeron,
            &config.request_control_channel,
            &config.response_control_channel,
            &RECORDING_CHANNEL,
        )?;

        let archive_async_connect = AeronArchiveAsyncConnect::new_with_aeron(&archive_ctx, &aeron)?;
        let archive = archive_async_connect.poll_blocking(Duration::from_secs(10))?;
        Ok(archive)
    }

    /// Starts or extends if provided recording ID
    /// Return the subscription ID, that media driver is using for recording
    fn start_recording(
        archive: &AeronArchive,
        recording: Option<ExtendedRecordingDescriptor>,
    ) -> Result<(i64, String), ServerError> {
        match recording {
            Some(ExtendedRecordingDescriptor {
                recording_id,
                channel,
            }) => Ok((
                archive.extend_recording(
                    recording_id,
                    &channel.clone().into_c_string(),
                    RECORDING_STREAM_ID,
                    SourceLocation::AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
                    false,
                )?,
                channel,
            )),
            None => {
                // Check for existing active recordings before starting a new one
                let mut active_reader = Handler::leak(ActiveRecordingReader::new());
                let _ = archive.list_recordings_for_uri(
                    0,
                    i32::MAX,
                    &RECORDING_CHANNEL.into_c_string(),
                    RECORDING_STREAM_ID,
                    Some(&active_reader),
                )?;
                
                if let Some(active_record) = &active_reader.active_recording {
                    info!(
                        target: "recording",
                        action = "found_active_recording",
                        recording_id = active_record.recording_id,
                        start_position = active_record.start_position
                    );
                    // For active recordings, we can't extend them, but we can get the subscription_id
                    // by querying the archive. Since the recording is already active, we return
                    // a dummy subscription_id (0) and the channel. The publication will connect to
                    // the existing recording.
                    active_reader.release();
                    Ok((0, RECORDING_CHANNEL.to_string()))
                } else {
                    active_reader.release();
                    info!(
                        target: "recording",
                        action = "starting_new_recording"
                    );
                    // Try to start recording, and if it fails with "recording exists",
                    // find the last recording and extend it
                    match archive.start_recording(
                        &RECORDING_CHANNEL.into_c_string(),
                        RECORDING_STREAM_ID,
                        SourceLocation::AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
                        false,
                    ) {
                        Ok(subscription_id) => Ok((subscription_id, RECORDING_CHANNEL.to_string())),
                        Err(e) => {
                            // Check if error is "recording exists"
                            let error_msg = format!("{}", e);
                            if error_msg.contains("recording exists") {
                                info!(
                                    target: "recording",
                                    action = "recording_exists_fallback",
                                    error = %error_msg
                                );
                                // First check for active recordings (stop_position == 0)
                                let mut active_reader = Handler::leak(ActiveRecordingReader::new());
                                let _ = archive.list_recordings_for_uri(
                                    0,
                                    i32::MAX,
                                    &RECORDING_CHANNEL.into_c_string(),
                                    RECORDING_STREAM_ID,
                                    Some(&active_reader),
                                )?;
                                
                                if let Some(active_record) = &active_reader.active_recording {
                                    info!(
                                        target: "recording",
                                        action = "found_active_recording_in_fallback",
                                        recording_id = active_record.recording_id,
                                        start_position = active_record.start_position
                                    );
                                    // For active recordings, we can't extend them, but we can use them
                                    // Return a dummy subscription_id (0) and the channel
                                    active_reader.release();
                                    Ok((0, RECORDING_CHANNEL.to_string()))
                                } else {
                                    active_reader.release();
                                    // No active recording, check for completed recordings
                                    let mut reader = Handler::leak(RecorderDescriptorReader::new());
                                    let _ = archive.list_recordings_for_uri(
                                        0,
                                        i32::MAX,
                                        &RECORDING_CHANNEL.into_c_string(),
                                        RECORDING_STREAM_ID,
                                        Some(&reader),
                                    )?;
                                    
                                    if let Some(last_record) = &reader.last_recording {
                                        info!(
                                            target: "recording",
                                            action = "extending_existing_recording",
                                            recording_id = last_record.recording_id,
                                            start_position = last_record.start_position,
                                            stop_position = last_record.stop_position
                                        );
                                        let extended_recording = ExtendedRecordingDescriptor::new(
                                            last_record.initial_term_id,
                                            last_record.start_position,
                                            last_record.term_buffer_length,
                                            last_record.recording_id,
                                        )?;
                                        reader.release();
                                        Ok((
                                            archive.extend_recording(
                                                extended_recording.recording_id,
                                                &extended_recording.channel.clone().into_c_string(),
                                                RECORDING_STREAM_ID,
                                                SourceLocation::AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
                                                false,
                                            )?,
                                            extended_recording.channel,
                                        ))
                                    } else {
                                        reader.release();
                                        // If we can't find any recording, return the original error
                                        Err(ServerError::AeronConnectionError(e))
                                    }
                                }
                            } else {
                                // Some other error, return it
                                Err(ServerError::AeronConnectionError(e))
                            }
                        }
                    }
                }
            }
        }
    }

    /// Starts replaying from the last recording if available
    /// Returns the recording ID if replay was completed successfully
    fn start_replay(
        aeron: &Aeron,
        archive: &AeronArchive,
        producer: MultiProducer<OrderCommand, SingleConsumerBarrier>,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Option<ExtendedRecordingDescriptor>, ServerError> {
        let mut reader = Handler::leak(RecorderDescriptorReader::new());
        let last_recording_id = archive.list_recordings_for_uri(
            0,
            i32::MAX,
            &RECORDING_CHANNEL.into_c_string(), // aeron control request channel
            RECORDING_STREAM_ID,
            Some(&reader),
        )?;
        info!(
            target: "replay",
            action = "recordings_listed",
            recording_id = last_recording_id
        );
        if let Some(record) = &reader.last_recording {
            let session_id = record.session_id;
            let recording_id = record.recording_id;
            info!(
                target: "replay",
                action = "recording_selected",
                recording_id,
                session_id,
                start_position = record.start_position,
                stop_position = record.stop_position
            );
            let params = AeronArchiveReplayParams::new(
                0,
                i32::MAX,
                record.start_position,
                record.stop_position - record.start_position,
                0,
                0,
            )?;
            debug!(
                target: "replay",
                action = "replay_params",
                params = ?params
            );
            let replay_session_id = archive.start_replay(
                recording_id,
                &RECORDING_CHANNEL.into_c_string(),
                REPLAY_STREAM_ID,
                &params,
            )? as i32;

            let replay_channel_with_session =
                format!("{}?session-id={}", &RECORDING_CHANNEL, replay_session_id);
            info!(
                target: "replay",
                action = "subscription_created",
                channel = %replay_channel_with_session
            );

            let mut h1 = Handler::leak(AeronAvailableImageLogger);
            let mut h2 = Handler::leak(AeronUnavailableImageLogger);
            let mut message_handler = Handler::leak(ReplayFragmentHandler {
                producer,
                gateway_id: 0,
            });
            let subscription = aeron.add_subscription(
                &replay_channel_with_session.into_c_string(),
                REPLAY_STREAM_ID,
                Some(&h1),
                Some(&h2),
                Duration::from_secs(5),
            )?;

            while !subscription.is_connected() {
                std::thread::sleep(Duration::from_millis(100));
                debug!(
                    target: "replay",
                    action = "subscription_wait",
                );
            }

            let mut position = record.start_position;
            while let fragaments_read = subscription.poll(Some(&message_handler), 1)?
                && position < record.stop_position && !shutdown.load(Ordering::Acquire)
            {
                if fragaments_read == 0 {
                    AeronIdleStrategy::busy_spinning_idle(std::ptr::null_mut(), 0);
                    continue;
                }
                position += FRAMESIZE;
                debug!(
                    target: "replay",
                    action = "position_advanced",
                    position
                );
            }
            info!(
                target: "replay",
                action = "completed",
                recording_id,
                session_id
            );
            let extended_recording_descriptor = ExtendedRecordingDescriptor::new(
                record.initial_term_id,
                record.stop_position,
                record.term_buffer_length,
                recording_id,
            )?;
            h1.release();
            h2.release();
            message_handler.release();
            reader.release();
            subscription.close::<AeronNotificationLogger>(None)?;
            return Ok(Some(extended_recording_descriptor));
        } else {
            info!(target: "replay", action = "no_recording_available");
            return Ok(None);
        }
    }

    /// Gets core configuration
    pub fn config(&self) -> &CoreNetworkingConfig {
        &self.config
    }
}
