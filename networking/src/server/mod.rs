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
mod progress;
#[cfg(test)]
mod recovery_tests;
mod replay;

use crate::server::cmd_handler::ReplayFragmentHandler;
use crate::server::gateway_handler::{
    GatewayImageAvailableHandler, GatewayImageUnavailableHandler, HandshakeMessageHandler,
};
use crate::server::gateway_manager::GatewayManager;
use crate::server::replay::{
    ExtendedRecordingDescriptor, JournalHolders, RecorderDescriptorReader, RecordingCounter,
    RecordingInfo, assert_replay_complete, await_journal_release, drain_replay,
    ensure_replayable_recording, ensure_stopped_recordings, fail_on_replay_error,
    is_live_recording, journal_publication, replay_lineage, tail_recording, tail_window_start,
};
use crate::utils::{new_publication_with_mdc, new_subscription_with_handlers};
use common::{OrderCommand, decode_order_command};
use disruptor::{MultiProducer, SingleConsumerBarrier};
use progress::{DRAIN_TIMEOUT, finish_shutdown, wait_until};
use rusteron_archive::bindings::AERON_NULL_COUNTER_ID;
use rusteron_archive::{
    Aeron, AeronArchiveAsyncConnect, AeronArchiveReplayParams, AeronAvailableImageLogger,
    AeronCError, AeronCnc, AeronContext, AeronImage, AeronNotificationLogger, AeronSubscription,
    AeronUnavailableImageLogger, Handler, IntoCString, SourceLocation,
};
use rusteron_archive::{AeronArchive, AeronArchiveContext};
use rusteron_media_driver::AeronIdleStrategy;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::{debug, error, info, warn};
use vex_config::{CoreNetworkingConfig, GatewayAuthenticationKey};

pub use gateway_publications::Publications;

/// Stream ID for gateway communication
const ALL_GATEWAYS_STREAM_ID: i32 = 1001;

/// Recording stream ID for Aeron Archive
const RECORDING_STREAM_ID: i32 = 2001;
/// Replay Stream ID for Aeron Archive
pub const REPLAY_STREAM_ID: i32 = 2002;
/// Channel for Aeron Recording also known as Aeron Control Channel
const RECORDING_CHANNEL: &str = "aeron:ipc";
const STARTUP_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
/// How long past the media driver's client liveness timeout a dead process may still hold the
/// journal: the driver notices the timeout at its next timer check, and frees the dead process's
/// publication within two more (draining, then lingering; `aeron.timer.interval`, 1 s by default)
/// once the archive has recorded what it held; the archive then stops the recording.
const JOURNAL_RELEASE_MARGIN: Duration = Duration::from_secs(5);

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
    #[error("Pipeline/archive drain failed: {0}")]
    DrainError(String),
    #[error("Replay error: {0}")]
    ReplayError(String),
    #[error("{0} did not connect within 10 seconds")]
    StartupConnectionTimeout(&'static str),
}

fn wait_for_startup_connection(
    resource: &'static str,
    mut is_connected: impl FnMut() -> bool,
) -> Result<(), ServerError> {
    let start = Instant::now();
    while !is_connected() {
        if start.elapsed() >= STARTUP_CONNECTION_TIMEOUT {
            return Err(ServerError::StartupConnectionTimeout(resource));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

fn should_log_poll_error(last_error: &mut Option<String>, error: impl std::fmt::Display) -> bool {
    let error = error.to_string();
    if last_error.as_ref() == Some(&error) {
        false
    } else {
        *last_error = Some(error);
        true
    }
}

/// Runs `undo` when dropped unless [`UndoOnError::disarm`] is called first, so a completed
/// startup step is reverted on every early `?` return that follows it.
struct UndoOnError<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> UndoOnError<F> {
    fn new(undo: F) -> Self {
        Self(Some(undo))
    }

    fn disarm(mut self) {
        self.0 = None;
    }
}

impl<F: FnOnce()> Drop for UndoOnError<F> {
    fn drop(&mut self) {
        if let Some(undo) = self.0.take() {
            undo();
        }
    }
}

/// Enhanced VEX Core server for handling gateway connections
pub struct VexCoreServer {
    /// Core configuration
    config: CoreNetworkingConfig,
    /// Gateway state management (lock-free)
    gateways: Rc<GatewayManager>,
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
    /// Archive Client (optional, only when archiving is enabled)
    archive: Option<AeronArchive>,
    publications: Arc<Publications>,
    recording_id: Option<i64>,
    /// Subscription ID for recording (optional, only when archiving is enabled)
    subscription_id: Option<i64>,
}

impl VexCoreServer {
    /// Creates a new VEX Core instance
    pub fn new(
        config: CoreNetworkingConfig,
        authentication_key: GatewayAuthenticationKey,
        producer: MultiProducer<OrderCommand, SingleConsumerBarrier>,
        publications: Arc<Publications>,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Self, ServerError> {
        // Validate configuration
        Self::validate_config(&config)?;
        gateway_manager::validate_authentication_config(
            config.enable_authentication,
            &authentication_key,
        )?;

        if !config.enable_authentication {
            warn!(
                target: "core_server",
                action = "gateway_authentication_disabled",
                "GATEWAY HANDSHAKE AUTHENTICATION IS DISABLED; DEVELOPMENT/TEST USE ONLY"
            );
        }

        // Initialize Aeron context
        let aeron = Self::initialize_aeron(&config)?;
        // Armed once recording starts, disarmed only when construction succeeds.
        let mut stop_recording_on_error = None;

        // Initialize Aeron Archive (only if archive channels are configured)
        let (archive, subscription_id, recording_id) = if !config.request_control_channel.is_empty()
        {
            let archive = Self::initialize_archive(&config, &aeron)?;
            let release_timeout = Self::journal_release_timeout(&config.context_dir)?;
            let recordings = Self::list_recordings(&aeron, &archive, release_timeout, &shutdown)?;

            // Every live command is stamped above the journal's last command, and its order id
            // is derived from that timestamp.
            let journal_tail = match tail_recording(&recordings) {
                Some(record) => Self::read_journal_tail(&aeron, &archive, record, &shutdown)?,
                None => 0,
            };
            publications.set_journal_tail(journal_tail);

            // Every start replays the journal, and extends the recording it replayed. An empty
            // archive holds nothing to replay, so the start is fresh.
            let recording = match replay_lineage(&recordings) {
                None => {
                    info!(target: "replay", action = "no_recording_available");
                    None
                }
                Some(record) => Some(Self::start_replay(
                    &aeron,
                    &archive,
                    record,
                    producer.clone(),
                    Arc::clone(&shutdown),
                    Arc::clone(&publications),
                )?),
            };

            // Start recording
            let (subscription_id, channel) = Self::start_recording(&archive, recording)?;
            // If startup fails from here on, stop the recording explicitly: a publication that
            // never connected leaves no image for `auto_stop`, and the archive would then refuse
            // to start or extend the next recording with "recording exists".
            let recording_archive = archive.clone();
            stop_recording_on_error = Some(UndoOnError::new(move || {
                if let Err(error) = recording_archive.stop_recording_subscription(subscription_id) {
                    warn!(
                        target: "core_server",
                        action = "failed_startup_recording_stop_failed",
                        subscription_id,
                        error = %error
                    );
                }
            }));

            // Publisher for Recording the incoming messages
            let archive_publication = aeron.add_publication(
                &channel.into_c_string(),
                RECORDING_STREAM_ID,
                Duration::from_secs(1),
            )?;

            wait_for_startup_connection("archive publication", || {
                archive_publication.is_connected()
            })?;

            let counters = aeron.counters_reader();
            let mut recording_id = None;
            wait_until(DRAIN_TIMEOUT, || {
                let counter = rusteron_archive::RecordingPos::find_counter_id_by_session(
                    &counters,
                    archive_publication.session_id(),
                );
                if counter != AERON_NULL_COUNTER_ID {
                    recording_id = Some(rusteron_archive::RecordingPos::get_recording_id(
                        &counters, counter,
                    )?);
                }
                Ok(recording_id.is_some())
            })?;
            publications.set_archive_publication(archive_publication);

            info!(
                target: "core_server",
                action = "initialized",
                archive_recording = true,
                core_id = %config.core_id
            );

            (Some(archive), Some(subscription_id), recording_id)
        } else {
            // No journal, so no tail to continue from: timestamps, and the order ids derived from
            // them, follow the wall clock alone (see `Publications::journal_tail`).
            info!(
                target: "core_server",
                action = "initialized",
                archive_recording = false,
                core_id = %config.core_id
            );
            (None, None, None)
        };

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

        let gateways = Rc::new(GatewayManager::new(
            config.clone(),
            authentication_key,
            aeron,
            producer,
            Arc::clone(&publications),
        )?);

        // Create handshake handler
        let handshake_handler = HandshakeMessageHandler::new(Rc::clone(&gateways), publication);

        if let Some(stop_recording_on_error) = stop_recording_on_error {
            stop_recording_on_error.disarm();
        }
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
            publications,
            recording_id,
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
        // 1. Listens for new handshakes
        // 2. Listens for new orders
        let mut last_subscription_poll_error = None;
        let mut last_gateway_poll_error = None;
        loop {
            if self.shutdown.load(Ordering::Acquire) {
                return self.shutdown();
            }

            match self.subscription.poll(Some(&self.handshake_handler), 10) {
                Ok(_) => last_subscription_poll_error = None,
                Err(e) => {
                    if should_log_poll_error(&mut last_subscription_poll_error, &e) {
                        error!(
                            target: "core_server",
                            action = "poll_subscription_failed",
                            core_id = %self.config.core_id,
                            error = %e
                        );
                    }
                }
            }

            match self.gateways.poll() {
                Ok(()) => last_gateway_poll_error = None,
                Err(e) => {
                    if should_log_poll_error(&mut last_gateway_poll_error, &e) {
                        error!(
                            target: "core_server",
                            action = "poll_gateways_failed",
                            core_id = %self.config.core_id,
                            error = %e
                        );
                    }
                }
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
        // Ingress is closed. Keep the recording subscription and client alive until
        // accepted commands finish journaling and the archive catches the publication.
        finish_shutdown(
            || self.publications.drain_pipeline(),
            || {
                if let (Some(archive), Some(recording_id), Some(publication)) = (
                    self.archive.as_ref(),
                    self.recording_id,
                    self.publications.archive_publication(),
                ) {
                    let target = publication.position();
                    if target < 0 {
                        return Err(ServerError::DrainError(format!(
                            "archive publication position: {target}"
                        )));
                    }
                    wait_until(DRAIN_TIMEOUT, || {
                        Ok(archive.get_max_recorded_position(recording_id)? >= target)
                    })
                    .map_err(|error| {
                        ServerError::DrainError(format!(
                            "recording {recording_id} did not reach publication position {target}: {error}"
                        ))
                    })?;
                }
                Ok(())
            },
            || {
                if let Some(sub_id) = self.subscription_id
                    && let Some(ref archive) = self.archive
                {
                    archive.stop_recording_subscription(sub_id)?;
                }
                if let Some(ref archive) = self.archive {
                    archive.close()?;
                }
                Ok(())
            },
        )?;

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
            aeron,
            &config.request_control_channel,
            &config.response_control_channel,
            RECORDING_CHANNEL,
        )?;

        let archive_async_connect = AeronArchiveAsyncConnect::new_with_aeron(&archive_ctx, aeron)?;
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
                    true,
                )?,
                channel,
            )),
            None => {
                info!(
                    target: "recording",
                    action = "starting_new_recording"
                );
                let subscription_id = archive.start_recording(
                    &RECORDING_CHANNEL.into_c_string(),
                    RECORDING_STREAM_ID,
                    SourceLocation::AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
                    true,
                )?;
                Ok((subscription_id, RECORDING_CHANNEL.to_string()))
            }
        }
    }

    /// How long a start waits for a process that died while journaling to release the journal:
    /// the client liveness timeout of the media driver at `context_dir`, as the driver publishes
    /// it, plus [`JOURNAL_RELEASE_MARGIN`].
    fn journal_release_timeout(context_dir: &str) -> Result<Duration, ServerError> {
        let liveness = AeronCnc::new_on_heap(context_dir)?
            .get_constants()?
            .client_liveness_timeout();
        let liveness = u64::try_from(liveness)
            .ok()
            .filter(|&nanos| nanos > 0)
            .ok_or_else(|| {
                ServerError::ConfigurationError(format!(
                    "the media driver at {context_dir} reports a client liveness timeout of \
                     {liveness} ns"
                ))
            })?;
        Ok(Duration::from_nanos(liveness) + JOURNAL_RELEASE_MARGIN)
    }

    /// What holds the journal: the archive's live recordings of it, and its publications that the
    /// media driver holds.
    fn journal_holders(
        aeron: &Aeron,
        archive: &AeronArchive,
    ) -> Result<JournalHolders, ServerError> {
        let mut holders = JournalHolders::default();
        let mut descriptor_count = 0;
        archive.list_recordings_for_uri_once(
            &mut descriptor_count,
            0,
            i32::MAX,
            &RECORDING_CHANNEL.into_c_string(),
            RECORDING_STREAM_ID,
            |descriptor| {
                if is_live_recording(descriptor.stop_position) {
                    holders.live_recordings.push(descriptor.recording_id);
                }
            },
        )?;
        aeron
            .counters_reader()
            .foreach_counter_once(|_, _, type_id, key, _| {
                holders
                    .publications
                    .extend(journal_publication(type_id, key));
            });
        Ok(holders)
    }

    /// Lists the journal's recordings, oldest first, each at its final stop position. Waits first,
    /// for at most `release_timeout`, until no process that died while journaling holds the
    /// journal (see [`await_journal_release`]).
    fn list_recordings(
        aeron: &Aeron,
        archive: &AeronArchive,
        release_timeout: Duration,
        shutdown: &AtomicBool,
    ) -> Result<Vec<RecordingInfo>, ServerError> {
        await_journal_release(release_timeout, shutdown, || {
            Self::journal_holders(aeron, archive)
        })?;
        // A process killed between starting its recording and its publication connecting leaves
        // a recording subscription with no image, which auto_stop never removes; the archive
        // would then refuse to start or extend the recording ("recording exists"). No recording
        // of the journal is live now, so this stops none.
        archive.try_stop_recording_channel_and_stream(
            &RECORDING_CHANNEL.into_c_string(),
            RECORDING_STREAM_ID,
        )?;
        let mut recording_counter = Handler::leak(RecordingCounter);
        let archive_recording_count =
            archive.list_recordings(0, i32::MAX, Some(&recording_counter));
        recording_counter.release();
        let archive_recording_count = archive_recording_count?;
        let mut reader = Handler::leak(RecorderDescriptorReader::new());
        let matching_recording_count = archive.list_recordings_for_uri(
            0,
            i32::MAX,
            &RECORDING_CHANNEL.into_c_string(), // aeron control request channel
            RECORDING_STREAM_ID,
            Some(&reader),
        );
        // Copy the result out and free the reader before anything below can return early.
        let recordings = std::mem::take(&mut reader.recordings);
        let skipped_invalid = reader.skipped_invalid;
        reader.release();
        let matching_recording_count = matching_recording_count?;
        info!(
            target: "replay",
            action = "recordings_listed",
            archive_recording_count,
            matching_recording_count,
            skipped_invalid
        );
        ensure_replayable_recording(skipped_invalid)?;
        ensure_stopped_recordings(&recordings)?;
        Ok(recordings)
    }

    /// Timestamp of the last command in `record`, which must hold one. Replays only the term
    /// that holds it.
    fn read_journal_tail(
        aeron: &Aeron,
        archive: &AeronArchive,
        record: &RecordingInfo,
        shutdown: &AtomicBool,
    ) -> Result<u64, ServerError> {
        let mut tail = None;
        let mut decode_error = None;
        Self::replay_range(
            aeron,
            archive,
            record.recording_id,
            tail_window_start(record),
            record.stop_position,
            shutdown,
            |image| {
                image.poll_once(
                    |buffer, _| match decode_order_command(buffer) {
                        Ok(command) => tail = Some(command.timestamp),
                        Err(e) => {
                            decode_error =
                                Some(format!("failed to decode recorded order command: {e:?}"))
                        }
                    },
                    10,
                )?;
                fail_on_replay_error(decode_error.as_deref())
            },
        )?;
        let tail = tail.ok_or_else(|| {
            ServerError::ReplayError(format!(
                "recording {} holds no command in its last term",
                record.recording_id
            ))
        })?;
        info!(
            target: "replay",
            action = "journal_tail_read",
            recording_id = record.recording_id,
            timestamp = tail
        );
        Ok(tail)
    }

    /// Replays `record` into the pipeline and returns the descriptor that extends it.
    fn start_replay(
        aeron: &Aeron,
        archive: &AeronArchive,
        record: &RecordingInfo,
        producer: MultiProducer<OrderCommand, SingleConsumerBarrier>,
        shutdown: Arc<AtomicBool>,
        publications: Arc<Publications>,
    ) -> Result<ExtendedRecordingDescriptor, ServerError> {
        let (recording_id, session_id) = (record.recording_id, record.session_id);
        info!(
            target: "replay",
            action = "recording_selected",
            recording_id,
            session_id,
            start_position = record.start_position,
            stop_position = record.stop_position
        );
        let mut message_handler = Handler::leak(ReplayFragmentHandler {
            producer,
            gateway_id: 0,
            commands_published: 0,
            replay_error: None,
            publications: Arc::clone(&publications),
            shutdown: Arc::clone(&shutdown),
        });
        let replay_result = Self::replay_range(
            aeron,
            archive,
            recording_id,
            record.start_position,
            record.stop_position,
            &shutdown,
            |image| {
                let before = message_handler.commands_published;
                let fragments = image.poll(Some(&message_handler), 1)?;
                fail_on_replay_error(message_handler.replay_error.as_deref())?;
                assert_replay_complete(
                    i64::from(fragments),
                    message_handler.commands_published - before,
                )
            },
        );
        // Release the producer whatever the outcome: a stranded clone would keep the disruptor
        // from shutting down after a failed start. Callbacks are no longer polled.
        message_handler.release();
        replay_result?;
        publications
            .drain_pipeline()
            .map_err(|e| ServerError::ReplayError(e.to_string()))?;
        info!(
            target: "replay",
            action = "completed",
            recording_id,
            session_id
        );
        ExtendedRecordingDescriptor::new(
            record.initial_term_id,
            record.stop_position,
            record.term_buffer_length,
            recording_id,
        )
    }

    /// Replays `[start, stop)` of a recording, calling `poll` to poll its image until the image
    /// reaches `stop`.
    fn replay_range(
        aeron: &Aeron,
        archive: &AeronArchive,
        recording_id: i64,
        start: i64,
        stop: i64,
        shutdown: &AtomicBool,
        mut poll: impl FnMut(&AeronImage) -> Result<(), ServerError>,
    ) -> Result<(), ServerError> {
        let params = AeronArchiveReplayParams::new(
            AERON_NULL_COUNTER_ID,
            i32::MAX,
            start,
            stop - start,
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
        )?;
        // Stop a replay that is still streaming if startup fails before it completes.
        let stop_replay_on_error = UndoOnError::new(|| {
            if let Err(error) = archive.stop_replay(replay_session_id) {
                warn!(
                    target: "replay",
                    action = "failed_replay_stop_failed",
                    replay_session_id,
                    error = %error
                );
            }
        });
        // The replay image's session id is the low 32 bits of the replay session id.
        let image_session_id = replay_session_id as i32;

        let replay_channel_with_session =
            format!("{}?session-id={}", RECORDING_CHANNEL, image_session_id);
        info!(
            target: "replay",
            action = "subscription_created",
            channel = %replay_channel_with_session
        );

        let subscription = aeron.add_subscription(
            &replay_channel_with_session.into_c_string(),
            REPLAY_STREAM_ID,
            None::<&Handler<AeronAvailableImageLogger>>,
            None::<&Handler<AeronUnavailableImageLogger>>,
            Duration::from_secs(5),
        )?;

        wait_for_startup_connection("replay subscription", || subscription.is_connected())?;

        let image = subscription.image_by_session_id(image_session_id);
        if image.get_inner().is_null() {
            return Err(ServerError::ReplayError("replay image unavailable".into()));
        }
        let replay_result = drain_replay(start, stop, DRAIN_TIMEOUT, || {
            if shutdown.load(Ordering::Acquire) {
                return Err(ServerError::ReplayError("replay cancelled".into()));
            }
            poll(&image)?;
            Ok((
                image.position(),
                image.is_end_of_stream() || image.is_closed(),
            ))
        });
        if replay_result.is_ok() {
            // A complete replay has already ended on the archive side.
            stop_replay_on_error.disarm();
        }
        subscription.image_release(&image)?;
        subscription.close::<AeronNotificationLogger>(None)?;
        replay_result
    }

    /// Gets core configuration
    pub fn config(&self) -> &CoreNetworkingConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::{ServerError, UndoOnError, should_log_poll_error};
    use std::cell::Cell;

    #[test]
    fn poll_error_deduplication_keeps_first_occurrence_and_resets() {
        let mut last_error = None;

        assert!(should_log_poll_error(&mut last_error, "persistent error"));
        assert!(!should_log_poll_error(&mut last_error, "persistent error"));
        last_error = None;
        assert!(should_log_poll_error(&mut last_error, "persistent error"));
    }

    /// Mirrors `VexCoreServer::new`: arm after the step, `?` out on a later failure, disarm last.
    fn startup(undos: &Cell<u32>, later_step: Result<(), ServerError>) -> Result<(), ServerError> {
        let undo = UndoOnError::new(|| undos.set(undos.get() + 1));
        later_step?;
        undo.disarm();
        Ok(())
    }

    #[test]
    fn startup_step_is_undone_exactly_once_when_a_later_step_fails() {
        let undos = Cell::new(0);
        let error = startup(
            &undos,
            Err(ServerError::StartupConnectionTimeout("archive publication")),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ServerError::StartupConnectionTimeout("archive publication")
        ));
        assert_eq!(undos.get(), 1);
    }

    #[test]
    fn successful_startup_keeps_its_steps() {
        let undos = Cell::new(0);
        startup(&undos, Ok(())).unwrap();
        assert_eq!(undos.get(), 0);
    }
}
