use crate::server::duologue::{
    DUOLOGUE_STREAM_ID, Duologue, DuologueCloseNotification, DuologueImageAvailable,
    DuologueImageUnavailable,
};
use crate::server::gateway_publications::Publications;
use crate::utils::{PORT_COOLDOWN_SECONDS, PortAllocator, SessionAllocator, send_message};
use common::{MAX_GATEWAYS, OrderCommand};
use disruptor::{MultiProducer, SingleConsumerBarrier};
use rusteron_archive::{
    Aeron, AeronAsyncAddPublication, AeronAsyncAddSubscription, AeronPublication,
    AeronSubscription, Handler,
};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::ffi::CString;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};
use vex_config::{CoreNetworkingConfig, GatewayAuthenticationKey};

use super::ServerError;

const HANDSHAKE_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const HANDSHAKE_RATE_WINDOW: Duration = Duration::from_secs(1);
// Match the global budget to the slot count so every gateway can handshake concurrently.
const HANDSHAKE_RATE_LIMIT: usize = MAX_GATEWAYS;
const ACCEPT_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const ACCEPT_RETRY_LIMIT: usize = 5;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GatewaySlotState {
    AwaitingImage { allocated_at: Instant },
    Live,
}

const PORTS_PER_GATEWAY: usize = 2;
/// Every slot's ports, and as many again in their cooldown after a full turnover: the UDP range
/// `base_gateway_port..base_gateway_port + 63` that gateways reach.
const PORT_POOL: usize = 2 * MAX_GATEWAYS * PORTS_PER_GATEWAY;

pub struct Session {
    slots: [Option<GatewaySlot>; MAX_GATEWAYS],
}

pub struct GatewaySlot {
    duologue: Option<Duologue>,
    port_data: u16,
    port_reply: u16,
    session_id: i32,
    state: GatewaySlotState,
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
            .filter_map(|slot| slot.as_ref()?.duologue.as_ref())
    }

    fn reserve(
        &mut self,
        gateway_id: u8,
        session_id: i32,
        ports: [u16; 2],
        allocated_at: Instant,
    ) -> bool {
        if (gateway_id as usize) >= MAX_GATEWAYS {
            return false;
        }
        let slot = &mut self.slots[gateway_id as usize];
        if slot.is_some() {
            return false;
        }
        *slot = Some(GatewaySlot {
            duologue: None,
            session_id,
            port_data: ports[0],
            port_reply: ports[1],
            state: GatewaySlotState::AwaitingImage { allocated_at },
        });
        true
    }

    fn attach_duologue(
        &mut self,
        gateway_id: u8,
        session_id: i32,
        duologue: Duologue,
    ) -> Result<(), Duologue> {
        let Some(slot) = self
            .slots
            .get_mut(gateway_id as usize)
            .and_then(Option::as_mut)
        else {
            return Err(duologue);
        };
        if slot.session_id != session_id || slot.duologue.is_some() {
            return Err(duologue);
        }
        slot.duologue = Some(duologue);
        Ok(())
    }

    fn mark_live(&mut self, gateway_id: u8, session_id: i32) -> bool {
        let Some(slot) = self
            .slots
            .get_mut(gateway_id as usize)
            .and_then(Option::as_mut)
        else {
            return false;
        };
        if slot.session_id != session_id {
            return false;
        }
        slot.state = GatewaySlotState::Live;
        true
    }

    fn remove(&mut self, gateway_id: u8) -> Option<GatewaySlot> {
        let gateway_id = gateway_id as usize;
        if gateway_id < MAX_GATEWAYS {
            self.slots[gateway_id].take()
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

    /// Get all ports currently in use (both data and reply ports)
    pub fn get_ports_in_use(&self) -> Vec<u16> {
        let mut ports = Vec::new();
        for slot in self.slots.iter().flatten() {
            if slot.port_data != 0 {
                ports.push(slot.port_data);
            }
            if slot.port_reply != 0 {
                ports.push(slot.port_reply);
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

    fn gateway_ids(&self) -> Vec<u8> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(gateway_id, slot)| slot.as_ref().map(|_| gateway_id as u8))
            .collect()
    }

    /// Only attached slots: until then the image handlers may still be referenced by an in-flight
    /// subscription registration, which cannot be cancelled. An unattached slot is reaped on the
    /// first poll after it attaches (its age is unchanged), through the safe `Duologue::close`.
    fn expired_pending(&self, now: Instant) -> Vec<u8> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(gateway_id, slot)| {
                slot.as_ref()
                    .filter(|slot| slot.duologue.is_some() && handshake_is_expired(slot.state, now))
                    .map(|_| gateway_id as u8)
            })
            .collect()
    }
}

fn handshake_is_expired(state: GatewaySlotState, now: Instant) -> bool {
    match state {
        GatewaySlotState::AwaitingImage { allocated_at } => {
            now.saturating_duration_since(allocated_at) >= HANDSHAKE_IDLE_TIMEOUT
        }
        GatewaySlotState::Live => false,
    }
}

struct HandshakeRateLimiter {
    accepted: VecDeque<Instant>,
}

impl HandshakeRateLimiter {
    fn new() -> Self {
        Self {
            accepted: VecDeque::with_capacity(HANDSHAKE_RATE_LIMIT),
        }
    }

    fn allow(&mut self, now: Instant) -> bool {
        while self.accepted.front().is_some_and(|accepted_at| {
            now.saturating_duration_since(*accepted_at) >= HANDSHAKE_RATE_WINDOW
        }) {
            self.accepted.pop_front();
        }
        if self.accepted.len() >= HANDSHAKE_RATE_LIMIT {
            return false;
        }
        self.accepted.push_back(now);
        true
    }
}

pub(super) enum GatewaySessionEvent {
    ImageAvailable { gateway_id: u8, session_id: i32 },
    ImageUnavailable { gateway_id: u8, session_id: i32 },
}

struct PendingHandshake {
    gateway_id: u8,
    handshake_session_id: i32,
    dedicated_session_id: i32,
    ports: [u16; 2],
    encryption_key: i32,
    response_publication: AeronPublication,
    publication_registration: Option<AeronAsyncAddPublication>,
    subscription_registration: Option<AeronAsyncAddSubscription>,
    publication: Option<AeronPublication>,
    subscription: Option<AeronSubscription>,
    on_image_available_handler: Option<Handler<DuologueImageAvailable>>,
    on_image_unavailable_handler: Option<Handler<DuologueImageUnavailable>>,
    session_attached: bool,
    accept_attempts: usize,
    next_accept_attempt: Instant,
}

enum PendingHandshakeStatus {
    Pending,
    Complete,
}

/// Manages gateway connections and session lifecycle
///
/// Handles all gateway operations including handshake processing,
/// session allocation, resource management, and cleanup operations.
pub struct GatewayManager {
    /// Active gateway sessions mapped by gateway id
    gateway_sessions: RefCell<Session>,
    /// Aeron messaging instance
    aeron: Aeron,
    /// Core configuration
    config: CoreNetworkingConfig,
    authentication_key: GatewayAuthenticationKey,
    /// Port allocator for gateway sessions
    port_allocator: PortAllocator,
    /// Session ID allocator
    session_allocator: SessionAllocator,
    /// Producer that sends commands to the disruptor ring
    producer: MultiProducer<OrderCommand, SingleConsumerBarrier>,
    /// Aeron publications for each gateway
    publications: Arc<Publications>,
    /// Channel for receiving liveness changes from Aeron image callbacks
    session_event_rx: Receiver<GatewaySessionEvent>,
    /// Channel sender cloned for each callback
    session_event_tx: Sender<GatewaySessionEvent>,
    /// Non-blocking Aeron registrations and ACCEPT delivery state
    pending_handshakes: RefCell<[Option<PendingHandshake>; MAX_GATEWAYS]>,
    /// Bounds unauthenticated session allocation work
    handshake_rate_limiter: RefCell<HandshakeRateLimiter>,
    /// Bounds the REJECT replies and log lines a flood of bad HELLOs can cause
    rejected_handshakes: RefCell<HandshakeRateLimiter>,
}

impl GatewayManager {
    /// Creates a new gateway manager
    pub fn new(
        config: CoreNetworkingConfig,
        authentication_key: GatewayAuthenticationKey,
        aeron: Aeron,
        producer: MultiProducer<OrderCommand, SingleConsumerBarrier>,
        publications: Arc<Publications>,
    ) -> Result<Self, ServerError> {
        let (session_event_tx, session_event_rx) = channel();

        Ok(Self {
            gateway_sessions: RefCell::new(Session::new()),
            aeron,
            port_allocator: PortAllocator::new(config.base_gateway_port, PORT_POOL)
                .map_err(|e| ServerError::ResourceAllocationError(e.to_string()))?,
            session_allocator: SessionAllocator::new(
                config.reserved_session_id_low,
                config.reserved_session_id_high,
            )
            .map_err(|e| ServerError::ResourceAllocationError(e.to_string()))?,
            config,
            authentication_key,
            producer,
            publications,
            session_event_rx,
            session_event_tx,
            pending_handshakes: RefCell::new([(); MAX_GATEWAYS].map(|_| None)),
            handshake_rate_limiter: RefCell::new(HandshakeRateLimiter::new()),
            rejected_handshakes: RefCell::new(HandshakeRateLimiter::new()),
        })
    }

    /// Checks if a gateway is currently connected
    pub fn is_gateway_connected(&self, gateway_id: u8) -> bool {
        self.gateway_sessions
            .borrow()
            .is_gateway_connected(gateway_id)
    }

    /// Gives `gateway_id` a slot without a handshake, so a test can hold a session open.
    #[cfg(test)]
    pub(super) fn reserve_slot(&self, gateway_id: u8, session_id: i32) -> bool {
        self.gateway_sessions.borrow_mut().reserve(
            gateway_id,
            session_id,
            [10_000, 10_001],
            Instant::now(),
        )
    }

    /// Applies liveness changes reported by Aeron image callbacks.
    fn process_session_events(&self) {
        loop {
            match self.session_event_rx.try_recv() {
                Ok(GatewaySessionEvent::ImageAvailable {
                    gateway_id,
                    session_id,
                }) => {
                    let marked_live = self
                        .gateway_sessions
                        .borrow_mut()
                        .mark_live(gateway_id, session_id);
                    if !marked_live {
                        warn!(
                            target: "gateway_manager",
                            action = "image_available_stale",
                            gateway_id,
                            session = format_args!("{:#x}", session_id)
                        );
                    }
                }
                Ok(GatewaySessionEvent::ImageUnavailable {
                    gateway_id,
                    session_id,
                }) => {
                    let session_matches = self
                        .gateway_sessions
                        .borrow()
                        .slots
                        .get(gateway_id as usize)
                        .and_then(Option::as_ref)
                        .is_some_and(|slot| slot.session_id == session_id);
                    if session_matches && let Err(e) = self.remove_gateway_session(gateway_id) {
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
                        action = "session_event_channel_disconnected"
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
        source: &str,
        buffer: &[u8],
    ) -> Result<(), ServerError> {
        let message = std::str::from_utf8(buffer)
            .map_err(|e| ServerError::GatewayMessageError(format!("Invalid UTF-8: {e}")))?;

        debug!(
            target: "gateway_manager",
            action = "handshake_received",
            session = format_args!("{:#x}", session_id),
            source,
            length = buffer.len()
        );

        // Parse "HELLO gateway_id credential encryption_key": the credential authenticates the
        // gateway; the per-handshake encryption key only masks the session id sent in ACCEPT.
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

        let credential = parts
            .next()
            .ok_or_else(|| ServerError::GatewayMessageError("Missing credential".to_string()))?;

        // A HELLO without its own encryption key comes from a client that masked the session
        // with the credential; reject it rather than fall back to that.
        let Some(Ok(encryption_key)) = parts.next().map(str::parse::<i32>) else {
            let error_msg = format!("{session_id} {gateway_id_str} REJECT Malformed HELLO message");
            send_message(publication, error_msg.as_bytes())?;
            return Err(ServerError::GatewayMessageError(
                "Malformed HELLO message".to_string(),
            ));
        };

        // Authenticate before parsing the id, checking for duplicates, or allocating any
        // resource, so an unauthenticated peer can never reach the allocator (PR #176).
        if self.authentication_is_enabled()
            && let Err(e) = authenticate_gateway(&self.authentication_key, credential)
        {
            return self.reject_handshake(publication, session_id, gateway_id_str, source, e);
        }

        // Rate-limit after authentication (PR #174), so an unauthenticated flood cannot
        // consume the budget legitimate gateways need to reconnect.
        let now = Instant::now();
        if !self.handshake_rate_limiter.borrow_mut().allow(now) {
            let unavailable_msg = format!(
                "{session_id} {gateway_id_str} UNAVAILABLE {}",
                HANDSHAKE_RATE_WINDOW.as_secs()
            );
            send_message(publication, unavailable_msg.as_bytes())?;
            return Err(ServerError::CapacityExceededError(
                "Handshake rate limit exceeded".to_string(),
            ));
        }

        let gateway_id = {
            const PREFIX: &str = "gateway-";
            if !gateway_id_str.starts_with(PREFIX) {
                return self.reject_handshake(
                    publication,
                    session_id,
                    gateway_id_str,
                    source,
                    ServerError::GatewayMessageError("Invalid gateway ID format".to_string()),
                );
            }
            match gateway_id_str[PREFIX.len()..].parse::<u8>() {
                Ok(id) if (id as usize) < MAX_GATEWAYS => id,
                Ok(id) => {
                    return self.reject_handshake(
                        publication,
                        session_id,
                        gateway_id_str,
                        source,
                        ServerError::GatewayMessageError(format!("Gateway ID {id} out of range")),
                    );
                }
                Err(_) => {
                    return self.reject_handshake(
                        publication,
                        session_id,
                        gateway_id_str,
                        source,
                        ServerError::GatewayMessageError("Invalid gateway ID".to_string()),
                    );
                }
            }
        };

        self.check_duplicate_connection(publication, session_id, gateway_id)?;

        if let Err(e) = self.allocate_gateway_session(
            gateway_id,
            session_id,
            encryption_key,
            publication,
            source,
            now,
        ) {
            // Ports in their cooldown or a failed Aeron registration pass: say when to retry.
            let msg = format!("{session_id} {gateway_id_str} UNAVAILABLE {PORT_COOLDOWN_SECONDS}");
            send_message(publication, msg.as_bytes())?;
            return Err(e);
        }

        Ok(())
    }

    /// Polls all active gateway sessions
    pub fn poll(&self) -> Result<(), ServerError> {
        self.process_session_events();
        // Before the handshakes, so a close is never applied to a session attached in this poll.
        self.close_slow_gateways();
        self.process_pending_handshakes();
        self.reap_idle_handshakes();

        // polls all active gateway sessions
        let guard = self.gateway_sessions.borrow();
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

    /// Closes the sessions whose replies could not be delivered (`Publications::publish_response`).
    fn close_slow_gateways(&self) {
        for gateway_id in 0..MAX_GATEWAYS as u8 {
            if self.publications.take_closing(gateway_id) {
                warn!(target: "gateway_manager", action = "slow_gateway_closed", gateway_id);
                if let Err(e) = self.remove_gateway_session(gateway_id) {
                    // Already gone: its image went unavailable in the same poll.
                    debug!(
                        target: "gateway_manager",
                        action = "slow_gateway_already_closed",
                        gateway_id,
                        error = %e
                    );
                }
            }
        }
    }

    /// Shuts down all gateway connections
    pub fn shutdown_all_gateways(&self) -> Result<(), ServerError> {
        let gateways_ids: Vec<u8> = self.gateway_sessions.borrow().gateway_ids();

        for gateway_id in gateways_ids {
            self.remove_gateway_session(gateway_id)?;
        }

        info!(
            target: "gateway_manager",
            action = "shutdown_complete"
        );
        Ok(())
    }

    fn authentication_is_enabled(&self) -> bool {
        self.config.enable_authentication && !self.authentication_key.is_empty()
    }

    fn reject_handshake(
        &self,
        publication: &AeronPublication,
        session_id: i32,
        gateway_label: &str,
        source: &str,
        error: ServerError,
    ) -> Result<(), ServerError> {
        // Past HANDSHAKE_RATE_LIMIT rejections a second nothing is sent or logged, so a flood of
        // bad HELLOs costs the ingress thread one compare each and fills no log.
        if self.rejected_handshakes.borrow_mut().allow(Instant::now()) {
            warn!(
                target: "gateway_manager",
                action = "handshake_rejected",
                source,
                session = format_args!("{:#x}", session_id),
                reason = %error
            );
            let error_msg = format!("{session_id} {gateway_label} REJECT Handshake rejected");
            send_message(publication, error_msg.as_bytes())?;
        }
        Err(error)
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

    /// Reserves a slot and submits non-blocking Aeron resource registrations.
    fn allocate_gateway_session(
        &self,
        gateway_id: u8,
        handshake_session_id: i32,
        encryption_key: i32,
        response_publication: &AeronPublication,
        hello_source: &str,
        now: Instant,
    ) -> Result<(), ServerError> {
        let (ports_in_use, sessions_in_use) = {
            let guard = self.gateway_sessions.borrow();
            (guard.get_ports_in_use(), guard.get_sessions_in_use())
        };
        let allocated_ports = self
            .port_allocator
            .allocate(2, &ports_in_use)
            .map_err(|e| ServerError::ResourceAllocationError(e.to_string()))?;
        let ports = [allocated_ports[0], allocated_ports[1]];
        let dedicated_session_id = self
            .session_allocator
            .allocate(&sessions_in_use)
            .map_err(|e| ServerError::ResourceAllocationError(e.to_string()))?;
        let publication_uri =
            CString::new(reply_uri(hello_source, ports[1], dedicated_session_id)?)
                .expect("Aeron source identity contains no NUL");

        if !self
            .gateway_sessions
            .borrow_mut()
            .reserve(gateway_id, dedicated_session_id, ports, now)
        {
            return Err(ServerError::GatewayMessageError(format!(
                "Gateway gateway-{gateway_id} became occupied during allocation"
            )));
        }

        let on_image_available_handler = Handler::leak(DuologueImageAvailable {
            expected_session_id: dedicated_session_id,
            gateway_id,
            tx: self.session_event_tx.clone(),
        });
        let on_image_unavailable_handler = Handler::leak(DuologueImageUnavailable {
            session_id: dedicated_session_id,
            gateway_id,
            tx: self.session_event_tx.clone(),
        });

        let subscription_uri = CString::new(format!(
            "aeron:udp?endpoint={}:{}|session-id={dedicated_session_id}",
            self.config.local_address, ports[0]
        ))
        .expect("controlled Aeron subscription URI contains no NUL");

        let publication_registration = match self
            .aeron
            .async_add_publication(&publication_uri, DUOLOGUE_STREAM_ID)
        {
            Ok(registration) => registration,
            Err(e) => {
                release_unattached_image_handlers(
                    gateway_id,
                    None,
                    on_image_available_handler,
                    on_image_unavailable_handler,
                );
                self.remove_gateway_session(gateway_id)?;
                return Err(ServerError::ResourceAllocationError(format!(
                    "Failed to register publication: {e}"
                )));
            }
        };
        let subscription_registration = match self.aeron.async_add_subscription(
            &subscription_uri,
            DUOLOGUE_STREAM_ID,
            Some(&on_image_available_handler),
            Some(&on_image_unavailable_handler),
        ) {
            Ok(registration) => registration,
            Err(e) => {
                // The registration was never accepted, so Aeron holds no reference to them.
                release_unattached_image_handlers(
                    gateway_id,
                    None,
                    on_image_available_handler,
                    on_image_unavailable_handler,
                );
                self.remove_gateway_session(gateway_id)?;
                return Err(ServerError::ResourceAllocationError(format!(
                    "Failed to register subscription: {e}"
                )));
            }
        };

        self.pending_handshakes.borrow_mut()[gateway_id as usize] = Some(PendingHandshake {
            gateway_id,
            handshake_session_id,
            dedicated_session_id,
            ports,
            encryption_key,
            response_publication: response_publication.clone(),
            publication_registration: Some(publication_registration),
            subscription_registration: Some(subscription_registration),
            publication: None,
            subscription: None,
            on_image_available_handler: Some(on_image_available_handler),
            on_image_unavailable_handler: Some(on_image_unavailable_handler),
            session_attached: false,
            accept_attempts: 0,
            next_accept_attempt: now,
        });
        Ok(())
    }

    fn process_pending_handshakes(&self) {
        for gateway_id in 0..MAX_GATEWAYS {
            let pending = self.pending_handshakes.borrow_mut()[gateway_id].take();
            let Some(mut pending) = pending else {
                continue;
            };

            match self.advance_pending_handshake(&mut pending, Instant::now()) {
                Ok(PendingHandshakeStatus::Pending) => {
                    self.pending_handshakes.borrow_mut()[gateway_id] = Some(pending);
                }
                Ok(PendingHandshakeStatus::Complete) => {}
                Err(e) => {
                    error!(
                        target: "gateway_manager",
                        action = "handshake_setup_failed",
                        gateway_id = pending.gateway_id,
                        error = %e
                    );
                    if let Err(remove_error) = self.remove_gateway_session(pending.gateway_id) {
                        warn!(
                            target: "gateway_manager",
                            action = "failed_handshake_cleanup_failed",
                            gateway_id = pending.gateway_id,
                            error = %remove_error
                        );
                    }
                    // Once attached, the session's `Duologue` owns the handlers and its close
                    // released them. Before that, `advance_pending_handshake` fails only after the
                    // subscription registration resolved, so `subscription` tells whether Aeron
                    // can still invoke them.
                    if let (Some(available), Some(unavailable)) = (
                        pending.on_image_available_handler,
                        pending.on_image_unavailable_handler,
                    ) {
                        release_unattached_image_handlers(
                            pending.gateway_id,
                            pending.subscription,
                            available,
                            unavailable,
                        );
                    }
                }
            }
        }
    }

    fn advance_pending_handshake(
        &self,
        pending: &mut PendingHandshake,
        now: Instant,
    ) -> Result<PendingHandshakeStatus, ServerError> {
        if !pending.session_attached {
            if pending.subscription.is_none() {
                pending.subscription = pending
                    .subscription_registration
                    .as_ref()
                    .expect("subscription registration missing before attachment")
                    .poll()
                    .map_err(|e| {
                        ServerError::ResourceAllocationError(format!(
                            "Failed to create subscription: {e}"
                        ))
                    })?;
                if pending.subscription.is_some() {
                    pending.subscription_registration = None;
                }
            }
            // Poll the publication only once the subscription exists, so no error can abandon
            // an in-flight subscription registration that still references the image handlers.
            if pending.subscription.is_some() && pending.publication.is_none() {
                pending.publication = pending
                    .publication_registration
                    .as_ref()
                    .expect("publication registration missing before attachment")
                    .poll()
                    .map_err(|e| {
                        ServerError::ResourceAllocationError(format!(
                            "Failed to create publication: {e}"
                        ))
                    })?;
                if pending.publication.is_some() {
                    pending.publication_registration = None;
                }
            }

            if pending.publication.is_none() || pending.subscription.is_none() {
                return Ok(PendingHandshakeStatus::Pending);
            }

            let gateway_session = Duologue::new(
                pending
                    .subscription
                    .take()
                    .expect("checked dedicated subscription"),
                pending
                    .on_image_available_handler
                    .take()
                    .expect("image available handler missing"),
                pending
                    .on_image_unavailable_handler
                    .take()
                    .expect("image unavailable handler missing"),
                pending.gateway_id,
                self.producer.clone(),
                Arc::clone(&self.publications),
            );
            let attach_result = {
                self.gateway_sessions.borrow_mut().attach_duologue(
                    pending.gateway_id,
                    pending.dedicated_session_id,
                    gateway_session,
                )
            };
            if let Err(mut unattached_session) = attach_result {
                if let Err(e) = unattached_session.close() {
                    error!(
                        target: "gateway_manager",
                        action = "unattached_session_close_failed",
                        gateway_id = pending.gateway_id,
                        error = ?e
                    );
                }
                return Err(ServerError::GatewayMessageError(format!(
                    "Reserved slot for gateway-{} disappeared",
                    pending.gateway_id
                )));
            }
            self.publications.set(
                pending.gateway_id,
                Arc::new(
                    pending
                        .publication
                        .take()
                        .expect("checked dedicated publication"),
                ),
            )?;
            pending.session_attached = true;

            debug!(
                target: "gateway_manager",
                action = "session_allocated",
                gateway_id = pending.gateway_id,
                session = format_args!("{:#x}", pending.dedicated_session_id),
                data_port = pending.ports[0],
                reply_port = pending.ports[1]
            );
        }

        if !pending.response_publication.is_connected() || now < pending.next_accept_attempt {
            return Ok(PendingHandshakeStatus::Pending);
        }

        // ACCEPT <order port the core listens on> <reply port the gateway listens on> <masked session>
        let accept_msg = format!(
            "{} gateway-{} ACCEPT {} {} {}",
            pending.handshake_session_id,
            pending.gateway_id,
            pending.ports[0],
            pending.ports[1],
            pending.encryption_key ^ pending.dedicated_session_id
        );
        match send_message(&pending.response_publication, accept_msg.as_bytes()) {
            Ok(()) => {
                info!(
                    target: "gateway_manager",
                    action = "gateway_accepted",
                    gateway_id = pending.gateway_id,
                    session = format_args!("{:#x}", pending.dedicated_session_id),
                    data_port = pending.ports[0],
                    reply_port = pending.ports[1]
                );
                Ok(PendingHandshakeStatus::Complete)
            }
            Err(e) => {
                pending.accept_attempts += 1;
                if pending.accept_attempts >= ACCEPT_RETRY_LIMIT {
                    return Err(ServerError::GatewayMessageError(format!(
                        "Failed to send ACCEPT message after {ACCEPT_RETRY_LIMIT} attempts: {e}"
                    )));
                }
                pending.next_accept_attempt = now + ACCEPT_RETRY_INTERVAL;
                Ok(PendingHandshakeStatus::Pending)
            }
        }
    }

    fn reap_idle_handshakes(&self) {
        let now = Instant::now();
        let expired = self.gateway_sessions.borrow().expired_pending(now);
        for gateway_id in expired {
            warn!(
                target: "gateway_manager",
                action = "idle_handshake_reaped",
                gateway_id,
                idle_seconds = HANDSHAKE_IDLE_TIMEOUT.as_secs()
            );
            if let Err(e) = self.remove_gateway_session(gateway_id) {
                warn!(
                    target: "gateway_manager",
                    action = "idle_handshake_cleanup_failed",
                    gateway_id,
                    error = %e
                );
            }
        }
    }

    /// Removes a gateway session and frees all associated resources
    pub fn remove_gateway_session(&self, gateway_id: u8) -> Result<(), ServerError> {
        if (gateway_id as usize) < MAX_GATEWAYS {
            self.pending_handshakes.borrow_mut()[gateway_id as usize] = None;
        }

        let slot = match self.gateway_sessions.borrow_mut().remove(gateway_id) {
            Some(slot) => slot,
            None => {
                return Err(ServerError::GatewayMessageError(format!(
                    "No active session for gateway-{gateway_id}"
                )));
            }
        };

        // remove publication
        self.publications.remove(gateway_id)?;

        // close subscription
        if let Some(mut session) = slot.duologue
            && let Err(e) = session.close()
        {
            error!(
                target: "gateway_manager",
                action = "session_close_failed",
                gateway_id,
                error = ?e
            );
        }

        // Mark ports as recently freed to avoid OS-level port binding race conditions
        // The OS may still have the UDP port bound even after Aeron closes the subscription
        let ports_to_free = [slot.port_data, slot.port_reply];
        self.port_allocator.mark_freed(&ports_to_free);
        debug!(
            target: "gateway_manager",
            action = "ports_marked_freed",
            gateway_id,
            data_port = ports_to_free[0],
            reply_port = ports_to_free[1]
        );

        Ok(())
    }
}

/// The gateway's reply endpoint: the host that sent HELLO, on the reply port the core allocated.
/// Unicast, so no one else can join the channel and read the replies.
fn reply_uri(hello_source: &str, port: u16, session_id: i32) -> Result<String, ServerError> {
    // Aeron's source identity is `host:port`, `[v6 host]:port` for IPv6.
    let (host, _) = hello_source.rsplit_once(':').ok_or_else(|| {
        ServerError::GatewayMessageError(format!("HELLO source {hello_source} has no host"))
    })?;
    Ok(format!(
        "aeron:udp?endpoint={host}:{port}|session-id={session_id}"
    ))
}

/// Frees the image handlers of a dedicated subscription that never attached to a session. Aeron
/// may invoke them until that subscription is closed, so its close notification releases them;
/// with no subscription (its registration failed or was never accepted) nothing references them.
fn release_unattached_image_handlers(
    gateway_id: u8,
    subscription: Option<AeronSubscription>,
    mut on_image_available_handler: Handler<DuologueImageAvailable>,
    mut on_image_unavailable_handler: Handler<DuologueImageUnavailable>,
) {
    let Some(subscription) = subscription else {
        on_image_available_handler.release();
        on_image_unavailable_handler.release();
        return;
    };
    let closed = Handler::leak(DuologueCloseNotification {
        gateway_id,
        on_image_available_handler,
        on_image_unavailable_handler,
    });
    if let Err(e) = subscription.close(Some(&closed)) {
        // The handlers stay allocated: leaking them is the safe failure.
        error!(
            target: "gateway_manager",
            action = "unattached_subscription_close_failed",
            gateway_id,
            error = ?e
        );
    }
}

/// Compares every byte whatever the first difference, so the time taken does not tell a guess
/// how much of it was right.
fn authenticate_gateway(
    authentication_key: &GatewayAuthenticationKey,
    credential: &str,
) -> Result<(), ServerError> {
    let (key, credential) = (
        authentication_key.as_str().as_bytes(),
        credential.as_bytes(),
    );
    let mut difference = u8::from(key.len() != credential.len());
    for i in 0..key.len().max(credential.len()) {
        difference |= key.get(i).copied().unwrap_or(0) ^ credential.get(i).copied().unwrap_or(0);
    }
    if difference == 0 {
        Ok(())
    } else {
        Err(ServerError::AuthenticationError(
            "Invalid credentials".to_string(),
        ))
    }
}

/// Fails closed: enabled authentication with an empty or malformed key must stop startup rather
/// than silently accept any credential. Running without authentication is stated explicitly by
/// `enable_authentication = false` (the composition root does that for dev/test).
pub(super) fn validate_authentication_config(
    enable_authentication: bool,
    authentication_key: &GatewayAuthenticationKey,
) -> Result<(), ServerError> {
    if enable_authentication {
        authentication_key
            .check()
            .map_err(ServerError::ConfigurationError)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::VexCoreServer;
    use crate::server::gateway_publications::SLOW_GATEWAY_TIMEOUT;
    use crate::server::progress::{DRAIN_TIMEOUT, wait_until};
    use common::{ORDERCOMMANDSIZE, encode_order_command};
    use disruptor::{BusySpin, build_multi_producer};
    use rusteron_archive::{
        AeronAvailableImageCallback, AeronAvailableImageLogger, AeronImage,
        AeronNotificationCallback, AeronReservedValueSupplierLogger, AeronUnavailableImageLogger,
        IntoCString,
    };
    use rusteron_media_driver::{AeronCError, AeronDriver, AeronDriverContext};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread::JoinHandle;

    #[test]
    fn slot_reservation_rejects_duplicate_and_out_of_range_ids() {
        let now = Instant::now();
        let mut session = Session::new();

        assert!(session.reserve(0, 101, [10_000, 10_001], now));
        assert!(!session.reserve(0, 102, [10_002, 10_003], now));
        assert!(!session.reserve(MAX_GATEWAYS as u8, 103, [10_004, 10_005], now));
        assert_eq!(session.get_ports_in_use(), vec![10_000, 10_001]);
        assert_eq!(session.get_sessions_in_use(), vec![101]);
    }

    #[test]
    fn only_awaiting_image_slots_expire_at_idle_timeout() {
        let allocated_at = Instant::now();
        let awaiting = GatewaySlotState::AwaitingImage { allocated_at };

        assert!(!handshake_is_expired(
            awaiting,
            allocated_at + HANDSHAKE_IDLE_TIMEOUT - Duration::from_nanos(1)
        ));
        assert!(handshake_is_expired(
            awaiting,
            allocated_at + HANDSHAKE_IDLE_TIMEOUT
        ));
        assert!(!handshake_is_expired(
            GatewaySlotState::Live,
            allocated_at + HANDSHAKE_IDLE_TIMEOUT + HANDSHAKE_IDLE_TIMEOUT
        ));
    }

    #[test]
    fn reaper_never_abandons_a_slot_whose_registrations_are_in_flight() {
        let allocated_at = Instant::now();
        let mut session = Session::new();
        assert!(session.reserve(1, 101, [10_000, 10_001], allocated_at));

        assert!(
            session
                .expired_pending(allocated_at + HANDSHAKE_IDLE_TIMEOUT + HANDSHAKE_IDLE_TIMEOUT)
                .is_empty()
        );
    }

    fn image_handlers(
        tx: &Sender<GatewaySessionEvent>,
    ) -> (
        Handler<DuologueImageAvailable>,
        Handler<DuologueImageUnavailable>,
    ) {
        (
            Handler::leak(DuologueImageAvailable {
                expected_session_id: 101,
                gateway_id: 1,
                tx: tx.clone(),
            }),
            Handler::leak(DuologueImageUnavailable {
                session_id: 101,
                gateway_id: 1,
                tx: tx.clone(),
            }),
        )
    }

    #[test]
    fn image_handlers_without_a_subscription_are_released_at_once() {
        let (tx, rx) = channel();
        let (available, unavailable) = image_handlers(&tx);
        drop(tx);
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        release_unattached_image_handlers(1, None, available, unavailable);

        assert!(matches!(rx.try_recv(), Err(TryRecvError::Disconnected)));
    }

    #[test]
    fn subscription_close_notification_releases_image_handlers() {
        let (tx, rx) = channel();
        let (on_image_available_handler, on_image_unavailable_handler) = image_handlers(&tx);
        let mut closed = DuologueCloseNotification {
            gateway_id: 1,
            on_image_available_handler,
            on_image_unavailable_handler,
        };
        drop(tx);
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        closed.handle_aeron_notification();

        assert!(matches!(rx.try_recv(), Err(TryRecvError::Disconnected)));
    }

    /// An embedded media driver in a directory of its own, which it stops and removes when
    /// dropped, whatever the test's outcome.
    struct EmbeddedDriver {
        dir: String,
        stop: Arc<AtomicBool>,
        thread: Option<JoinHandle<Result<(), AeronCError>>>,
    }

    impl EmbeddedDriver {
        fn launch(dir: String) -> Self {
            let context = AeronDriverContext::new().unwrap();
            context.set_dir(&dir.clone().into_c_string()).unwrap();
            context.set_dir_delete_on_start(true).unwrap();
            let (stop, thread) = AeronDriver::launch_embedded(context, false);
            Self {
                dir,
                stop,
                thread: Some(thread),
            }
        }
    }

    impl Drop for EmbeddedDriver {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// Holds Aeron's conductor thread, which calls it when an image of its subscription becomes
    /// available: reports that it holds it, then waits until `release` is sent to or dropped.
    struct HoldConductor {
        held: Sender<()>,
        release: Receiver<()>,
    }

    impl AeronAvailableImageCallback for HoldConductor {
        fn handle_aeron_on_available_image(&mut self, _: AeronSubscription, _: AeronImage) {
            let _ = self.held.send(());
            let _ = self.release.recv();
        }
    }

    /// A gateway still connected at shutdown must not keep the pipeline alive. Its session holds a
    /// clone of the ring producer, and the pipeline joins its consumers when the last clone drops,
    /// which must be the server thread's own producer. A clone left to Aeron's close notification
    /// was released on Aeron's conductor thread after that, or never once the Aeron client was torn
    /// down (#200). The test holds that thread while the session closes and the producers drop,
    /// so the outcome does not depend on when it would have run the notification.
    ///
    /// Runs an embedded media driver; the session and its gateway use IPC, so it binds no port.
    #[test]
    fn shutdown_releases_a_connected_sessions_ring_producer() {
        let driver = EmbeddedDriver::launch(format!(
            "/dev/shm/vex-session-release-test-{}",
            std::process::id()
        ));
        let mut config = CoreNetworkingConfig::test_defaults();
        config.context_dir = driver.dir.clone();
        let aeron = VexCoreServer::initialize_aeron(&config).unwrap();
        // The pipeline: one consumer, which reports each command, and by its drop, the join.
        let (seen, commands) = channel();
        let producer = build_multi_producer(64, OrderCommand::default, BusySpin)
            .handle_events_with(move |cell, _, _| {
                // SAFETY: Sole consumer reads its currently owned slot.
                let _ = seen.send(unsafe { (*cell.get()).order_id });
            })
            .build();
        let manager = GatewayManager::new(
            config,
            GatewayAuthenticationKey::default(),
            aeron.clone(),
            producer.clone(),
            Arc::new(Publications::new()),
        )
        .unwrap();

        // A session, attached as a handshake attaches one but on IPC, and the gateway feeding it.
        let (available, unavailable) = image_handlers(&manager.session_event_tx);
        let subscription = aeron
            .add_subscription(
                &"aeron:ipc?session-id=101".into_c_string(),
                DUOLOGUE_STREAM_ID,
                Some(&available),
                Some(&unavailable),
                DRAIN_TIMEOUT,
            )
            .unwrap();
        let session = Duologue::new(
            subscription,
            available,
            unavailable,
            1,
            producer.clone(),
            Arc::clone(&manager.publications),
        );
        {
            let mut sessions = manager.gateway_sessions.borrow_mut();
            assert!(sessions.reserve(1, 101, [10_000, 10_001], Instant::now()));
            assert!(sessions.attach_duologue(1, 101, session).is_ok());
        }
        let gateway = aeron
            .add_publication(
                &"aeron:ipc?term-length=65536|session-id=101".into_c_string(),
                DUOLOGUE_STREAM_ID,
                DRAIN_TIMEOUT,
            )
            .unwrap();

        // The gateway's command reaches the pipeline through the session.
        let mut command = [0; ORDERCOMMANDSIZE];
        let order = OrderCommand {
            order_id: 7,
            ..OrderCommand::default()
        };
        encode_order_command(&order, &mut command).unwrap();
        wait_until(DRAIN_TIMEOUT, || {
            Ok(gateway.offer::<AeronReservedValueSupplierLogger>(&command, None) >= 0)
        })
        .unwrap();
        wait_until(DRAIN_TIMEOUT, || {
            manager.poll()?;
            Ok(commands.try_recv() == Ok(7))
        })
        .unwrap();

        // Aeron's conductor thread, held in the image handler of a stream of the test's own.
        let (held_tx, held) = channel();
        let (release_conductor, release) = channel();
        let mut hold = Handler::leak(HoldConductor {
            held: held_tx,
            release,
        });
        let hold_stream_id = DUOLOGUE_STREAM_ID + 1;
        let hold_publication = aeron
            .add_publication(
                &"aeron:ipc?term-length=65536".into_c_string(),
                hold_stream_id,
                DRAIN_TIMEOUT,
            )
            .unwrap();
        let hold_subscription = aeron
            .async_add_subscription(
                &"aeron:ipc".into_c_string(),
                hold_stream_id,
                Some(&hold),
                None::<&Handler<AeronUnavailableImageLogger>>,
            )
            .unwrap();
        held.recv_timeout(DRAIN_TIMEOUT).unwrap();

        // As the server shuts down: it closes the sessions with the gateway connected, and drops
        // the gateway manager; then the engine drops its own producer, which must be the last.
        manager.shutdown_all_gateways().unwrap();
        drop(manager);
        drop(producer);
        let joined = commands.try_recv();

        drop(release_conductor);
        drop(hold_subscription);
        drop(hold_publication);
        drop(gateway);
        drop(aeron);
        hold.release();
        assert_eq!(
            joined,
            Err(TryRecvError::Disconnected),
            "a closed session kept a ring producer, so the pipeline never joined its consumer"
        );
    }

    /// A gateway that stops reading its replies must neither stall the replies to the others nor
    /// keep its session: its replies are offered once and dropped, and after the slow-gateway
    /// timeout the server thread closes its session.
    ///
    /// Runs an embedded media driver; the reply channels use IPC, so it binds no port.
    #[test]
    fn a_gateway_that_stops_reading_replies_loses_its_session() {
        let driver = EmbeddedDriver::launch(format!(
            "/dev/shm/vex-slow-gateway-test-{}",
            std::process::id()
        ));
        let mut config = CoreNetworkingConfig::test_defaults();
        config.context_dir = driver.dir.clone();
        let aeron = VexCoreServer::initialize_aeron(&config).unwrap();
        let producer = build_multi_producer(64, OrderCommand::default, BusySpin)
            .handle_events_with(|_, _, _| {})
            .build();
        let publications = Arc::new(Publications::new());
        let manager = GatewayManager::new(
            config,
            GatewayAuthenticationKey::default(),
            aeron.clone(),
            producer.clone(),
            Arc::clone(&publications),
        )
        .unwrap();

        // Two sessions, each with its reply channel and the gateway subscribed to it.
        let reply_channel = |gateway_id: u8| {
            let stream_id = DUOLOGUE_STREAM_ID + 10 + i32::from(gateway_id);
            let publication = aeron
                .add_publication(
                    &"aeron:ipc?term-length=65536".into_c_string(),
                    stream_id,
                    DRAIN_TIMEOUT,
                )
                .unwrap();
            let subscription = aeron
                .add_subscription(
                    &"aeron:ipc".into_c_string(),
                    stream_id,
                    None::<&Handler<AeronAvailableImageLogger>>,
                    None::<&Handler<AeronUnavailableImageLogger>>,
                    DRAIN_TIMEOUT,
                )
                .unwrap();
            wait_until(DRAIN_TIMEOUT, || Ok(publication.is_connected())).unwrap();
            assert!(manager.reserve_slot(gateway_id, 100 + i32::from(gateway_id)));
            let publication = Arc::new(publication);
            publications
                .set(gateway_id, Arc::clone(&publication))
                .unwrap();
            (publication, subscription)
        };
        // Gateway 1 never reads its replies; gateway 2 reads each one.
        let (slow_reply, _slow_gateway) = reply_channel(1);
        let (_, gateway) = reply_channel(2);
        let reply = |gateway_id: u8| OrderCommand {
            route_gateway_id: gateway_id,
            ..OrderCommand::default()
        };
        let answer_gateway_2 = || {
            publications.publish_response(&reply(2));
            wait_until(DRAIN_TIMEOUT, || Ok(gateway.poll_once(|_, _| {}, 1)? == 1)).unwrap();
        };

        // Fill gateway 1's window.
        for _ in 0..10_000 {
            if slow_reply.position() >= slow_reply.position_limit() {
                break;
            }
            publications.publish_response(&reply(1));
            answer_gateway_2();
        }
        assert!(slow_reply.position() >= slow_reply.position_limit());

        // Its replies now fail, at the cost of one offer each, never a wait.
        let started = Instant::now();
        for _ in 0..20 {
            publications.publish_response(&reply(1));
        }
        let stalled = started.elapsed();
        assert!(
            stalled < Duration::from_millis(100),
            "20 replies to a gateway that does not read took {stalled:?}"
        );
        answer_gateway_2();
        manager.poll().unwrap();
        assert!(manager.is_gateway_connected(1));

        // Undeliverable for the timeout: its session goes, gateway 2's stays.
        std::thread::sleep(SLOW_GATEWAY_TIMEOUT);
        publications.publish_response(&reply(1));
        manager.poll().unwrap();
        assert!(!manager.is_gateway_connected(1));
        assert!(publications.get(1).unwrap().is_none());
        assert!(manager.is_gateway_connected(2));
        assert!(publications.get(2).unwrap().is_some());
        answer_gateway_2();
    }

    /// Runs an embedded media driver; the handshake reply uses IPC, so it binds no port.
    #[test]
    fn allocation_failure_after_authentication_answers_unavailable() {
        let driver = EmbeddedDriver::launch(format!(
            "/dev/shm/vex-unavailable-test-{}",
            std::process::id()
        ));
        let mut config = CoreNetworkingConfig::test_defaults();
        config.context_dir = driver.dir.clone();
        let aeron = VexCoreServer::initialize_aeron(&config).unwrap();
        let producer = build_multi_producer(64, OrderCommand::default, BusySpin)
            .handle_events_with(|_, _, _| {})
            .build();
        let manager = GatewayManager::new(
            config.clone(),
            GatewayAuthenticationKey::default(),
            aeron.clone(),
            producer,
            Arc::new(Publications::new()),
        )
        .unwrap();
        // Every port of the pool in its cooldown, as after a burst of reconnects.
        let pool: Vec<u16> = (config.base_gateway_port..).take(PORT_POOL).collect();
        manager.port_allocator.mark_freed(&pool);

        let channel = "aeron:ipc".into_c_string();
        let replies = aeron
            .add_subscription(
                &channel,
                DUOLOGUE_STREAM_ID,
                None::<&Handler<AeronAvailableImageLogger>>,
                None::<&Handler<AeronUnavailableImageLogger>>,
                DRAIN_TIMEOUT,
            )
            .unwrap();
        let handshake = aeron
            .add_publication(&channel, DUOLOGUE_STREAM_ID, DRAIN_TIMEOUT)
            .unwrap();
        wait_until(DRAIN_TIMEOUT, || Ok(handshake.is_connected())).unwrap();

        let hello = b"HELLO gateway-1 - 3";
        assert!(matches!(
            manager.process_handshake_message(&handshake, 7, "127.0.0.1:40000", hello),
            Err(ServerError::ResourceAllocationError(_))
        ));

        let mut reply = String::new();
        wait_until(DRAIN_TIMEOUT, || {
            let read = replies.poll_once(
                |message, _| reply = String::from_utf8_lossy(message).into_owned(),
                1,
            )?;
            Ok(read == 1)
        })
        .unwrap();
        assert_eq!(reply, "7 gateway-1 UNAVAILABLE 10");
        assert!(!manager.is_gateway_connected(1));
    }

    #[test]
    fn handshake_rate_limit_uses_a_rolling_window() {
        let start = Instant::now();
        let mut limiter = HandshakeRateLimiter::new();

        for _ in 0..MAX_GATEWAYS {
            assert!(limiter.allow(start));
        }
        assert!(!limiter.allow(start));
        assert!(!limiter.allow(start + HANDSHAKE_RATE_WINDOW - Duration::from_nanos(1)));
        assert!(limiter.allow(start + HANDSHAKE_RATE_WINDOW));
    }

    #[test]
    fn reply_publication_is_unicast_to_the_hello_host() {
        assert_eq!(
            reply_uri("172.28.0.7:41234", 50_001, 77).unwrap(),
            "aeron:udp?endpoint=172.28.0.7:50001|session-id=77"
        );
        assert_eq!(
            reply_uri("[fd00::7]:41234", 50_001, 77).unwrap(),
            "aeron:udp?endpoint=[fd00::7]:50001|session-id=77"
        );
        assert!(reply_uri("aeron-session-5", 50_001, 77).is_err());
    }

    #[test]
    fn credential_is_compared_as_bytes() {
        let hex64 = "0123456789abcdef".repeat(4);
        let key = GatewayAuthenticationKey::from(hex64.as_str());
        let one_off = format!("{}0", &hex64[..63]);

        assert!(authenticate_gateway(&key, &hex64).is_ok());
        assert!(authenticate_gateway(&key, &one_off).is_err());
        assert!(authenticate_gateway(&key, &hex64[..32]).is_err());
        assert!(authenticate_gateway(&key, &format!("{hex64}0")).is_err());
    }

    #[test]
    fn enabled_authentication_requires_a_long_key() {
        for key in ["", "123456789", &"a b".repeat(16)] {
            assert!(matches!(
                validate_authentication_config(true, &GatewayAuthenticationKey::from(key)),
                Err(ServerError::ConfigurationError(_))
            ));
        }
        let hex64 = "0123456789abcdef".repeat(4);
        assert!(
            validate_authentication_config(true, &GatewayAuthenticationKey::from(hex64.as_str()))
                .is_ok()
        );
        assert!(
            validate_authentication_config(false, &GatewayAuthenticationKey::default()).is_ok()
        );
    }
}
