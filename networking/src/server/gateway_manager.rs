use crate::server::duologue::{
    DUOLOGUE_STREAM_ID, Duologue, DuologueImageAvailable, DuologueImageUnavailable,
};
use crate::server::gateway_publications::Publications;
use crate::utils::{PortAllocator, SessionAllocator, send_message};
use common::{MAX_GATEWAYS, OrderCommand};
use disruptor::{MultiProducer, SingleConsumerBarrier};
use rusteron_archive::{
    Aeron, AeronAsyncAddPublication, AeronAsyncAddSubscription, AeronPublication,
    AeronSubscription, Handler,
};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::ffi::CString;
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::sync::{Arc, RwLock};
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

fn gateway_port_capacity(max_gateways: u16) -> usize {
    usize::from(max_gateways) * PORTS_PER_GATEWAY
}

pub struct Session {
    slots: [Option<GatewaySlot>; MAX_GATEWAYS],
}

pub struct GatewaySlot {
    duologue: Option<Duologue>,
    port_data: u16,
    port_control: u16,
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
            port_control: ports[1],
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

    fn gateway_ids(&self) -> Vec<u8> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(gateway_id, slot)| slot.as_ref().map(|_| gateway_id as u8))
            .collect()
    }

    fn expired_pending(&self, now: Instant) -> Vec<u8> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(gateway_id, slot)| {
                slot.as_ref()
                    .filter(|slot| handshake_is_expired(slot.state, now))
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
    gateway_sessions: RwLock<Session>,
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
            gateway_sessions: RwLock::new(Session::new()),
            aeron,
            port_allocator: PortAllocator::new(
                config.base_gateway_port,
                gateway_port_capacity(config.max_gateways),
            )
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
                        .write()
                        .unwrap()
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
                        .read()
                        .unwrap()
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

        let encryption_key_str = parts.next().ok_or_else(|| {
            ServerError::GatewayMessageError("Missing encryption key".to_string())
        })?;

        let encryption_key = match encryption_key_str.parse::<i32>() {
            Ok(key) => key,
            Err(_) if self.authentication_is_enabled() => {
                return self.reject_handshake(
                    publication,
                    session_id,
                    gateway_id_str,
                    source,
                    ServerError::AuthenticationError("Invalid credentials".to_string()),
                );
            }
            Err(e) => {
                return Err(ServerError::GatewayMessageError(format!(
                    "Invalid encryption key: {e}"
                )));
            }
        };

        // Authenticate before parsing the id, checking for duplicates, or allocating any
        // resource, so an unauthenticated peer can never reach the allocator (PR #176).
        if self.authentication_is_enabled()
            && let Err(e) = self.authenticate_gateway(encryption_key)
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

        self.allocate_gateway_session(gateway_id, session_id, encryption_key, publication, now)?;

        Ok(())
    }

    /// Polls all active gateway sessions
    pub fn poll(&self) -> Result<(), ServerError> {
        self.process_session_events();
        self.process_pending_handshakes();
        self.reap_idle_handshakes();

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
            .gateway_ids();

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
        warn!(
            target: "gateway_manager",
            action = "handshake_rejected",
            source,
            session = format_args!("{:#x}", session_id),
            reason = %error
        );
        let error_msg = format!("{session_id} {gateway_label} REJECT Handshake rejected");
        send_message(publication, error_msg.as_bytes())?;
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
        now: Instant,
    ) -> Result<(), ServerError> {
        let (ports_in_use, sessions_in_use) = {
            let guard = self.gateway_sessions.read().unwrap();
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

        if !self.gateway_sessions.write().unwrap().reserve(
            gateway_id,
            dedicated_session_id,
            ports,
            now,
        ) {
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

        let publication_uri = CString::new(format!(
            "aeron:udp?control={}:{}|control-mode=dynamic|session-id={dedicated_session_id}",
            self.config.local_address, ports[1]
        ))
        .expect("controlled Aeron publication URI contains no NUL");
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
            if pending.publication.is_none() {
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
                self.gateway_sessions.write().unwrap().attach_duologue(
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
                control_port = pending.ports[1]
            );
        }

        if !pending.response_publication.is_connected() || now < pending.next_accept_attempt {
            return Ok(PendingHandshakeStatus::Pending);
        }

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
                    control_port = pending.ports[1]
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
        let expired = self.gateway_sessions.read().unwrap().expired_pending(now);
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

    fn authenticate_gateway(&self, credentials: i32) -> Result<(), ServerError> {
        authenticate_gateway(&self.authentication_key, credentials)
    }

    /// Removes a gateway session and frees all associated resources
    pub fn remove_gateway_session(&self, gateway_id: u8) -> Result<(), ServerError> {
        if (gateway_id as usize) < MAX_GATEWAYS {
            self.pending_handshakes.borrow_mut()[gateway_id as usize] = None;
        }

        let slot = match self.gateway_sessions.write().unwrap().remove(gateway_id) {
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
        let ports_to_free = [slot.port_data, slot.port_control];
        self.port_allocator.mark_freed(&ports_to_free);
        debug!(
            target: "gateway_manager",
            action = "ports_marked_freed",
            gateway_id,
            data_port = ports_to_free[0],
            control_port = ports_to_free[1]
        );

        Ok(())
    }
}

fn constant_time_i32_eq(left: i32, right: i32) -> bool {
    let mut difference = 0_u8;
    for (left_byte, right_byte) in left.to_ne_bytes().iter().zip(right.to_ne_bytes()) {
        difference |= left_byte ^ right_byte;
    }
    difference == 0
}

fn authenticate_gateway(
    authentication_key: &GatewayAuthenticationKey,
    credentials: i32,
) -> Result<(), ServerError> {
    let expected = authentication_key.as_i32().ok_or_else(|| {
        ServerError::ConfigurationError(
            "Gateway authentication key must be a signed 32-bit integer".to_string(),
        )
    })?;

    if constant_time_i32_eq(expected, credentials) {
        Ok(())
    } else {
        Err(ServerError::AuthenticationError(
            "Invalid credentials".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn correct_gateway_credential_is_accepted() {
        let key = GatewayAuthenticationKey::from("123456789");
        assert!(authenticate_gateway(&key, 123456789).is_ok());
    }

    #[test]
    fn wrong_gateway_credential_is_rejected_without_a_slot() {
        let key = GatewayAuthenticationKey::from("123456789");
        let sessions = Session::new();

        assert!(authenticate_gateway(&key, 987654321).is_err());
        assert!(!sessions.is_gateway_connected(1));
    }

    #[test]
    fn port_allocator_has_two_ports_for_every_gateway() {
        let allocator = PortAllocator::new(
            10_000,
            gateway_port_capacity(MAX_GATEWAYS.try_into().unwrap()),
        )
        .unwrap();

        assert_eq!(allocator.total_ports(), MAX_GATEWAYS * PORTS_PER_GATEWAY);
    }
}
