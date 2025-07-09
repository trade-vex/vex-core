use std::ffi::{CString};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::{collections::HashMap, sync::RwLock};

// use regex::Regex;
use rusteron_client::{
    Aeron, AeronCError, AeronContext, AeronFragmentAssembler, AeronFragmentHandlerCallback,
    AeronHeader, AeronImage, AeronPublication, AeronReservedValueSupplierLogger, AeronSubscription,
    Handler, AeronAvailableImageCallback, AeronUnavailableImageCallback,
};
use thiserror::Error;
use tracing::{debug, error, warn};

const ECHO_STREAM_ID: i32 = 1002;

// Callback handler for client connections
struct ClientConnectedHandler {
    clients: Arc<RwLock<HashMap<i32, ServerClient>>>,
    aeron: Arc<Aeron>,
}

impl ClientConnectedHandler {
    fn new(clients: Arc<RwLock<HashMap<i32, ServerClient>>>, aeron: Arc<Aeron>) -> Self {
        Self { clients, aeron }
    }
}

impl AeronAvailableImageCallback for ClientConnectedHandler {
    fn handle_aeron_on_available_image(
        &mut self,
        _subscription: AeronSubscription,
        image: AeronImage,
    ) {
        let session = image.get_constants().unwrap().session_id;
        debug!("client connected: {:?}", image.get_constants().unwrap().session_id());

        let client = ServerClient::new(session, image, Arc::clone(&self.aeron));
        self.clients.write().unwrap().insert(session, client);
    }
}

// Callback handler for client disconnections
struct ClientDisconnectedHandler {
    clients: Arc<RwLock<HashMap<i32, ServerClient>>>,
}

impl ClientDisconnectedHandler {
    fn new(clients: Arc<RwLock<HashMap<i32, ServerClient>>>) -> Self {
        Self { clients }
    }
}

impl AeronUnavailableImageCallback for ClientDisconnectedHandler {
    fn handle_aeron_on_unavailable_image(
        &mut self,
        _subscription: AeronSubscription,
        image: AeronImage,
    ) {
        let session = image.get_constants().unwrap().session_id();
        // debug!("onClientDisconnected: {}", image.source_identity());

        if let Some(_) = self.clients.write().unwrap().remove(&session) {
            debug!(
                "onClientDisconnected: closing client for session 0x{:x}",
                session as u32
            );
            // Client will be dropped automatically, triggering cleanup
        }
    }
}


struct FragmentHandler {
    clients: Arc<RwLock<HashMap<i32, ServerClient>>>,
}

impl FragmentHandler {
    fn new(clients: Arc<RwLock<HashMap<i32, ServerClient>>>) -> Self {
        Self { clients }
    }
}

impl AeronFragmentHandlerCallback for FragmentHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) {
        let session = header.get_values().unwrap().frame.session_id;
        let mut clients = self.clients.write().unwrap();
        let client = clients.get_mut(&session);

        if let Some(client) = client {
            let message = String::from_utf8_lossy(&buffer);
            debug!("server received message: {}", message);
            if let Err(e) = client.on_receive_message(&message) {
                error!("Error handling message: {}", e);
            }
        } else {
            warn!("received message from unknown client: {}", session);
        }
    }
}

#[derive(Error, Debug)]
pub enum EchoServerError {
    #[error("Media driver initialization failed: {0}")]
    MediaDriverError(String),

    #[error("Aeron connection failed: {0}")]
    AeronConnectionError(#[from] AeronCError),

    #[error("AeronSubscription setup failed: {0}")]
    SubscriptionError(String),

    #[error("AeronPublication creation failed: {0}")]
    PublicationError(String),

    #[error("Invalid client message: {0}")]
    InvalidClientMessage(String),

    #[error("URI parsing error: {0}")]
    UriParseError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Invalid input: {0}")]
    InvalidInput(#[from] std::ffi::NulError),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ClientState {
    Initial,
    Connected,
}

struct ServerClient {
    session: i32,
    image: AeronImage,
    aeron: Arc<Aeron>,
    state: ClientState,
    buffer: [u8; 2048],
    publication: Option<AeronPublication>,
}

impl ServerClient {
    fn new(session: i32, image: AeronImage, aeron: Arc<Aeron>) -> Self {
        let buffer = [0u8; 2048];

        Self {
            session,
            image,
            aeron,
            state: ClientState::Initial,
            buffer,
            publication: None,
        }
    }

    fn on_receive_message(&mut self, message: &str) -> Result<(), EchoServerError> {
        debug!("receive [0x{:x}]: {}", self.session as u32, message);

        match self.state {
            ClientState::Initial => self.on_receive_message_initial(message),
            ClientState::Connected => {
                if let Some(ref publication) = self.publication {
                    Self::send_message(publication, &mut self.buffer, message)?;
                }
                Ok(())
            }
        }
    }

    fn on_receive_message_initial(&mut self, message: &str) -> Result<(), EchoServerError> {
        debug!("server received initial message: {}", message);
        let parts: Vec<&str> = message.split_whitespace().collect();

        if parts.len() != 2 || parts[0] != "HELLO" {
            return Err(EchoServerError::InvalidClientMessage(format!(
                "Malformed HELLO message: {}",
                message
            )));
        }

        let port: u16 = parts[1].parse().map_err(|_| {
            EchoServerError::InvalidClientMessage(format!(
                "Invalid port in HELLO message: {}",
                message
            ))
        })?;

        let binding = self.image.get_constants().unwrap();
        let source_identity = binding.source_identity();

        // Extract the host directly from the source identity
        let host = source_identity.split(':').next().ok_or_else(|| {
            EchoServerError::InvalidClientMessage(
                "Could not extract host from source identity".to_string(),
            )
        })?;

        let uri = CString::new(format!(
            "aeron:udp?endpoint={}:{}",
            host, port
        ))
        .unwrap();

        let publication = self
            .aeron
            .add_publication(
                &uri,
                ECHO_STREAM_ID,
                Duration::from_secs(1),
            )
            .map_err(|e| EchoServerError::PublicationError(e.to_string()))?;

        self.publication = Some(publication);
        self.state = ClientState::Connected;

        Ok(())
    }

    fn send_message(
        publication: &AeronPublication,
        buffer: &mut [u8; 2048],
        text: &str,
    ) -> Result<bool, EchoServerError> {
        debug!(
            "send: [session 0x{:x}] {}",
            publication.session_id() as u32,
            text
        );

        let value = text.as_bytes();
        if value.len() > buffer.len() {
            return Err(EchoServerError::InvalidClientMessage(
                "Message too long".to_string(),
            ));
        }
        buffer[..value.len()].copy_from_slice(value);

        for _ in 0..5 {
            let result = publication.offer::<AeronReservedValueSupplierLogger>(
                buffer,
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
}

pub struct EchoServer {
    aeron: Arc<Aeron>,
    local_address: SocketAddr,
    clients: Arc<RwLock<HashMap<i32, ServerClient>>>,
}

impl EchoServer {
    pub fn create(context_dir: &str, local_address: SocketAddr) -> Result<Self, EchoServerError> {
        let ctx = AeronContext::new()?;
        let context_dir = std::ffi::CString::new(context_dir)?;
        ctx.set_dir(&context_dir)?;
        ctx.set_driver_timeout_ms(1_000)?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let clients = Arc::new(RwLock::new(HashMap::new()));

        Ok(Self {
            aeron: Arc::new(aeron),
            local_address,
            clients,
        })
    }

    pub fn run(&self) -> Result<(), EchoServerError> {
        let subscription = self.setup_subscription()?;
        self.run_loop(subscription)
    }

    fn run_loop(&self, subscription: AeronSubscription) -> Result<(), EchoServerError> {
        // Create FragmentHandler with access to clients
        let fragment_handler = FragmentHandler::new(Arc::clone(&self.clients));
        
        let fragment_handler = AeronFragmentAssembler::new(
            Some(&Handler::leak(fragment_handler)),
        )?;
        
        let fragment_handler = Handler::leak(fragment_handler);
        
        loop {
            if subscription.is_connected() {
                subscription.poll(Some(&fragment_handler), 10)?;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn setup_subscription(&self) -> Result<AeronSubscription, EchoServerError> {
        let endpoint = self.local_address.to_string();
        let sub_uri = CString::new(format!(
            "aeron:udp?endpoint={}",
            endpoint
        ))
        .unwrap();

        debug!("subscription URI: {:?}", sub_uri);

        // Create the callback handlers
        let connected_handler = ClientConnectedHandler::new(
            Arc::clone(&self.clients),
            Arc::clone(&self.aeron)
        );
        let disconnected_handler = ClientDisconnectedHandler::new(Arc::clone(&self.clients));

        let subscription = self
            .aeron
            .add_subscription(
                &sub_uri,
                ECHO_STREAM_ID,
                Some(&Handler::leak(connected_handler)),
                Some(&Handler::leak(disconnected_handler)),
                Duration::from_secs(1),
            )
            .map_err(|e| EchoServerError::SubscriptionError(e.to_string()))?;

        Ok(subscription)
    }
}
