use std::sync::Arc;
use std::{collections::HashMap, sync::RwLock};

// use regex::Regex;
use rusteron_client::{
    Aeron, AeronCError, AeronContext, AeronFragmentHandlerCallback,
    AeronHeader, AeronImage, AeronPublication, AeronSubscription,
    Handler, AeronAvailableImageCallback, AeronUnavailableImageCallback,
};
use thiserror::Error;
use tracing::{debug, error};

use crate::server::config::ServerConfig;
use crate::server::duologue::Duologue;
use crate::server::utils::{new_publication_with_mdc, new_subsciption_with_handlers, send_message, PortAllocator, SessionAllocator};

const ECHO_STREAM_ID: i32 = 1002;

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

    #[error("Port allocation error: {0}")]
    PortAllocationError(String),

    #[error("Session allocation error: {0}")]
    SessionAllocationError(String),
}

pub struct EchoServer {
    aeron: Arc<Aeron>,
    config: ServerConfig,
    clients: Arc<RwLock<ClientState>>
}

impl EchoServer {

    pub fn new(config: ServerConfig) -> Result<Self, EchoServerError> {
        let ctx = AeronContext::new()?;
        let context_dir = std::ffi::CString::new(config.context_dir.clone())?;
        ctx.set_dir(&context_dir)?;
        ctx.set_driver_timeout_ms(1_000)?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        Ok(Self { config: config.clone(), clients: Arc::new(RwLock::new(ClientState::new(Arc::new(aeron.clone()), config)?)), aeron: Arc::new(aeron) })
    }

    pub fn run(&self) -> Result<(), EchoServerError> {
        let publication = new_publication_with_mdc(&self.aeron, &self.config.local_address, self.config.initial_port, ECHO_STREAM_ID)?;
        let mc_image_available_handler = MCImageAvailableHandler{ clients: self.clients.clone() };
        let mc_image_unavailable_handler = MCImageUnavailableHandler{ clients: self.clients.clone() };
        let subscription = new_subsciption_with_handlers(&self.aeron, &self.config.local_address, self.config.initial_port, ECHO_STREAM_ID, mc_image_available_handler, mc_image_unavailable_handler)?;
        let mut fragment_handler = InitialMessageHandler::new(self.clients.clone(), publication);
        loop {
            subscription.poll(Some(&Handler::leak(&mut fragment_handler)), 10)?;
            self.clients.write().unwrap().poll()?;
        }
        Ok(())
    }
}

struct ClientState {
    client_session_address: HashMap<i32, String>,
    client_duologues: HashMap<i32, Duologue>,
    aeron: Arc<Aeron>,
    config: ServerConfig,
    buffer: [u8; 2048],
    address_counter: HashMap<String, u16>,
    port_allocator: PortAllocator,
    session_allocator: SessionAllocator,
}

impl ClientState {
    fn new(aeron: Arc<Aeron>, config: ServerConfig) -> Result<Self, EchoServerError> {
        Ok(Self {
            client_session_address: HashMap::new(),
            client_duologues: HashMap::new(),
            aeron,
            port_allocator: PortAllocator::new(config.base_client_port, config.max_clients.into())?,
            session_allocator: SessionAllocator::new(config.reserved_session_id_low, config.reserved_session_id_high)?,
            config,
            buffer: [0u8; 2048],
            address_counter: HashMap::new(),
        })
    }

    fn process_initial_message(&mut self, publication: &AeronPublication, session_name: &str, session_id: i32, message: &str) -> Result<(), EchoServerError> {
        debug!("[0x{:x}] received initial message: {}", session_id, message);

        // accept Hello key
        let parts: Vec<&str> = message.split_whitespace().collect();
        if parts.len() != 2 || parts[0] != "HELLO" {
            send_message(publication, &mut self.buffer, "ERROR: Malformed HELLO message")?;
            return Ok(());
        }
        let key = parts[1];
         
        // check if too many clients are connected
        if self.client_duologues.len() >= self.config.max_clients.into() {
            send_message(publication, &mut self.buffer, "ERROR: Too many clients connected")?;
            return Ok(());
        }

        // check if this ip has many connections
        let owner = self.client_session_address.get(&session_id);
        if let Some(owner) = owner {
            if owner.len() >= self.config.max_connections_per_address.into() {
                send_message(publication, &mut self.buffer, "ERROR: Too many connections from this IP")?;
                return Ok(());
            }
        }
        let owner = owner.unwrap().to_string();

        // parse the key to int which will be used as one time padding key to send encrypted messages
        let key = key.parse::<i32>().map_err(|e| EchoServerError::InvalidClientMessage(format!("Invalid key: {}", e)))?;
        // Allocate a new duologue
        let (session, ports) = self.allocate_duologue(session_name, session_id, &owner)?;
        // encrypted session
        let encrypted_session = key ^ session;
        let message = format!("{} CONNECT {} {} {}", session_name, ports[0], ports[1], encrypted_session);
        send_message(publication, &mut self.buffer, &message)?;
        Ok(())
    }

    fn allocate_duologue(&mut self, session_name: &str, session_id: i32, owner: &str) -> Result<(i32, [u16; 2]), EchoServerError> {
        // increment the address counter for this session name // move to last?
        let counter = self.address_counter.entry(owner.to_string()).or_insert(0);
        *counter += 1;

        // allocate 2 new ports
        let ports = self.port_allocator.allocate(2)?;
        let session = self.session_allocator.allocate()?;

        // allocate a new session
        let duologue = Duologue::new(&self.aeron, &self.config.local_address, owner , ports[0], ports[1], session)?;
        self.client_duologues.insert(session_id, duologue);
        self.client_session_address.insert(session_id, owner.to_string());

        debug!("allocated duologue for session 0x{} with ports {} and {}", session_name, ports[0], ports[1]);

        Ok((session, [ports[0], ports[1]]))
    }

    pub fn poll(&mut self) -> Result<(), EchoServerError> {
        let mut client_iter = self.client_duologues.iter_mut();
        while let Some((_session_id, duologue)) = client_iter.next() {
            let mut delete = false;
            if duologue.is_expired() {
                delete = true;
            }

            if duologue.is_closed() {
                delete = true;
            }

            if delete {
                duologue.close()?;
                self.port_allocator.free(duologue.port_data);
                self.port_allocator.free(duologue.port_control);
                self.address_counter.entry(duologue.owner.clone()).and_modify(|c| *c -= 1);
                continue;
            }

            duologue.poll()?;
        }
        Ok(())
    }
}
struct InitialMessageHandler{
    clients: Arc<RwLock<ClientState>>,
    publication: AeronPublication,
}

impl InitialMessageHandler {
    fn new(clients: Arc<RwLock<ClientState>>, publication: AeronPublication) -> Self {
        Self { clients, publication }
    }
}

impl AeronFragmentHandlerCallback for &mut InitialMessageHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) {
        let message = String::from_utf8_lossy(buffer);
        debug!("InitialMessageHandler: {:?}", message);
        let session_name = header.get_values().unwrap().frame.session_id.to_string();
        let session_id = header.get_values().unwrap().frame.session_id;
        let message = String::from_utf8_lossy(buffer);

        let mut clients = self.clients.write().unwrap();

        match clients.process_initial_message(&self.publication, &session_name, session_id, &message) {
            Ok(_) => {},
            Err(e) => {
                error!("Error processing initial message: {}", e);
            }
        }
    }
}

struct MCImageAvailableHandler{
    clients: Arc<RwLock<ClientState>>,
}

impl AeronAvailableImageCallback for MCImageAvailableHandler {
    fn handle_aeron_on_available_image(
        &mut self,
        _subscription: AeronSubscription,
        image: AeronImage,
    ) {
        let session_id = image.get_constants().unwrap().session_id;
        let binding = image.get_constants().unwrap();
        let address = binding.source_identity();
        debug!("Main Channel: Image available for session 0x{} from {}", session_id, address);
        let mut clients = self.clients.write().unwrap();
        clients.client_session_address.insert(session_id, address.to_string());
    }
}

struct MCImageUnavailableHandler{
    clients: Arc<RwLock<ClientState>>,
}

impl AeronUnavailableImageCallback for MCImageUnavailableHandler {
    fn handle_aeron_on_unavailable_image(
        &mut self,
        _subscription: AeronSubscription,
        image: AeronImage,
    ) {
        let session_id = image.get_constants().unwrap().session_id;
        let binding = image.get_constants().unwrap();
        let address = binding.source_identity();
        debug!("Main Channel: Image unavailable for session 0x{} from {}", session_id, address);
        let mut clients = self.clients.write().unwrap();
        clients.client_session_address.remove(&session_id);
    }
}
