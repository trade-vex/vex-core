use rusteron_client::{
    Aeron, AeronCError, AeronContext, AeronFragmentAssembler, AeronFragmentHandlerCallback,
    AeronHeader, AeronPublication, AeronReservedValueSupplierLogger, Handler, AeronAvailableImageLogger, AeronUnavailableImageLogger,
    AeronSubscription,
};
use rand;
use std::ffi::CString;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::{debug, error, info, warn};

const ALL_CLIENTS_STREAM_ID: i32 = 1001;
const DUOLOGUE_STREAM_ID: i32 = 1002;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Represents the server's response to our initial HELLO.
#[derive(Debug, PartialEq)]
enum ServerResponse {
    Connect {
        port: u16,
        control_port: u16,
        encrypted_session: u32,
    },
    Error(String),
    Ignore,
}

/// Parses a server response message.
fn parse_server_response(message: &str, expected_session: i32) -> ServerResponse {
    let parts: Vec<&str> = message.split_whitespace().collect();
    if parts.len() < 2 {
        return ServerResponse::Ignore;
    }

    let session_id = match parts[0].parse::<i32>() {
        Ok(id) => id,
        Err(_) => return ServerResponse::Ignore,
    };

    if session_id != expected_session {
        warn!(
            "Ignoring message for another session. Expected: {}, Got: {}",
            expected_session, session_id
        );
        return ServerResponse::Ignore;
    }

    match parts[1] {
        "CONNECT" if parts.len() == 5 => {
            let port = parts[2].parse().ok();
            let control_port = parts[3].parse().ok();
            let encrypted_session = u32::from_str_radix(parts[4], 16).ok();

            if let (Some(port), Some(control_port), Some(encrypted_session)) =
                (port, control_port, encrypted_session)
            {
                ServerResponse::Connect {
                    port,
                    control_port,
                    encrypted_session,
                }
            } else {
                ServerResponse::Error("Malformed CONNECT message".to_string())
            }
        }
        "ERROR" => ServerResponse::Error(parts[2..].join(" ")),
        _ => ServerResponse::Ignore,
    }
}

/// A shared state for the fragment handler to communicate with the main thread.
type SharedResponse = Arc<Mutex<Option<ServerResponse>>>;

/// Fragment handler for parsing the server's CONNECT/ERROR response.
struct ConnectResponseHandler {
    response: SharedResponse,
    expected_session: i32,
}

impl AeronFragmentHandlerCallback for ConnectResponseHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
        let message = String::from_utf8_lossy(buffer);
        debug!("Received initial response from server: {}", message);
        let parsed = parse_server_response(&message, self.expected_session);
        if parsed != ServerResponse::Ignore {
            *self.response.lock().unwrap() = Some(parsed);
        }
    }
}

struct EchoLoopHandler;

impl AeronFragmentHandlerCallback for EchoLoopHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
        let message = String::from_utf8_lossy(buffer);
        info!("ECHO response: {}", message);
    }
}


#[derive(Error, Debug)]
pub enum VexClientV2Error {
    #[error("Aeron operation failed: {0}")]
    AeronError(#[from] AeronCError),
    #[error("Invalid CString: {0}")]
    NulError(#[from] std::ffi::NulError),
    #[error("Connection timed out: {0}")]
    Timeout(String),
    #[error("Server returned an error: {0}")]
    ServerError(String),
    #[error("Failed to send message: {0}")]
    SendError(String),
    #[error("Protocol error: {0}")]
    ProtocolError(String),
}

/// A more robust echo client that supports NAT traversal (Take 2).
pub struct VexClientV2 {
    aeron: Arc<Aeron>,
    server_addr: SocketAddr,
    buffer: [u8; 2048],
}

impl VexClientV2 {
    pub fn create(
        context_dir: &str,
        server_addr: SocketAddr,
    ) -> Result<Self, VexClientV2Error> {
        let ctx = AeronContext::new()?;
        let context_dir = CString::new(context_dir)?;
        ctx.set_dir(&context_dir)?;
        ctx.set_driver_timeout_ms(5_000)?;
        // Reserve session IDs for duologues

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;
        info!("VexClientV2 started");

        Ok(Self {
            aeron: Arc::new(aeron),
            server_addr,
            buffer: [0u8; 2048],
        })
    }

    pub fn run(&mut self) -> Result<(), VexClientV2Error> {
        // Phase 1: Connect to the "all clients" channel and get duologue details.
        let duologue_key = rand::random::<u32>();
        let (duologue_port, duologue_control_port, duologue_session_id) =
            self.connect_to_all_clients_channel(duologue_key)?;

        info!(
            "Received duologue details. Port: {}, Control Port: {}, Session ID: {}",
            duologue_port, duologue_control_port, duologue_session_id
        );

        // Phase 2: Connect to the dedicated duologue channel and start echoing.
        self.run_echo_loop(
            duologue_port,
            duologue_control_port,
            duologue_session_id,
        )
    }

    fn setup_publication(
        &self,
        uri: &str,
        stream_id: i32,
    ) -> Result<AeronPublication, VexClientV2Error> {
        let uri = CString::new(uri)?;
        let publication = self.aeron.add_publication(&uri, stream_id, CONNECT_TIMEOUT)?;

        let start = Instant::now();
        while !publication.is_connected() {
            if start.elapsed() > CONNECT_TIMEOUT {
                return Err(VexClientV2Error::Timeout(format!(
                    "Connecting publication failed for uri: {}",
                    uri.to_string_lossy()
                )));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        Ok(publication)
    }

    fn setup_subscription(
        &self,
        uri: &str,
        stream_id: i32,
    ) -> Result<AeronSubscription, VexClientV2Error> {
        let uri = CString::new(uri)?;
        let available_logger = AeronAvailableImageLogger {};
        let available_handler = Handler::leak(available_logger);
        let unavailable_logger = AeronUnavailableImageLogger {};
        let unavailable_handler = Handler::leak(unavailable_logger);

        let subscription = self.aeron.add_subscription(
            &uri,
            stream_id,
            Some(&available_handler),
            Some(&unavailable_handler),
            CONNECT_TIMEOUT,
        )?;
        Ok(subscription)
    }


    /// Phase 1: Connect to the server's initial channel to get a dedicated channel.
    fn connect_to_all_clients_channel(
        &mut self,
        key: u32,
    ) -> Result<(u16, u16, i32), VexClientV2Error> {
        // Subscription with dynamic MDC to receive the server's response
        let sub_uri = CString::new(format!(
            "aeron:udp?control-mode=dynamic|control={}",
            self.server_addr
        ))?;


        let available_logger = AeronAvailableImageLogger {};
        let available_handler = Handler::leak(available_logger);
        let unavailable_logger = AeronUnavailableImageLogger {};
        let unavailable_handler = Handler::leak(unavailable_logger);

        let subscription = self
            .aeron
            .add_subscription(&sub_uri, ALL_CLIENTS_STREAM_ID, Some(&available_handler), Some(&unavailable_handler), CONNECT_TIMEOUT)?;

        // Publication to send our HELLO message
        let pub_uri = CString::new(format!("aeron:udp?endpoint={}", self.server_addr))?;
        let publication = self
            .aeron
            .add_publication(&pub_uri, ALL_CLIENTS_STREAM_ID, CONNECT_TIMEOUT)?;

        // Wait for publication to be connected
        let start = Instant::now();
        while !publication.is_connected() {
            if start.elapsed() > CONNECT_TIMEOUT {
                return Err(VexClientV2Error::Timeout(
                    "Connecting to all-clients publication".to_string(),
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        let session_id = publication.session_id();
        info!(
            "Connected to all-clients channel with session ID: {}",
            session_id
        );

        // Send HELLO message with our one-time pad
        let hello_msg = format!("HELLO {:X}", key);
        self.send_message(&publication, &hello_msg)?;

        // Wait for the server's CONNECT response
        let shared_response = Arc::new(Mutex::new(None));
        let fragment_handler = ConnectResponseHandler {
            response: shared_response.clone(),
            expected_session: session_id,
        };
        let assembler = AeronFragmentAssembler::new(Some(&Handler::leak(fragment_handler)))?;
        let handler = Handler::leak(assembler);

        let start = Instant::now();
        while start.elapsed() < CONNECT_TIMEOUT {
            subscription.poll(Some(&handler), 10)?;
            if shared_response.lock().unwrap().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        match shared_response.lock().unwrap().take() {
            Some(ServerResponse::Connect {
                port,
                control_port,
                encrypted_session,
            }) => {
                let decrypted_session = encrypted_session ^ key;
                Ok((port, control_port, decrypted_session as i32))
            }
            Some(ServerResponse::Error(e)) => Err(VexClientV2Error::ServerError(e)),
            _ => Err(VexClientV2Error::Timeout(
                "Waiting for server CONNECT response".to_string(),
            )),
        }
    }

    /// Phase 2: Run the main echo loop on the dedicated duologue channel.
    fn run_echo_loop(
        &mut self,
        port: u16,
        control_port: u16,
        session_id: i32,
    ) -> Result<(), VexClientV2Error> {
        let server_control_addr = format!("{}:{}", self.server_addr.ip(), control_port);
        let server_data_addr = format!("{}:{}", self.server_addr.ip(), port);

        // Subscription with MDC and explicit session ID
        let sub_uri = CString::new(format!(
            "aeron:udp?control-mode=dynamic|control={}|session-id={}",
            server_control_addr, session_id
        ))?;

        let subscription = self.setup_subscription(sub_uri.to_str().unwrap(), DUOLOGUE_STREAM_ID)?;

        // Publication with explicit session ID
        let pub_uri = format!(
            "aeron:udp?endpoint={}|session-id={}",
            server_data_addr, session_id
        );
        let publication = self.setup_publication(&pub_uri, DUOLOGUE_STREAM_ID)?;
        
        // Wait for connections
        let start = Instant::now();
        while !publication.is_connected() || !subscription.is_connected() {
            if start.elapsed() > CONNECT_TIMEOUT {
                return Err(VexClientV2Error::Timeout(
                    "Connecting to duologue channel".to_string(),
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        info!("Successfully connected to duologue channel.");

        // Simple fragment handler for echo messages
        let fragment_handler = EchoLoopHandler;
        let assembler = AeronFragmentAssembler::new(Some(&Handler::leak(fragment_handler)))?;
        let handler = Handler::leak(assembler);

        let mut counter = 0u64;
        loop {
            let message = format!("ECHO {}", counter);
            self.send_message(&publication, &message)?;
            counter += 1;

            subscription.poll(Some(&handler), 10)?;
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    /// Helper to send a message with retries.
    fn send_message(
        &mut self,
        publication: &AeronPublication,
        text: &str,
    ) -> Result<(), VexClientV2Error> {
        debug!("Sending message: {}", text);
        let value = text.as_bytes();
        if value.len() > self.buffer.len() {
            return Err(VexClientV2Error::SendError("Message too long".to_string()));
        }
        self.buffer[..value.len()].copy_from_slice(value);

        for _ in 0..5 {
            let result = publication
                .offer::<AeronReservedValueSupplierLogger>(&self.buffer[..value.len()], None);
            if result >= 0 {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        Err(VexClientV2Error::SendError(
            "Failed to send after 5 attempts".to_string(),
        ))
    }
}