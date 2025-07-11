/// Server Configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// The directory used for the underlying media driver
    pub context_dir: String,
    /// The local address to bind to
    pub local_address: String,
    /// The initial port to use for client introduction
    pub initial_port: u16,
    /// The initial control port to use for client introduction
    pub initial_control_port: u16,
    /// The base port to use for individual client connections
    pub base_client_port: u16,
    /// The maximum number of clients to support
    pub max_clients: u16,
    /// The maximum number of connections per address
    pub max_connections_per_address: u16,
    /// Reserved session id lower bound
    pub reserved_session_id_low: i32,
    /// Reserved session id upper bound
    pub reserved_session_id_high: i32,
}