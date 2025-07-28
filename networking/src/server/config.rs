/// VEX Core server configuration
#[derive(Debug, Clone)]
pub struct CoreConfig {
    /// The directory used for the underlying Aeron media driver
    pub context_dir: String,
    /// The local address to bind to
    pub local_address: String,
    /// The initial port to use for gateway introduction
    pub initial_port: u16,
    /// The initial control port to use for gateway introduction
    pub initial_control_port: u16,
    /// The base port to use for individual gateway connections
    pub base_gateway_port: u16,
    /// The maximum number of gateways to support
    pub max_gateways: u16,
    /// The maximum number of connections per address
    pub max_connections_per_address: u16,
    /// Reserved session id lower bound
    pub reserved_session_id_low: i32,
    /// Reserved session id upper bound
    pub reserved_session_id_high: i32,
    /// Enable authentication for gateways
    pub enable_authentication: bool,
    /// Enable heartbeat monitoring
    pub enable_heartbeat: bool,
    /// Gateway timeout in seconds
    pub gateway_timeout_seconds: u64,
    /// Core identifier
    pub core_id: String,
}

impl Default for CoreConfig {
    fn default() -> Self {
        Self {
            context_dir: "/tmp/aeron-core".to_string(),
            local_address: "127.0.0.1".to_string(),
            initial_port: 40001,
            initial_control_port: 40002,
            base_gateway_port: 50000,
            max_gateways: 100,
            max_connections_per_address: 10,
            reserved_session_id_low: 1000,
            reserved_session_id_high: 9999,
            enable_authentication: true,
            enable_heartbeat: true,
            gateway_timeout_seconds: 30,
            core_id: "vex-core-1".to_string(),
        }
    }
}
