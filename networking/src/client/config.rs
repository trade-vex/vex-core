/// Gateway configuration for connecting to VEX Core
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// The directory used for the underlying Aeron media driver
    pub context_dir: String,
    /// The local IP address for this gateway
    pub local_address: String,
    /// VEX Core address to connect to
    pub core_address: String,
    /// VEX Core port for initial handshake
    pub core_port: u16,
    /// VEX Core control port for receiving messages
    pub core_control_port: u16,
    /// Gateway identifier for this instance
    pub gateway_id: String,
    /// Maximum message size in bytes
    pub max_message_size: usize,
    /// Enable heartbeat mechanism
    pub enable_heartbeat: bool,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            context_dir: "/tmp/aeron".to_string(),
            local_address: "127.0.0.1".to_string(),
            core_address: "127.0.0.1".to_string(),
            core_port: 40001,
            core_control_port: 40002,
            gateway_id: "gateway-1".to_string(),
            max_message_size: 2048,
            enable_heartbeat: true,
        }
    }
}
