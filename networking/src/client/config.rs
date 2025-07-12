/// Server Configuration
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// The directory used for the underlying media driver
    pub context_dir: String,
    /// The local ip address
    pub local_address: String,
    /// The local port, where we will listen to
    // pub local_port: u16,
    /// server address
    pub server_address: String,
    /// server port to publish message to
    pub server_port: u16,
    /// server control port from which server's publisher will publish
    pub server_control_port: u16,
}