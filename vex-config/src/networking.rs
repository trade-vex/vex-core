//! Networking configuration modules for VEX Core

use crate::{ConfigError, Environment, Result};
use common::{MAX_GATEWAYS, ORDERCOMMANDSIZE};
use serde::{Deserialize, Serialize};

/// Core networking configuration for VEX Core server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreNetworkingConfig {
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
    /// Reserved session id lower bound
    pub reserved_session_id_low: i32,
    /// Reserved session id upper bound
    pub reserved_session_id_high: i32,
    /// Enable authentication for gateways
    pub enable_authentication: bool,
    /// Core identifier
    pub core_id: String,
    /// Buffer size for network operations (bytes)
    pub buffer_size: usize,
    /// Connection retry attempts
    pub retry_attempts: u32,
    /// Connection retry delay in milliseconds
    pub retry_delay_ms: u64,
    /// Control Response Channel for Aeron Archive
    pub request_control_channel: String,
    pub response_control_channel: String,
    pub recording_events_channel: String,
    /// CPU core pinning for processor threads
    /// Disabled by default in development/test, enabled in production.
    #[serde(default = "default_enable_core_pinning")]
    pub enable_core_pinning: bool,
}

fn default_enable_core_pinning() -> bool {
    false
}

impl CoreNetworkingConfig {
    /// Create configuration optimized for the given environment
    pub fn for_environment(env: &Environment) -> Self {
        match env {
            Environment::Development => Self::development_defaults(),
            Environment::Test => Self::test_defaults(),
            Environment::Production => Self::production_defaults(),
        }
    }

    /// Development environment defaults - relaxed settings for local development
    pub fn development_defaults() -> Self {
        Self {
            context_dir: "/dev/shm/aeron-test-server".to_string(),
            local_address: "127.0.0.1".to_string(),
            initial_port: 40001,
            initial_control_port: 40002,
            base_gateway_port: 50000,
            max_gateways: 15,
            reserved_session_id_low: 1000,
            reserved_session_id_high: 9999,
            enable_authentication: false,
            core_id: "vex-core-dev".to_string(),
            buffer_size: 1024 * 1024,
            retry_attempts: 3,
            retry_delay_ms: 1000,
            request_control_channel: "aeron:udp?endpoint=localhost:8010".to_string(),
            response_control_channel: "aeron:udp?endpoint=localhost:0".to_string(),
            recording_events_channel: "aeron:udp?endpoint=localhost:0".to_string(),
            enable_core_pinning: false,
        }
    }

    /// Test environment defaults - moderate settings for automated testing
    pub fn test_defaults() -> Self {
        Self {
            context_dir: "/dev/shm/aeron-test-server".to_string(),
            local_address: "127.0.0.1".to_string(),
            initial_port: 40001,
            initial_control_port: 40002,
            base_gateway_port: 50000,
            max_gateways: 15,
            reserved_session_id_low: 1000,
            reserved_session_id_high: 9999,
            enable_authentication: true,
            core_id: "vex-core-test".to_string(),
            buffer_size: 1024 * 1024,
            retry_attempts: 3,
            retry_delay_ms: 1000,
            request_control_channel: "aeron:udp?endpoint=localhost:8010".to_string(),
            response_control_channel: "aeron:udp?endpoint=localhost:0".to_string(),
            recording_events_channel: "aeron:udp?endpoint=localhost:0".to_string(),
            enable_core_pinning: false,
        }
    }

    /// Production environment defaults - strict settings for production deployment
    pub fn production_defaults() -> Self {
        Self {
            context_dir: "/var/lib/vex/aeron-core".to_string(),
            local_address: "127.0.0.1".to_string(), // Bind to all interfaces
            initial_port: 3521,
            initial_control_port: 3522,
            base_gateway_port: 50000,
            max_gateways: 1000,
            reserved_session_id_low: 1000,
            reserved_session_id_high: 9999,
            enable_authentication: true,
            core_id: "vex-core-prod".to_string(),
            buffer_size: 4 * 1024 * 1024, // 4MB
            retry_attempts: 5,
            retry_delay_ms: 2000,
            request_control_channel: "aeron:udp?endpoint=localhost:8010".to_string(),
            response_control_channel: "aeron:udp?endpoint=localhost:0".to_string(),
            recording_events_channel: "aeron:udp?endpoint=localhost:8012".to_string(),
            enable_core_pinning: true,
        }
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.initial_port == 0 {
            return Err(ConfigError::network("Initial port cannot be 0"));
        }

        if self.initial_control_port == 0 {
            return Err(ConfigError::network("Initial control port cannot be 0"));
        }

        if self.initial_port == self.initial_control_port {
            return Err(ConfigError::network(
                "Initial port and control port cannot be the same",
            ));
        }

        if self.max_gateways == 0 {
            return Err(ConfigError::network("Max gateways must be greater than 0"));
        }

        if self.reserved_session_id_low >= self.reserved_session_id_high {
            return Err(ConfigError::network(
                "Reserved session ID low must be less than high",
            ));
        }

        if self.core_id.is_empty() {
            return Err(ConfigError::network("Core ID cannot be empty"));
        }

        if self.buffer_size == 0 {
            return Err(ConfigError::network("Buffer size must be greater than 0"));
        }

        if self.context_dir.is_empty() {
            return Err(ConfigError::network("Context directory cannot be empty"));
        }

        Ok(())
    }

    /// Merge with another configuration, with the other taking precedence
    pub fn merge_with(&mut self, other: &Self) -> Result<()> {
        // For networking config, we do a field-by-field merge
        // In a real implementation, you might want more sophisticated merging rules
        *self = other.clone();
        Ok(())
    }
}

/// Gateway networking configuration for connecting to VEX Core
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayNetworkingConfig {
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
    pub gateway_id: u8,
    /// Maximum message size in bytes, 64 for OrderCommand
    pub max_message_size: usize,
    /// Enable heartbeat mechanism
    pub enable_heartbeat: bool,
    /// Heartbeat interval in seconds
    pub heartbeat_interval_seconds: u64,
    /// Connection timeout in seconds
    pub connection_timeout_seconds: u64,
    /// Reconnection attempts
    pub reconnection_attempts: u32,
    /// Reconnection delay in milliseconds
    pub reconnection_delay_ms: u64,
    /// Buffer size for gateway operations
    pub buffer_size: usize,
}

impl GatewayNetworkingConfig {
    /// Create configuration optimized for the given environment
    pub fn for_environment(env: &Environment) -> Self {
        match env {
            Environment::Development => Self::development_defaults(),
            Environment::Test => Self::test_defaults(),
            Environment::Production => Self::production_defaults(),
        }
    }

    /// Development environment defaults
    pub fn development_defaults() -> Self {
        Self {
            context_dir: "/tmp/aeron-test-client".to_string(),
            local_address: "127.0.0.1".to_string(),
            core_address: "127.0.0.1".to_string(),
            core_port: 3521,
            core_control_port: 3522,
            gateway_id: 1,
            max_message_size: ORDERCOMMANDSIZE,
            enable_heartbeat: true,
            heartbeat_interval_seconds: 10,
            connection_timeout_seconds: 60,
            reconnection_attempts: 5,
            reconnection_delay_ms: 2000,
            buffer_size: 512 * 1024, // 512KB
        }
    }

    /// Test environment defaults
    pub fn test_defaults() -> Self {
        Self {
            context_dir: "/dev/shm/aeron-test-client".to_string(),
            local_address: "127.0.0.1".to_string(),
            core_address: "127.0.0.1".to_string(),
            core_port: 3521,
            core_control_port: 3522,
            gateway_id: 1,
            max_message_size: ORDERCOMMANDSIZE,
            enable_heartbeat: true,
            heartbeat_interval_seconds: 5,
            connection_timeout_seconds: 30,
            reconnection_attempts: 3,
            reconnection_delay_ms: 1000,
            buffer_size: 256 * 1024, // 256KB
        }
    }

    /// Production environment defaults
    pub fn production_defaults() -> Self {
        Self {
            context_dir: "/var/lib/vex/aeron-gateway".to_string(),
            local_address: "127.0.0.1".to_string(),
            core_address: "127.0.0.1".to_string(), // Example production IP
            core_port: 3521,
            core_control_port: 3522,
            gateway_id: 1,
            max_message_size: ORDERCOMMANDSIZE,
            enable_heartbeat: true,
            heartbeat_interval_seconds: 5,
            connection_timeout_seconds: 15,
            reconnection_attempts: 10,
            reconnection_delay_ms: 5000,
            buffer_size: 2 * 1024 * 1024, // 2MB
        }
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.core_port == 0 {
            return Err(ConfigError::network("Core port cannot be 0"));
        }

        if self.core_control_port == 0 {
            return Err(ConfigError::network("Core control port cannot be 0"));
        }

        if self.core_port == self.core_control_port {
            return Err(ConfigError::network(
                "Core port and control port cannot be the same",
            ));
        }

        if self.gateway_id > MAX_GATEWAYS as u8 {
            return Err(ConfigError::network(format!(
                "Gateway ID must be between 0 and {}",
                MAX_GATEWAYS
            )));
        }

        if self.max_message_size == 0 {
            return Err(ConfigError::network(
                "Max message size must be greater than 0",
            ));
        }

        if self.max_message_size > 1024 * 1024 * 10 {
            // 10MB limit
            return Err(ConfigError::network("Max message size cannot exceed 10MB"));
        }

        if self.connection_timeout_seconds == 0 {
            return Err(ConfigError::network(
                "Connection timeout must be greater than 0",
            ));
        }

        if self.heartbeat_interval_seconds == 0 && self.enable_heartbeat {
            return Err(ConfigError::network(
                "Heartbeat interval must be greater than 0 when heartbeat is enabled",
            ));
        }

        if self.context_dir.is_empty() {
            return Err(ConfigError::network("Context directory cannot be empty"));
        }

        if self.buffer_size == 0 {
            return Err(ConfigError::network("Buffer size must be greater than 0"));
        }

        Ok(())
    }

    /// Merge with another configuration, with the other taking precedence
    pub fn merge_with(&mut self, other: &Self) -> Result<()> {
        *self = other.clone();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_core_networking_validation() {
        let mut config = CoreNetworkingConfig::development_defaults();
        assert!(config.validate().is_ok());

        config.initial_port = 0;
        assert!(config.validate().is_err());

        config.initial_port = 8080;
        config.initial_control_port = 8080;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_gateway_networking_validation() {
        let mut config = GatewayNetworkingConfig::development_defaults();
        assert!(config.validate().is_ok());

        config.gateway_id = 20;
        assert!(config.validate().is_err());

        config.gateway_id = 16;
        config.max_message_size = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_environment_specific_configs() {
        let dev_config = CoreNetworkingConfig::for_environment(&Environment::Development);
        let test_config = CoreNetworkingConfig::for_environment(&Environment::Test);
        let prod_config = CoreNetworkingConfig::for_environment(&Environment::Production);

        assert!(!dev_config.enable_authentication);
        assert!(test_config.enable_authentication);
        assert!(prod_config.enable_authentication);

        assert_eq!(dev_config.initial_port, 3521);
        assert_eq!(test_config.initial_port, 3521);
        assert_eq!(prod_config.initial_port, 3521);
    }
}
