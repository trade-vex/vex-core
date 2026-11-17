//! Networking configuration modules for VEX Core

use crate::{ConfigError, Environment, Result};
use common::{MAX_GATEWAYS, ORDERCOMMANDSIZE};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Shared secret that authenticates gateway handshakes.
#[derive(Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GatewayAuthenticationKey(String);

impl GatewayAuthenticationKey {
    pub const MIN_LEN: usize = 32;

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Usable as a HELLO credential: long enough to be unguessable, one whitespace-free token.
    pub fn check(&self) -> std::result::Result<(), String> {
        if self.0.len() < Self::MIN_LEN {
            return Err(format!(
                "gateway_authentication_key must be at least {} characters; generate it with `openssl rand -hex 32`",
                Self::MIN_LEN
            ));
        }
        if self.0.contains(char::is_whitespace) {
            return Err("gateway_authentication_key must not contain whitespace".to_string());
        }
        Ok(())
    }
}

impl From<&str> for GatewayAuthenticationKey {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl fmt::Debug for GatewayAuthenticationKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(if self.is_empty() {
            "<empty>"
        } else {
            "<redacted>"
        })
    }
}

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
    /// The first of the 64 UDP gateway ports, `base_gateway_port..base_gateway_port + 63`. They
    /// must lie outside the host's ephemeral port range (Linux `ip_local_port_range`, 32768-60999
    /// by default), which the core checks at startup.
    pub base_gateway_port: u16,
    /// Reserved session id lower bound
    pub reserved_session_id_low: i32,
    /// Reserved session id upper bound
    pub reserved_session_id_high: i32,
    /// Enable authentication for gateways
    pub enable_authentication: bool,
    /// Shared bearer credential every gateway presents in its HELLO: an opaque secret of at least
    /// 32 characters (`openssl rand -hex 32`), or empty, which outside production runs the
    /// handshake unauthenticated. Carried in cleartext over `aeron:udp`: trusted network segments
    /// only.
    #[serde(default)]
    pub gateway_authentication_key: GatewayAuthenticationKey,
    /// Core identifier
    pub core_id: String,
    /// Control Response Channel for Aeron Archive
    pub request_control_channel: String,
    pub response_control_channel: String,
    pub recording_events_channel: String,
    /// Where the engine's state snapshots go, which bound a start's replay. Required with
    /// journaling; a persistent disk in production (`/dev/shm` is for development and tests only).
    pub snapshot_dir: String,
    /// CPU core pinning for processor threads
    pub enable_core_pinning: bool,
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
            base_gateway_port: 20000,
            reserved_session_id_low: 1,
            reserved_session_id_high: i32::MAX,
            enable_authentication: false,
            gateway_authentication_key: GatewayAuthenticationKey::default(),
            core_id: "vex-core-dev".to_string(),
            request_control_channel: "aeron:udp?endpoint=localhost:8010".to_string(),
            response_control_channel: "aeron:udp?endpoint=localhost:0".to_string(),
            recording_events_channel: "aeron:udp?endpoint=localhost:0".to_string(),
            snapshot_dir: "/dev/shm/vex-snapshots".to_string(),
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
            base_gateway_port: 20000,
            reserved_session_id_low: 1,
            reserved_session_id_high: i32::MAX,
            enable_authentication: true,
            gateway_authentication_key: GatewayAuthenticationKey::default(),
            core_id: "vex-core-test".to_string(),
            request_control_channel: "aeron:udp?endpoint=localhost:8010".to_string(),
            response_control_channel: "aeron:udp?endpoint=localhost:0".to_string(),
            recording_events_channel: "aeron:udp?endpoint=localhost:0".to_string(),
            snapshot_dir: "/dev/shm/vex-snapshots".to_string(),
            enable_core_pinning: false,
        }
    }

    /// Production environment defaults - strict settings for production deployment
    pub fn production_defaults() -> Self {
        Self {
            context_dir: "/var/lib/vex/aeron-core".to_string(),
            local_address: "127.0.0.1".to_string(), // Bind to localhost only
            initial_port: 3521,
            initial_control_port: 3522,
            base_gateway_port: 20000,
            reserved_session_id_low: 1,
            reserved_session_id_high: i32::MAX,
            enable_authentication: true,
            gateway_authentication_key: GatewayAuthenticationKey::default(),
            core_id: "vex-core-prod".to_string(),
            request_control_channel: "aeron:udp?endpoint=localhost:8010".to_string(),
            response_control_channel: "aeron:udp?endpoint=localhost:0".to_string(),
            recording_events_channel: "aeron:udp?endpoint=localhost:8012".to_string(),
            snapshot_dir: "/var/lib/vex/snapshots".to_string(),
            enable_core_pinning: true,
        }
    }

    /// Validate the configuration
    pub fn validate(&self, environment: &Environment) -> Result<()> {
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

        if self.reserved_session_id_low >= self.reserved_session_id_high {
            return Err(ConfigError::network(
                "Reserved session ID low must be less than high",
            ));
        }

        if self.core_id.is_empty() {
            return Err(ConfigError::network("Core ID cannot be empty"));
        }

        // An empty key is resolved at startup: production refuses it, dev/test run unauthenticated.
        if self.enable_authentication && !self.gateway_authentication_key.is_empty() {
            self.gateway_authentication_key
                .check()
                .map_err(ConfigError::network)?;
        }

        if self.context_dir.is_empty() {
            return Err(ConfigError::network("Context directory cannot be empty"));
        }

        if environment.is_production() && self.request_control_channel.is_empty() {
            return Err(ConfigError::network(
                "Archive request control channel cannot be empty in production",
            ));
        }

        if !self.request_control_channel.is_empty() && self.snapshot_dir.is_empty() {
            return Err(ConfigError::network(
                "snapshot_dir is required when journaling is configured",
            ));
        }

        Ok(())
    }
}

/// Gateway networking configuration for connecting to VEX Core
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayNetworkingConfig {
    /// The directory used for the underlying Aeron media driver
    pub context_dir: String,
    /// VEX Core address to connect to
    pub core_address: String,
    /// VEX Core port for initial handshake
    pub core_port: u16,
    /// VEX Core control port for receiving messages
    pub core_control_port: u16,
    /// Gateway identifier for this instance
    pub gateway_id: u8,
    /// Shared bearer credential used to authenticate with VEX Core: an opaque secret of at least
    /// 32 characters (`openssl rand -hex 32`) equal to the core's key, or empty when the core runs
    /// without authentication. `VexGateway::new` rejects any other value.
    ///
    /// This credential is carried in cleartext over `aeron:udp` and is meaningful
    /// only on a trusted or private network segment.
    #[serde(default)]
    pub gateway_authentication_key: GatewayAuthenticationKey,
    /// Maximum message size in bytes, 64 for OrderCommand
    pub max_message_size: usize,
}

impl GatewayNetworkingConfig {
    /// Development environment defaults
    pub fn development_defaults() -> Self {
        Self {
            context_dir: "/tmp/aeron-test-client".to_string(),
            core_address: "127.0.0.1".to_string(),
            core_port: 3521,
            core_control_port: 3522,
            gateway_id: 1,
            gateway_authentication_key: GatewayAuthenticationKey::default(),
            max_message_size: ORDERCOMMANDSIZE,
        }
    }

    /// Test environment defaults
    pub fn test_defaults() -> Self {
        Self {
            context_dir: "/dev/shm/aeron-test-client".to_string(),
            core_address: "127.0.0.1".to_string(),
            core_port: 3521,
            core_control_port: 3522,
            gateway_id: 1,
            gateway_authentication_key: GatewayAuthenticationKey::default(),
            max_message_size: ORDERCOMMANDSIZE,
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

        if self.gateway_id >= MAX_GATEWAYS as u8 {
            return Err(ConfigError::network(format!(
                "Gateway ID must be between 0 and {} (exclusive)",
                MAX_GATEWAYS
            )));
        }

        if !self.gateway_authentication_key.is_empty() {
            self.gateway_authentication_key
                .check()
                .map_err(ConfigError::network)?;
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

        if self.context_dir.is_empty() {
            return Err(ConfigError::network("Context directory cannot be empty"));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_core_networking_validation() {
        let mut config = CoreNetworkingConfig::development_defaults();
        assert!(config.validate(&Environment::Development).is_ok());

        config.initial_port = 0;
        assert!(config.validate(&Environment::Development).is_err());

        config.initial_port = 8080;
        config.initial_control_port = 8080;
        assert!(config.validate(&Environment::Development).is_err());
    }

    #[test]
    fn empty_archive_channel_is_only_valid_outside_production() {
        let mut config = CoreNetworkingConfig::development_defaults();
        config.request_control_channel.clear();

        assert!(config.validate(&Environment::Development).is_ok());
        assert!(config.validate(&Environment::Test).is_ok());
        assert!(config.validate(&Environment::Production).is_err());
    }

    #[test]
    fn snapshot_dir_is_required_with_journaling() {
        let mut config = CoreNetworkingConfig::development_defaults();
        config.snapshot_dir.clear();
        assert!(matches!(
            config.validate(&Environment::Development),
            Err(ConfigError::NetworkError(message)) if message.contains("snapshot_dir is required")
        ));

        config.request_control_channel.clear();
        assert!(config.validate(&Environment::Development).is_ok());
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
    fn key_shorter_than_32_chars_fails_validation() {
        let hex64 = "0123456789abcdef".repeat(4);
        assert!(GatewayAuthenticationKey::from("short").check().is_err());
        assert!(
            GatewayAuthenticationKey::from(hex64.as_str())
                .check()
                .is_ok()
        );
        let spaced = format!("{} {}", &hex64[..32], &hex64[32..]);
        assert!(
            GatewayAuthenticationKey::from(spaced.as_str())
                .check()
                .is_err()
        );

        let mut config = GatewayNetworkingConfig::development_defaults();
        config.gateway_authentication_key = "123456789".into();
        assert!(config.validate().is_err());
    }

    #[test]
    fn core_key_is_checked_when_authentication_is_enabled() {
        let mut config = CoreNetworkingConfig::development_defaults();
        config.gateway_authentication_key = "123456789".into();
        assert!(config.validate(&Environment::Development).is_ok());

        config.enable_authentication = true;
        assert!(matches!(
            config.validate(&Environment::Development),
            Err(ConfigError::NetworkError(message)) if message.contains("at least 32 characters")
        ));

        config.gateway_authentication_key = "0123456789abcdef".repeat(4).as_str().into();
        assert!(config.validate(&Environment::Development).is_ok());
    }

    #[test]
    fn enable_core_pinning_is_required() {
        let mut json = serde_json::to_value(CoreNetworkingConfig::production_defaults()).unwrap();
        json.as_object_mut().unwrap().remove("enable_core_pinning");

        let error = serde_json::from_value::<CoreNetworkingConfig>(json).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("missing field `enable_core_pinning`"),
            "{error}"
        );
    }

    #[test]
    fn session_ids_span_the_positive_i32_range() {
        for env in [
            Environment::Development,
            Environment::Test,
            Environment::Production,
        ] {
            let config = CoreNetworkingConfig::for_environment(&env);
            assert!(config.reserved_session_id_low > 0);
            assert!(config.reserved_session_id_high - config.reserved_session_id_low >= 1 << 30);
        }
    }

    #[test]
    fn test_environment_specific_configs() {
        let dev_config = CoreNetworkingConfig::for_environment(&Environment::Development);
        let test_config = CoreNetworkingConfig::for_environment(&Environment::Test);
        let prod_config = CoreNetworkingConfig::for_environment(&Environment::Production);

        assert!(!dev_config.enable_authentication);
        assert!(test_config.enable_authentication);
        assert!(prod_config.enable_authentication);

        assert_eq!(dev_config.initial_port, 40001);
        assert_eq!(test_config.initial_port, 40001);
        assert_eq!(prod_config.initial_port, 3521);
    }
}
