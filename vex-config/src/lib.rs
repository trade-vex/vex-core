//! Configuration for VEX Core: environment defaults, one optional file, VEX__* overrides and validation.

pub mod environment;
pub mod error;
pub mod loader;
pub mod networking;
pub mod symbols;

#[cfg(feature = "balance-preload")]
pub mod balance_preload;

pub use environment::Environment;
pub use error::{ConfigError, Result};
pub use loader::ConfigLoader;
pub use networking::{CoreNetworkingConfig, GatewayAuthenticationKey, GatewayNetworkingConfig};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
pub use symbols::SymbolSpecificationConfig;

#[cfg(feature = "balance-preload")]
pub use balance_preload::BalancePreloadConfig;

/// Main configuration structure that combines all VEX Core configuration modules
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VexConfig {
    /// Current environment (dev, test, prod)
    pub environment: Environment,
    /// Core networking configuration
    pub core_networking: CoreNetworkingConfig,
    /// Symbol specifications configuration
    pub symbols: SymbolSpecificationConfig,
    /// Kafka broker address for event streaming
    pub kafka_broker: String,
    /// Schema Registry URL for the Kafka events' protobuf schemas: an `http://` or `https://` URL.
    /// An unreachable one does not fail startup: the first event stalls and, since no event is
    /// dropped, intake halts.
    pub schema_registry_url: String,
    /// Balance preload configuration (test/local only)
    #[cfg(feature = "balance-preload")]
    #[serde(default)]
    pub balance_preload: BalancePreloadConfig,
}

fn default_kafka_broker() -> String {
    "localhost:9092".to_string()
}

fn default_schema_registry_url() -> String {
    "http://localhost:8081".to_string()
}

impl VexConfig {
    /// Create a new configuration for the specified environment
    pub fn new(environment: Environment) -> Self {
        Self {
            core_networking: CoreNetworkingConfig::for_environment(&environment),
            symbols: SymbolSpecificationConfig::for_environment(&environment),
            kafka_broker: default_kafka_broker(),
            schema_registry_url: default_schema_registry_url(),
            environment,
            #[cfg(feature = "balance-preload")]
            balance_preload: BalancePreloadConfig::new(),
        }
    }

    /// Load configuration using an explicitly selected environment
    /// Reads the `VEX_ENV` environment variable
    pub fn load_auto() -> Result<Self> {
        ConfigLoader::new().load_auto()
    }

    /// Load configuration for a specific environment
    pub fn load_for_environment(environment: Environment) -> Result<Self> {
        ConfigLoader::new().load_for_environment(environment)
    }

    /// Load configuration from a specific file path
    pub fn load_from_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        ConfigLoader::new().load_from_file(path)
    }

    /// Validate the entire configuration
    pub fn validate(&self) -> Result<()> {
        self.core_networking.validate(&self.environment)?;
        if self.is_production()
            && (!self.core_networking.enable_authentication
                || self.core_networking.gateway_authentication_key.is_empty())
        {
            return Err(ConfigError::validation(
                "Production requires gateway authentication and a nonempty gateway authentication key",
            ));
        }
        self.symbols.validate()?;
        // The registry client does not check the URL either; a malformed one would fail every
        // event's registration, and that is retried forever rather than failing startup.
        if !["http://", "https://"]
            .iter()
            .any(|scheme| self.schema_registry_url.starts_with(scheme))
        {
            return Err(ConfigError::validation(format!(
                "schema_registry_url must be an http:// or https:// URL, got {:?}",
                self.schema_registry_url
            )));
        }
        if self.is_production() && self.symbols.is_empty() {
            return Err(ConfigError::ValidationError(
                "Symbol configuration cannot be empty in production".to_string(),
            ));
        }
        #[cfg(feature = "balance-preload")]
        if self.balance_preload.enabled {
            if self.is_production() {
                return Err(ConfigError::ValidationError(
                    "Balance preload cannot be enabled in production".to_string(),
                ));
            }
            self.balance_preload.validate(&self.symbols)?;
        }
        Ok(())
    }

    /// Get the environment this configuration is for
    pub fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Check if configuration is for production environment
    pub fn is_production(&self) -> bool {
        matches!(self.environment, Environment::Production)
    }

    /// Check if configuration is for development environment
    pub fn is_development(&self) -> bool {
        matches!(self.environment, Environment::Development)
    }

    /// Check if configuration is for test environment
    pub fn is_test(&self) -> bool {
        matches!(self.environment, Environment::Test)
    }
}

impl Display for VexConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_symbols_rejected_in_production_but_accepted_in_development() {
        let mut production = VexConfig::new(Environment::Production);
        // PR #176 added an earlier production gate (authentication + key). Satisfy it so this
        // test still exercises PR #151's symbols rule rather than tripping on the auth rule.
        production.core_networking.enable_authentication = true;
        production.core_networking.gateway_authentication_key = GatewayAuthenticationKey::from(
            "5f0c3a9e7d2b4816c4e1a7f39b0d2c6e8a1f4b7d3e9c0a2f6b8d1e4c7a3f9b05",
        );
        assert!(matches!(
            production.validate(),
            Err(ConfigError::ValidationError(message))
                if message == "Symbol configuration cannot be empty in production"
        ));

        let mut development = VexConfig::new(Environment::Development);
        development.symbols.symbols.clear();
        assert!(development.validate().is_ok());
    }

    #[test]
    fn test_valid_configuration_accepted() {
        assert!(VexConfig::new(Environment::Development).validate().is_ok());
    }

    #[cfg(feature = "balance-preload")]
    #[test]
    fn preload_above_asset_supply_is_rejected() {
        let mut config = VexConfig::new(Environment::Development);
        config.balance_preload.enabled = true;
        let eth_supply = config.symbols.assets[&3].atomic_supply() as u64;
        for (asset_id, amount, accepted) in
            [(3, eth_supply, true), (3, u64::MAX, false), (9, 1, false)]
        {
            config.balance_preload.users =
                [(1, vec![balance_preload::UserBalance { asset_id, amount }])].into();
            assert_eq!(
                config.validate().is_ok(),
                accepted,
                "asset {asset_id} amount {amount}"
            );
        }
    }
}
