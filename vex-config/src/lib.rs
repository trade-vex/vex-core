//! VEX Configuration Management
//!
//! This crate provides comprehensive configuration management for VEX Core,
//! supporting multiple environments (dev, test, prod) with advanced configuration operations.
//! Features auto-detection of environment from environment variables and multiple loading strategies.

pub mod environment;
pub mod error;
pub mod loader;
pub mod logging;
pub mod networking;
pub mod symbols;

pub use environment::Environment;
pub use error::{ConfigError, Result};
pub use loader::ConfigLoader;
pub use logging::LoggingConfig;
pub use networking::{CoreNetworkingConfig, GatewayNetworkingConfig};
use serde::{Deserialize, Serialize};
pub use symbols::SymbolSpecificationConfig;
use std::fmt::{Display, Formatter};

/// Main configuration structure that combines all VEX Core configuration modules
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VexConfig {
    /// Current environment (dev, test, prod)
    pub environment: Environment,
    /// Core networking configuration
    pub core_networking: CoreNetworkingConfig,
    /// Gateway networking configuration
    pub gateway_networking: GatewayNetworkingConfig,
    /// Logging configuration
    pub logging: LoggingConfig,
    /// Symbol specifications configuration
    pub symbols: SymbolSpecificationConfig,
    /// Kafka broker address for event streaming
    pub kafka_broker: String,
}

fn default_kafka_broker() -> String {
    "localhost:9092".to_string()
}

impl VexConfig {
    /// Create a new configuration for the specified environment
    pub fn new(environment: Environment) -> Self {
        Self {
            core_networking: CoreNetworkingConfig::for_environment(&environment),
            gateway_networking: GatewayNetworkingConfig::for_environment(&environment),
            logging: LoggingConfig::for_environment(&environment),
            symbols: SymbolSpecificationConfig::for_environment(&environment),
            kafka_broker: default_kafka_broker(),
            environment,
        }
    }

    /// Load configuration using auto-detection of environment from ENV variables
    /// Looks for VEX_ENV, ENVIRONMENT, or ENV environment variables
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

    /// Load configuration with custom search paths and environment
    pub fn load_with_options(
        environment: Option<Environment>,
        search_paths: Vec<String>,
    ) -> Result<Self> {
        ConfigLoader::new()
            .with_search_paths(search_paths)
            .load_with_environment(environment)
    }

    /// Save configuration to a file (format determined by extension)
    pub fn save_to_file<P: AsRef<std::path::Path>>(&self, path: P) -> Result<()> {
        self.validate()?;

        let path = path.as_ref();
        let content = match path.extension().and_then(|ext| ext.to_str()) {
            Some("toml") => toml::to_string_pretty(self)
                .map_err(|e| ConfigError::SerializationError(e.to_string()))?,
            Some("json") => serde_json::to_string_pretty(self)
                .map_err(|e| ConfigError::SerializationError(e.to_string()))?,
            Some("yaml") | Some("yml") => serde_yaml::to_string(self)
                .map_err(|e| ConfigError::SerializationError(e.to_string()))?,
            _ => {
                return Err(ConfigError::ValidationError(
                    "Unsupported file format. Use .toml, .json, .yaml, or .yml".to_string(),
                ));
            }
        };

        std::fs::write(path, content).map_err(|e| ConfigError::IoError(e.to_string()))?;

        Ok(())
    }

    /// Validate the entire configuration
    pub fn validate(&self) -> Result<()> {
        self.core_networking.validate()?;
        self.gateway_networking.validate()?;
        self.logging.validate()?;
        self.symbols.validate()?;
        Ok(())
    }

    /// Merge configuration with another config, with the other config taking precedence
    pub fn merge_with(&mut self, other: &Self) -> Result<()> {
        if self.environment != other.environment {
            return Err(ConfigError::ValidationError(
                "Cannot merge configurations with different environments".to_string(),
            ));
        }

        self.core_networking.merge_with(&other.core_networking)?;
        self.gateway_networking
            .merge_with(&other.gateway_networking)?;
        self.logging.merge_with(&other.logging)?;
        self.symbols.merge_with(&other.symbols)?;

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

impl Default for VexConfig {
    fn default() -> Self {
        Self::new(Environment::Development)
    }
}

impl Display for VexConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}
