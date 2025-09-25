//! Error types for VEX configuration management

use thiserror::Error;

/// Result type alias for configuration operations
pub type Result<T> = std::result::Result<T, ConfigError>;

/// Configuration-related errors
#[derive(Error, Debug)]
pub enum ConfigError {
    /// IO error when reading/writing configuration files
    #[error("IO error: {0}")]
    IoError(String),

    /// Error parsing configuration files
    #[error("Parse error: {0}")]
    ParseError(String),

    /// Error serializing configuration
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Configuration validation error
    #[error("Validation error: {0}")]
    ValidationError(String),

    /// Environment variable error
    #[error("Environment variable error: {0}")]
    EnvironmentError(String),

    /// Configuration not found error
    #[error("Configuration not found: {0}")]
    NotFound(String),

    /// Configuration merge error
    #[error("Merge error: {0}")]
    MergeError(String),

    /// Network configuration error
    #[error("Network configuration error: {0}")]
    NetworkError(String),

    /// Logging configuration error
    #[error("Logging configuration error: {0}")]
    LoggingError(String),
}

impl ConfigError {
    /// Create a new validation error
    pub fn validation<S: Into<String>>(msg: S) -> Self {
        ConfigError::ValidationError(msg.into())
    }

    /// Create a new IO error
    pub fn io<S: Into<String>>(msg: S) -> Self {
        ConfigError::IoError(msg.into())
    }

    /// Create a new parse error
    pub fn parse<S: Into<String>>(msg: S) -> Self {
        ConfigError::ParseError(msg.into())
    }

    /// Create a new not found error
    pub fn not_found<S: Into<String>>(msg: S) -> Self {
        ConfigError::NotFound(msg.into())
    }

    /// Create a new network error
    pub fn network<S: Into<String>>(msg: S) -> Self {
        ConfigError::NetworkError(msg.into())
    }

    /// Create a new logging error
    pub fn logging<S: Into<String>>(msg: S) -> Self {
        ConfigError::LoggingError(msg.into())
    }
}

impl From<std::io::Error> for ConfigError {
    fn from(err: std::io::Error) -> Self {
        ConfigError::IoError(err.to_string())
    }
}

impl From<config::ConfigError> for ConfigError {
    fn from(err: config::ConfigError) -> Self {
        match err {
            config::ConfigError::NotFound(_) => ConfigError::NotFound(err.to_string()),
            config::ConfigError::Type { .. } => ConfigError::ParseError(err.to_string()),
            config::ConfigError::Message(msg) => ConfigError::ParseError(msg),
            _ => ConfigError::ParseError(err.to_string()),
        }
    }
}
