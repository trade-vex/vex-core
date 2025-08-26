//! Logging configuration for VEX Core

use crate::{ConfigError, Environment, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Logging level configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    /// Trace level - most verbose
    Trace,
    /// Debug level - detailed diagnostic information
    Debug,
    /// Info level - general information about program execution
    Info,
    /// Warn level - warnings about potentially harmful situations
    Warn,
    /// Error level - error events but application continues
    Error,
    /// Off - disable logging
    Off,
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Trace => write!(f, "trace"),
            LogLevel::Debug => write!(f, "debug"),
            LogLevel::Info => write!(f, "info"),
            LogLevel::Warn => write!(f, "warn"),
            LogLevel::Error => write!(f, "error"),
            LogLevel::Off => write!(f, "off"),
        }
    }
}

/// Log output format configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Human-readable format for development
    Pretty,
    /// JSON format for structured logging
    Json,
    /// Compact single-line format
    Compact,
    /// Full detailed format
    Full,
}

/// Log output destination
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum LogOutput {
    /// Output to stdout
    Stdout,
    /// Output to stderr
    Stderr,
    /// Output to a file
    File {
        /// Path to the log file
        path: String,
        /// Whether to rotate the log file
        rotate: bool,
        /// Maximum file size before rotation (in bytes)
        max_size: Option<u64>,
        /// Number of rotated files to keep
        max_files: Option<u32>,
    },
    /// Output to syslog
    Syslog {
        /// Syslog facility
        facility: String,
        /// Syslog identifier
        ident: String,
    },
}

/// Comprehensive logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Global log level
    pub level: LogLevel,
    /// Log format
    pub format: LogFormat,
    /// Log outputs
    pub outputs: Vec<LogOutput>,
    /// Module-specific log levels
    pub module_levels: HashMap<String, LogLevel>,
    /// Whether to include source location in logs
    pub include_location: bool,
    /// Whether to include timestamps
    pub include_timestamp: bool,
    /// Whether to include thread ID
    pub include_thread_id: bool,
    /// Whether to include span information
    pub include_spans: bool,
    /// Custom fields to include in structured logs
    pub custom_fields: HashMap<String, String>,
    /// Whether to enable async logging
    pub async_logging: bool,
    /// Buffer size for async logging
    pub async_buffer_size: usize,
    /// Log sampling rate (0.0 to 1.0, where 1.0 means log everything)
    pub sampling_rate: f64,
}

impl LoggingConfig {
    /// Create configuration optimized for the given environment
    pub fn for_environment(env: &Environment) -> Self {
        match env {
            Environment::Development => Self::development_defaults(),
            Environment::Test => Self::test_defaults(),
            Environment::Production => Self::production_defaults(),
        }
    }

    /// Development environment defaults - verbose logging to stdout
    pub fn development_defaults() -> Self {
        let mut module_levels = HashMap::new();
        module_levels.insert("vex_core".to_string(), LogLevel::Debug);
        module_levels.insert("networking".to_string(), LogLevel::Debug);
        module_levels.insert("orderbook".to_string(), LogLevel::Info);

        Self {
            level: LogLevel::Debug,
            format: LogFormat::Pretty,
            outputs: vec![LogOutput::Stdout],
            module_levels,
            include_location: true,
            include_timestamp: true,
            include_thread_id: true,
            include_spans: true,
            custom_fields: HashMap::new(),
            async_logging: false,
            async_buffer_size: 1024,
            sampling_rate: 1.0,
        }
    }

    /// Test environment defaults - structured logging with controlled output
    pub fn test_defaults() -> Self {
        let mut module_levels = HashMap::new();
        module_levels.insert("vex_core".to_string(), LogLevel::Info);
        module_levels.insert("networking".to_string(), LogLevel::Warn);
        module_levels.insert("test".to_string(), LogLevel::Debug);

        Self {
            level: LogLevel::Info,
            format: LogFormat::Json,
            outputs: vec![LogOutput::File {
                path: "/tmp/vex-test.log".to_string(),
                rotate: true,
                max_size: Some(10 * 1024 * 1024), // 10MB
                max_files: Some(5),
            }],
            module_levels,
            include_location: false,
            include_timestamp: true,
            include_thread_id: false,
            include_spans: false,
            custom_fields: {
                let mut fields = HashMap::new();
                fields.insert("environment".to_string(), "test".to_string());
                fields
            },
            async_logging: true,
            async_buffer_size: 512,
            sampling_rate: 1.0,
        }
    }

    /// Production environment defaults - optimized structured logging
    pub fn production_defaults() -> Self {
        let mut module_levels = HashMap::new();
        module_levels.insert("vex_core".to_string(), LogLevel::Info);
        module_levels.insert("networking".to_string(), LogLevel::Warn);
        module_levels.insert("security".to_string(), LogLevel::Debug);

        Self {
            level: LogLevel::Info,
            format: LogFormat::Json,
            outputs: vec![
                LogOutput::File {
                    path: "/var/log/vex/vex-core.log".to_string(),
                    rotate: true,
                    max_size: Some(100 * 1024 * 1024), // 100MB
                    max_files: Some(10),
                },
                LogOutput::Syslog {
                    facility: "daemon".to_string(),
                    ident: "vex-core".to_string(),
                },
            ],
            module_levels,
            include_location: false,
            include_timestamp: true,
            include_thread_id: false,
            include_spans: true,
            custom_fields: {
                let mut fields = HashMap::new();
                fields.insert("environment".to_string(), "production".to_string());
                fields.insert("service".to_string(), "vex-core".to_string());
                fields
            },
            async_logging: true,
            async_buffer_size: 4096,
            sampling_rate: 1.0,
        }
    }

    /// Validate the logging configuration
    pub fn validate(&self) -> Result<()> {
        if self.outputs.is_empty() {
            return Err(ConfigError::logging(
                "At least one log output must be configured",
            ));
        }

        for output in &self.outputs {
            self.validate_output(output)?;
        }

        if self.sampling_rate < 0.0 || self.sampling_rate > 1.0 {
            return Err(ConfigError::logging(
                "Sampling rate must be between 0.0 and 1.0",
            ));
        }

        if self.async_buffer_size == 0 {
            return Err(ConfigError::logging(
                "Async buffer size must be greater than 0",
            ));
        }

        // Validate module level overrides
        for (module, level) in &self.module_levels {
            if module.is_empty() {
                return Err(ConfigError::logging("Module name cannot be empty"));
            }
            if *level == LogLevel::Off && module == "vex_core" {
                return Err(ConfigError::logging(
                    "Cannot disable logging for core module",
                ));
            }
        }

        Ok(())
    }

    /// Validate a specific log output configuration
    fn validate_output(&self, output: &LogOutput) -> Result<()> {
        match output {
            LogOutput::File {
                path,
                max_size,
                max_files,
                ..
            } => {
                if path.is_empty() {
                    return Err(ConfigError::logging("Log file path cannot be empty"));
                }

                if let Some(size) = max_size {
                    if *size == 0 {
                        return Err(ConfigError::logging("Max file size must be greater than 0"));
                    }
                    if *size < 1024 {
                        return Err(ConfigError::logging("Max file size should be at least 1KB"));
                    }
                }

                if let Some(files) = max_files
                    && *files == 0
                {
                    return Err(ConfigError::logging("Max files must be greater than 0"));
                }
            }
            LogOutput::Syslog { facility, ident } => {
                if facility.is_empty() {
                    return Err(ConfigError::logging("Syslog facility cannot be empty"));
                }
                if ident.is_empty() {
                    return Err(ConfigError::logging("Syslog ident cannot be empty"));
                }
            }
            LogOutput::Stdout | LogOutput::Stderr => {
                // No specific validation needed for stdout/stderr
            }
        }
        Ok(())
    }

    /// Merge with another configuration, with the other taking precedence
    pub fn merge_with(&mut self, other: &Self) -> Result<()> {
        // Merge module levels
        for (module, level) in &other.module_levels {
            self.module_levels.insert(module.clone(), level.clone());
        }

        // Merge custom fields
        for (key, value) in &other.custom_fields {
            self.custom_fields.insert(key.clone(), value.clone());
        }

        // Replace other fields
        self.level = other.level.clone();
        self.format = other.format.clone();
        self.outputs = other.outputs.clone();
        self.include_location = other.include_location;
        self.include_timestamp = other.include_timestamp;
        self.include_thread_id = other.include_thread_id;
        self.include_spans = other.include_spans;
        self.async_logging = other.async_logging;
        self.async_buffer_size = other.async_buffer_size;
        self.sampling_rate = other.sampling_rate;

        Ok(())
    }

    /// Get the effective log level for a specific module
    pub fn get_module_level(&self, module: &str) -> &LogLevel {
        self.module_levels.get(module).unwrap_or(&self.level)
    }

    /// Set log level for a specific module
    pub fn set_module_level(&mut self, module: String, level: LogLevel) {
        self.module_levels.insert(module, level);
    }

    /// Add a custom field to structured logs
    pub fn add_custom_field(&mut self, key: String, value: String) {
        self.custom_fields.insert(key, value);
    }

    /// Check if logging is enabled for a given level and module
    pub fn is_enabled(&self, level: &LogLevel, module: Option<&str>) -> bool {
        let effective_level = match module {
            Some(m) => self.get_module_level(m),
            None => &self.level,
        };

        level >= effective_level && *effective_level != LogLevel::Off
    }
}

// Helper function to compare log levels
impl PartialOrd for LogLevel {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LogLevel {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_order = match self {
            LogLevel::Trace => 0,
            LogLevel::Debug => 1,
            LogLevel::Info => 2,
            LogLevel::Warn => 3,
            LogLevel::Error => 4,
            LogLevel::Off => 5,
        };

        let other_order = match other {
            LogLevel::Trace => 0,
            LogLevel::Debug => 1,
            LogLevel::Info => 2,
            LogLevel::Warn => 3,
            LogLevel::Error => 4,
            LogLevel::Off => 5,
        };

        self_order.cmp(&other_order)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_ordering() {
        assert!(LogLevel::Trace < LogLevel::Debug);
        assert!(LogLevel::Debug < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Warn);
        assert!(LogLevel::Warn < LogLevel::Error);
        assert!(LogLevel::Error < LogLevel::Off);
    }

    #[test]
    fn test_logging_config_validation() {
        let mut config = LoggingConfig::development_defaults();
        assert!(config.validate().is_ok());

        config.outputs.clear();
        assert!(config.validate().is_err());

        config.outputs.push(LogOutput::Stdout);
        config.sampling_rate = 1.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_module_level_resolution() {
        let mut config = LoggingConfig::development_defaults();
        config.set_module_level("test_module".to_string(), LogLevel::Error);

        assert_eq!(config.get_module_level("test_module"), &LogLevel::Error);
        assert_eq!(config.get_module_level("unknown_module"), &config.level);
    }

    #[test]
    fn test_is_enabled() {
        let mut config = LoggingConfig::production_defaults(); // Info level
        config.set_module_level("debug_module".to_string(), LogLevel::Debug);

        assert!(config.is_enabled(&LogLevel::Info, None));
        assert!(!config.is_enabled(&LogLevel::Debug, None));
        assert!(config.is_enabled(&LogLevel::Debug, Some("debug_module")));
        assert!(!config.is_enabled(&LogLevel::Trace, Some("debug_module")));
    }

    #[test]
    fn test_environment_specific_configs() {
        let dev_config = LoggingConfig::for_environment(&Environment::Development);
        let test_config = LoggingConfig::for_environment(&Environment::Test);
        let prod_config = LoggingConfig::for_environment(&Environment::Production);

        assert_eq!(dev_config.level, LogLevel::Debug);
        assert_eq!(test_config.level, LogLevel::Info);
        assert_eq!(prod_config.level, LogLevel::Info);

        assert_eq!(dev_config.format, LogFormat::Pretty);
        assert_eq!(test_config.format, LogFormat::Json);
        assert_eq!(prod_config.format, LogFormat::Json);
    }
}
