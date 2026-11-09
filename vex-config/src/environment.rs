//! Environment configuration and utilities

use crate::ConfigError;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Represents the deployment environment for VEX Core
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Environment {
    /// Development environment - relaxed security, verbose logging, local defaults
    #[default]
    Development,
    /// Test environment - moderate security, structured logging, test-friendly defaults
    Test,
    /// Production environment - strict security, optimized logging, production defaults
    Production,
}

impl Environment {
    /// The one environment variable that selects the environment.
    pub const VARIABLE: &'static str = "VEX_ENV";

    /// Read the environment selected by `VEX_ENV`: `None` when it is unset, and an error when it
    /// is set to anything other than `development`, `test` or `production`.
    pub fn detect_explicit() -> crate::Result<Option<Self>> {
        let Some(value) = std::env::var_os(Self::VARIABLE) else {
            return Ok(None);
        };
        let environment = value
            .to_string_lossy()
            .parse::<Environment>()
            .map_err(|error| {
                ConfigError::EnvironmentError(format!("{}: {error}", Self::VARIABLE))
            })?;
        tracing::info!(
            target: "config",
            action = "environment_detected",
            environment = %environment,
            source = Self::VARIABLE
        );
        Ok(Some(environment))
    }

    /// Check if this is a development environment
    pub fn is_development(&self) -> bool {
        matches!(self, Environment::Development)
    }

    /// Check if this is a test environment
    pub fn is_test(&self) -> bool {
        matches!(self, Environment::Test)
    }

    /// Check if this is a production environment
    pub fn is_production(&self) -> bool {
        matches!(self, Environment::Production)
    }

    /// Get the default config file name for this environment
    pub fn config_file_name(&self) -> String {
        match self {
            Environment::Development => "config.dev".to_string(),
            Environment::Test => "config.test".to_string(),
            Environment::Production => "config.prod".to_string(),
        }
    }

    /// Get default search paths for config files for this environment
    pub fn default_config_paths(&self) -> Vec<String> {
        let base_name = self.config_file_name();
        vec![
            format!("./{}.toml", base_name),
            format!("./{}.yaml", base_name),
            format!("./{}.yml", base_name),
            format!("./{}.json", base_name),
            format!("./config/{}.toml", base_name),
            format!("./config/{}.yaml", base_name),
            format!("./config/{}.yml", base_name),
            format!("./config/{}.json", base_name),
            format!("/etc/vex/{}.toml", base_name),
            format!("/etc/vex/{}.yaml", base_name),
            format!("/etc/vex/{}.yml", base_name),
            format!("/etc/vex/{}.json", base_name),
        ]
    }

    /// Get environment-specific configuration prefix for environment variables
    pub fn env_prefix(&self) -> String {
        match self {
            Environment::Development => "VEX_DEV".to_string(),
            Environment::Test => "VEX_TEST".to_string(),
            Environment::Production => "VEX_PROD".to_string(),
        }
    }
}

impl fmt::Display for Environment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Environment::Development => write!(f, "development"),
            Environment::Test => write!(f, "test"),
            Environment::Production => write!(f, "production"),
        }
    }
}

impl FromStr for Environment {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "development" => Ok(Environment::Development),
            "test" => Ok(Environment::Test),
            "production" => Ok(Environment::Production),
            _ => Err(format!(
                "Invalid environment: '{s}'. Valid options: development, test, production"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_environment_from_str() {
        assert_eq!(
            "development".parse::<Environment>().unwrap(),
            Environment::Development
        );
        assert_eq!("test".parse::<Environment>().unwrap(), Environment::Test);
        assert_eq!(
            "production".parse::<Environment>().unwrap(),
            Environment::Production
        );

        for alias in [
            "dev",
            "develop",
            "testing",
            "prod",
            "Production",
            "invalid",
            "",
        ] {
            assert!(alias.parse::<Environment>().is_err(), "{alias:?}");
        }
    }

    #[test]
    fn test_environment_display() {
        assert_eq!(Environment::Development.to_string(), "development");
        assert_eq!(Environment::Test.to_string(), "test");
        assert_eq!(Environment::Production.to_string(), "production");
    }

    #[test]
    fn test_environment_checks() {
        assert!(Environment::Development.is_development());
        assert!(!Environment::Development.is_test());
        assert!(!Environment::Development.is_production());

        assert!(!Environment::Test.is_development());
        assert!(Environment::Test.is_test());
        assert!(!Environment::Test.is_production());

        assert!(!Environment::Production.is_development());
        assert!(!Environment::Production.is_test());
        assert!(Environment::Production.is_production());
    }

    #[test]
    fn test_config_file_names() {
        assert_eq!(Environment::Development.config_file_name(), "config.dev");
        assert_eq!(Environment::Test.config_file_name(), "config.test");
        assert_eq!(Environment::Production.config_file_name(), "config.prod");
    }
}
