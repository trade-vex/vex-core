//! Configuration loader with advanced loading strategies

use crate::{ConfigError, Environment, Result, VexConfig};
use config::{Config, File, FileFormat};
use std::path::Path;

/// Advanced configuration loader with multiple loading strategies
pub struct ConfigLoader {
    search_paths: Vec<String>,
    env_prefix: Option<String>,
    allow_missing: bool,
}

impl ConfigLoader {
    /// Create a new configuration loader with default settings
    pub fn new() -> Self {
        Self {
            search_paths: Vec::new(),
            env_prefix: Some("VEX".to_string()),
            allow_missing: false,
        }
    }

    /// Set custom search paths for configuration files
    pub fn with_search_paths(mut self, paths: Vec<String>) -> Self {
        self.search_paths = paths;
        self
    }

    /// Set environment variable prefix (default: "VEX")
    pub fn with_env_prefix<S: Into<String>>(mut self, prefix: S) -> Self {
        self.env_prefix = Some(prefix.into());
        self
    }

    /// Disable environment variable prefix
    pub fn without_env_prefix(mut self) -> Self {
        self.env_prefix = None;
        self
    }

    /// Allow missing configuration files (use defaults)
    pub fn allow_missing_files(mut self) -> Self {
        self.allow_missing = true;
        self
    }

    /// Load configuration using an explicitly selected environment
    pub fn load_auto(self) -> Result<VexConfig> {
        self.load_auto_for_environment(Environment::detect_explicit())
    }

    fn load_auto_for_environment(
        self,
        explicit_environment: Option<Environment>,
    ) -> Result<VexConfig> {
        let environment = explicit_environment.ok_or_else(|| {
            ConfigError::EnvironmentError(
                "No environment selected. Set one of VEX_ENV, ENVIRONMENT, ENV, or NODE_ENV to development, test, or production"
                    .to_string(),
            )
        })?;

        match self.load_for_environment(environment.clone()) {
            Ok(config) => Ok(config),
            Err(error) if environment.is_development() || environment.is_test() => {
                tracing::warn!(
                    target: "config",
                    action = "config_load_failed",
                    environment = %environment,
                    error = %error
                );
                tracing::info!(
                    target: "config",
                    action = "using_environment_defaults",
                    environment = %environment
                );
                Ok(VexConfig::new(environment))
            }
            Err(error) => Err(error),
        }
    }

    /// Load configuration for a specific environment
    pub fn load_for_environment(self, environment: Environment) -> Result<VexConfig> {
        self.load_with_environment(Some(environment))
    }

    /// Load configuration from a specific file
    pub fn load_from_file<P: AsRef<Path>>(self, path: P) -> Result<VexConfig> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(ConfigError::not_found(format!(
                "Config file not found: {}",
                path.display()
            )));
        }

        let mut builder = Config::builder();

        // Add the specific file
        let format = self.detect_file_format(path)?;
        builder = builder.add_source(File::from(path).format(format));

        // Add environment variables if prefix is set
        if let Some(prefix) = &self.env_prefix {
            builder = builder.add_source(
                config::Environment::with_prefix(prefix)
                    .try_parsing(true)
                    .separator("__"),
            );
        }

        let config = builder.build()?;
        let vex_config: VexConfig = config.try_deserialize()?;

        vex_config.validate()?;
        Ok(vex_config)
    }

    /// Load configuration with optional environment and custom settings
    pub fn load_with_environment(self, environment: Option<Environment>) -> Result<VexConfig> {
        let env = environment.or_else(Environment::detect_explicit).ok_or_else(|| {
            ConfigError::EnvironmentError(
                "No environment selected. Set one of VEX_ENV, ENVIRONMENT, ENV, or NODE_ENV to development, test, or production"
                    .to_string(),
            )
        })?;

        let mut builder = Config::builder();

        // Get search paths
        let search_paths = if self.search_paths.is_empty() {
            env.default_config_paths()
        } else {
            self.search_paths.clone()
        };

        // Add configuration files in order of precedence
        let mut files_found = false;
        for path in &search_paths {
            let config_path = Path::new(path);
            if config_path.exists() {
                files_found = true;
                let format = self
                    .detect_file_format(config_path)
                    .map_err(|error| ConfigError::load(search_paths.clone(), error))?;
                builder = builder.add_source(File::from(config_path).format(format));
                tracing::debug!(
                    target: "config",
                    action = "config_file_loaded",
                    path = %path
                );
            }
        }

        // Check if we found any files
        if !files_found && !self.allow_missing {
            return Err(ConfigError::not_found(format!(
                "No configuration files found in search paths: {search_paths:?}"
            )));
        }

        // If no files found but missing files are allowed, return default config
        if !files_found && self.allow_missing {
            let mut default_config = VexConfig::new(env.clone());

            // Still apply environment variable overrides if configured
            if let Some(prefix) = &self.env_prefix {
                default_config = self.apply_env_vars_to_config(default_config, prefix, &env)?;
            }

            default_config.validate()?;
            return Ok(default_config);
        }

        // Add environment-specific variables
        if let Some(prefix) = &self.env_prefix {
            let env_prefix = format!("{}_{}", prefix, env.env_prefix());
            builder = builder.add_source(
                config::Environment::with_prefix(&env_prefix)
                    .try_parsing(true)
                    .separator("__"),
            );

            // Also add general VEX prefix
            builder = builder.add_source(
                config::Environment::with_prefix(prefix)
                    .try_parsing(true)
                    .separator("__"),
            );
        }

        let config = builder
            .build()
            .map_err(|error| ConfigError::load(search_paths.clone(), error.into()))?;
        let mut vex_config: VexConfig = config
            .try_deserialize()
            .map_err(|error| ConfigError::load(search_paths.clone(), error.into()))?;

        // Ensure environment matches what we expect
        vex_config.environment = env;

        vex_config
            .validate()
            .map_err(|error| ConfigError::load(search_paths, error))?;
        Ok(vex_config)
    }

    /// Apply environment variables to a config (simplified implementation)
    fn apply_env_vars_to_config(
        &self,
        mut config: VexConfig,
        prefix: &str,
        env: &Environment,
    ) -> Result<VexConfig> {
        // Create environment variable sources with both general and environment-specific prefixes
        let general_source = config::Environment::with_prefix(prefix)
            .try_parsing(true)
            .separator("__");

        let env_specific_prefix = format!("{}_{}", prefix, env.env_prefix());
        let env_specific_source = config::Environment::with_prefix(&env_specific_prefix)
            .try_parsing(true)
            .separator("__");

        // Build a config with just environment variables
        let env_config = Config::builder()
            .add_source(general_source)
            .add_source(env_specific_source)
            .build()?;

        // If we have any environment variables, deserialize them and merge with our config
        // Deserialize env vars into VexConfig (will only set fields that are specified)
        if let Ok(env_overrides) = env_config.try_deserialize::<VexConfig>() {
            config.merge_with(&env_overrides)?;
        }

        Ok(config)
    }

    /// Detect file format from extension
    fn detect_file_format(&self, path: &Path) -> Result<FileFormat> {
        match path.extension().and_then(|ext| ext.to_str()) {
            Some("toml") => Ok(FileFormat::Toml),
            Some("yaml") | Some("yml") => Ok(FileFormat::Yaml),
            Some("json") => Ok(FileFormat::Json),
            Some("ini") => Ok(FileFormat::Ini),
            Some(ext) => Err(ConfigError::parse(format!(
                "Unsupported file format: {ext}"
            ))),
            None => Err(ConfigError::parse("No file extension found")),
        }
    }
}

impl Default for ConfigLoader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_file_format() {
        let loader = ConfigLoader::new();

        assert!(matches!(
            loader.detect_file_format(Path::new("config.toml")).unwrap(),
            FileFormat::Toml
        ));
        assert!(matches!(
            loader.detect_file_format(Path::new("config.yaml")).unwrap(),
            FileFormat::Yaml
        ));
        assert!(matches!(
            loader.detect_file_format(Path::new("config.yml")).unwrap(),
            FileFormat::Yaml
        ));
        assert!(matches!(
            loader.detect_file_format(Path::new("config.json")).unwrap(),
            FileFormat::Json
        ));

        assert!(loader.detect_file_format(Path::new("config.txt")).is_err());
        assert!(loader.detect_file_format(Path::new("config")).is_err());
    }

    #[test]
    fn test_load_from_missing_file() {
        let loader = ConfigLoader::new();
        let result = loader.load_from_file("nonexistent.toml");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ConfigError::NotFound(_)));
    }

    #[test]
    fn test_explicit_test_environment_allows_missing_file_defaults() {
        let config = ConfigLoader::new()
            .with_search_paths(vec!["/missing/config.test.yaml".to_string()])
            .load_auto_for_environment(Some(Environment::Test))
            .unwrap();

        assert_eq!(config.environment, Environment::Test);
    }

    #[test]
    fn test_explicit_development_environment_allows_missing_file_defaults() {
        let config = ConfigLoader::new()
            .with_search_paths(vec!["/missing/config.dev.yaml".to_string()])
            .load_auto_for_environment(Some(Environment::Development))
            .unwrap();

        assert_eq!(config.environment, Environment::Development);
    }

    #[test]
    fn test_explicit_environment_loads_present_config_file() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../config.dev.yaml");
        let config = ConfigLoader::new()
            .with_search_paths(vec![path.display().to_string()])
            .load_auto_for_environment(Some(Environment::Development))
            .unwrap();

        assert_eq!(config.environment, Environment::Development);
        assert!(!config.symbols.is_empty());
    }

    #[test]
    fn test_explicit_production_environment_rejects_missing_files() {
        let path = "/missing/config.prod.yaml";
        let error = ConfigLoader::new()
            .with_search_paths(vec![path.to_string()])
            .load_auto_for_environment(Some(Environment::Production))
            .unwrap_err();

        assert!(error.to_string().contains(path));
        assert!(error.to_string().contains("No configuration files found"));
    }

    #[test]
    fn test_unset_environment_is_fatal() {
        let error = ConfigLoader::new()
            .load_auto_for_environment(None)
            .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("VEX_ENV"));
        assert!(message.contains("ENVIRONMENT"));
        assert!(message.contains("ENV"));
        assert!(message.contains("NODE_ENV"));
        assert!(message.contains("development, test, or production"));
    }

    // #[test]
    // fn test_load_with_allow_missing() {
    //     let loader = ConfigLoader::new().allow_missing_files();
    //     let result = loader.load_with_environment(Some(Environment::Development));
    //     // Should succeed with default config when no files are found
    //     assert!(result.is_ok());
    // }
}
