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

    /// Load configuration using auto-detection of environment
    pub fn load_auto(self) -> Result<VexConfig> {
        let environment = Environment::detect();
        self.load_for_environment(environment)
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

        // Start with VexConfig defaults, so partial configs can be loaded.
        let default_config_toml = toml::to_string(&VexConfig::default())
            .map_err(|e| ConfigError::SerializationError(e.to_string()))?;
        let mut builder = Config::builder().add_source(config::File::from_str(
            &default_config_toml,
            config::FileFormat::Toml,
        ));
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
        let env = environment.unwrap_or_else(Environment::detect);

        // Start from env‑specific defaults so partial files/env vars can override safely
        let env_config_toml = toml::to_string(&VexConfig::new(env.clone()))
            .map_err(|e| ConfigError::SerializationError(e.to_string()))?;
        let mut builder = Config::builder().add_source(config::File::from_str(
            &env_config_toml,
            config::FileFormat::Toml,
        ));

        // Get search paths
        let search_paths = if self.search_paths.is_empty() {
            env.default_config_paths()
        } else {
            self.search_paths.clone()
        };

        // Add configuration files in order of precedence
        let mut files_found = false;
        for path in search_paths.iter().rev() {
            let config_path = Path::new(path);
            if config_path.exists() {
                files_found = true;
                let format = self.detect_file_format(config_path)?;
                builder = builder.add_source(File::from(config_path).format(format));
                tracing::info!("Loaded config file: {}", path);
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
                // Apply environment variables to the default config
                // This is a simplified approach - in a real implementation,
                // you might want to use a more sophisticated merging strategy
                default_config = self.apply_env_vars_to_config(default_config, prefix, &env)?;
            }

            default_config.validate()?;
            return Ok(default_config);
        }

        // Add environment-specific variables
        if let Some(prefix) = &self.env_prefix {
            let env_prefix = format!("{}_{}", prefix, env.env_key());
            builder = builder.add_source(
                config::Environment::with_prefix(&env_prefix)
                    .try_parsing(true)
                    .separator("__"),
            );

            // Add environment-specific prefix (higher precedence)
            let env_specific_prefix = format!("{}_{}", prefix, env.env_key());
            builder = builder.add_source(
                config::Environment::with_prefix(&env_specific_prefix)
                    .try_parsing(true)
                    .separator("__"),
            );
        }

        let config = builder.build()?;
        let mut vex_config: VexConfig = config.try_deserialize()?;

        // Ensure environment matches what we expect
        vex_config.environment = env;

        vex_config.validate()?;
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

        let env_specific_prefix = format!("{}_{}", prefix, env.env_key());
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

    // #[test]
    // fn test_load_with_allow_missing() {
    //     let loader = ConfigLoader::new().allow_missing_files();
    //     let result = loader.load_with_environment(Some(Environment::Development));
    //     // Should succeed with default config when no files are found
    //     assert!(result.is_ok());
    // }
}
