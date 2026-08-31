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
        let environment = Environment::detect_explicit()?;
        self.load_auto_for_environment(environment)
    }

    fn load_auto_for_environment(
        self,
        explicit_environment: Option<Environment>,
    ) -> Result<VexConfig> {
        let environment = explicit_environment.ok_or_else(no_environment_selected)?;

        // Only a genuinely ABSENT config file may fall back to defaults, and only in development
        // and test. A file that exists but fails to parse, deserialize or validate is an operator
        // error and must surface: swallowing it silently substituted hardcoded defaults
        // (different markets, scale factors and fees) for the configuration the operator
        // intended (finding C2-1).
        let loader = if environment.is_development() || environment.is_test() {
            self.allow_missing_files()
        } else {
            self
        };
        loader.load_for_environment(environment)
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
        let env = match environment {
            Some(environment) => environment,
            None => Environment::detect_explicit()?.ok_or_else(no_environment_selected)?,
        };

        // Get search paths
        let search_paths = if self.search_paths.is_empty() {
            env.default_config_paths()
        } else {
            self.search_paths.clone()
        };

        // The first existing file wins, as documented. Layering every existing file let a later
        // search path (e.g. the shipped zero-fee `./config.dev.yaml`) silently override an
        // operator's earlier file key by key.
        let mut builder = match search_paths.iter().find(|path| Path::new(path).exists()) {
            Some(path) => {
                let config_path = Path::new(path);
                let format = self
                    .detect_file_format(config_path)
                    .map_err(|error| ConfigError::load(search_paths.clone(), error))?;
                tracing::info!(
                    target: "config",
                    action = "config_file_loaded",
                    path = %path
                );
                Config::builder().add_source(File::from(config_path).format(format))
            }
            None if self.allow_missing => {
                tracing::warn!(
                    target: "config",
                    action = "config_file_absent",
                    environment = %env,
                    search_paths = ?search_paths
                );
                tracing::info!(
                    target: "config",
                    action = "using_environment_defaults",
                    environment = %env
                );
                // The defaults take the place of the file, so environment overrides and
                // validation below apply to them exactly as they would to a file. They go in as
                // a JSON document rather than `Config::try_from`, whose serializer drops empty
                // maps (e.g. `logging.custom_fields`), which then fail to deserialize.
                let defaults =
                    serde_json::to_string(&VexConfig::new(env.clone())).map_err(|error| {
                        ConfigError::load(
                            search_paths.clone(),
                            ConfigError::SerializationError(error.to_string()),
                        )
                    })?;
                Config::builder().add_source(File::from_str(&defaults, FileFormat::Json))
            }
            None => {
                return Err(ConfigError::not_found(format!(
                    "No configuration files found in search paths: {search_paths:?}"
                )));
            }
        };

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

fn no_environment_selected() -> ConfigError {
    ConfigError::EnvironmentError(format!(
        "No environment selected. Set {} to development, test, or production",
        Environment::VARIABLE
    ))
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
    fn test_explicit_development_environment_rejects_malformed_file() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("config.dev.yaml");
        std::fs::write(&path, "invalid: [yaml").unwrap();

        let error = ConfigLoader::new()
            .with_search_paths(vec![path.display().to_string()])
            .load_auto_for_environment(Some(Environment::Development))
            .unwrap_err();

        assert!(matches!(error, ConfigError::LoadError { .. }), "{error}");
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
        assert!(message.contains("Set VEX_ENV to development, test, or production"));
    }

    // #[test]
    // fn test_load_with_allow_missing() {
    //     let loader = ConfigLoader::new().allow_missing_files();
    //     let result = loader.load_with_environment(Some(Environment::Development));
    //     // Should succeed with default config when no files are found
    //     assert!(result.is_ok());
    // }
}
