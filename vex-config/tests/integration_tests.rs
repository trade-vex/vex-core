//! Integration tests for VEX configuration

use std::env;
use std::sync::Mutex;
use tempfile::TempDir;
use vex_config::{ConfigError, ConfigLoader, Environment, GatewayAuthenticationKey, VexConfig};

static ENVIRONMENT_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn test_basic_configuration_loading() {
    let config = VexConfig::new(Environment::Development);

    assert_eq!(config.environment, Environment::Development);
    assert!(config.validate().is_ok());
    assert!(config.is_development());
    assert!(!config.is_production());
}

#[test]
fn test_environment_specific_defaults() {
    let dev_config = VexConfig::new(Environment::Development);
    let test_config = VexConfig::new(Environment::Test);
    let prod_config = VexConfig::new(Environment::Production);

    // Dev should have relaxed security
    assert!(!dev_config.core_networking.enable_authentication);

    // Test and prod should have strict security
    assert!(test_config.core_networking.enable_authentication);
    assert!(prod_config.core_networking.enable_authentication);

    // Different port ranges for environments
    assert_eq!(dev_config.core_networking.initial_port, 40001);
    assert_eq!(test_config.core_networking.initial_port, 40001);
    assert_eq!(prod_config.core_networking.initial_port, 3521);
}

#[test]
fn test_configuration_validation() {
    let mut config = VexConfig::new(Environment::Development);

    // Valid configuration should pass
    assert!(config.validate().is_ok());

    // Invalid port should fail
    config.core_networking.initial_port = 0;
    assert!(config.validate().is_err());

    // Same port for initial and control should fail
    config.core_networking.initial_port = 8080;
    config.core_networking.initial_control_port = 8080;
    assert!(config.validate().is_err());

    // Empty core ID should fail
    config.core_networking.initial_control_port = 8081;
    config.core_networking.core_id = String::new();
    assert!(config.validate().is_err());
}

#[cfg(feature = "balance-preload")]
#[test]
fn production_configuration_rejects_balance_preload() {
    let mut production = VexConfig::new(Environment::Production);
    // PRs #176 (auth) and #151 (symbols) added production gates that run BEFORE the preload
    // rule; satisfy them so this test still isolates the preload rule rather than passing
    // because an earlier gate fired.
    production.core_networking.gateway_authentication_key = GatewayAuthenticationKey::from(
        "5f0c3a9e7d2b4816c4e1a7f39b0d2c6e8a1f4b7d3e9c0a2f6b8d1e4c7a3f9b05",
    );
    production.symbols = VexConfig::new(Environment::Development).symbols;
    assert!(production.validate().is_ok());
    production.balance_preload.enabled = true;
    assert!(production.validate().is_err());

    let mut development = VexConfig::new(Environment::Development);
    development.balance_preload.enabled = true;
    assert!(development.validate().is_ok());
}

#[test]
fn test_empty_gateway_authentication_key_by_environment() {
    let development = VexConfig::new(Environment::Development);
    assert!(development.validate().is_ok());

    let test = VexConfig::new(Environment::Test);
    assert!(test.validate().is_ok());

    let production = VexConfig::new(Environment::Production);
    assert!(production.validate().is_err());

    let mut authenticated_production = production;
    authenticated_production
        .core_networking
        .gateway_authentication_key = GatewayAuthenticationKey::from(
        "5f0c3a9e7d2b4816c4e1a7f39b0d2c6e8a1f4b7d3e9c0a2f6b8d1e4c7a3f9b05",
    );
    // PR #151 additionally rejects a production config with no symbols. Satisfy that rule so
    // this test still isolates PR #176's authentication gate rather than tripping on #151's.
    authenticated_production.symbols = VexConfig::new(Environment::Development).symbols;
    assert!(authenticated_production.validate().is_ok());

    authenticated_production
        .core_networking
        .enable_authentication = false;
    assert!(authenticated_production.validate().is_err());
}

#[test]
fn test_file_loading() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config_path = temp_dir.path().join("test_config.json");

    let original_config = VexConfig::new(Environment::Test);
    std::fs::write(&config_path, serde_json::to_string(&original_config)?)?;

    // Load the configuration back
    let loaded_config = VexConfig::load_from_file(&config_path)?;

    assert_eq!(loaded_config.environment, Environment::Test);
    assert_eq!(
        loaded_config.core_networking.core_id,
        original_config.core_networking.core_id
    );

    Ok(())
}

#[test]
fn test_shipped_development_config_matches_defaults() -> Result<(), Box<dyn std::error::Error>> {
    // `load_from_file` applies `VEX__*` overrides, which other tests set.
    let _guard = ENVIRONMENT_LOCK.lock().unwrap();
    let config_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../config.dev.yaml");
    let loaded = VexConfig::load_from_file(config_path)?;
    let defaults = VexConfig::new(Environment::Development);

    assert_eq!(
        serde_json::to_value(loaded)?,
        serde_json::to_value(defaults)?
    );

    Ok(())
}

#[test]
fn test_load_error_names_path_and_parse_cause() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config_path = temp_dir.path().join("config.prod.yaml");
    std::fs::write(&config_path, "invalid: [yaml")?;

    let error = ConfigLoader::new()
        .with_search_paths(vec![config_path.display().to_string()])
        .load_for_environment(Environment::Production)
        .unwrap_err()
        .to_string();

    assert!(error.contains(&config_path.display().to_string()));
    assert!(error.contains("Parse error"));

    Ok(())
}

#[test]
fn test_environment_variable_overrides() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = ENVIRONMENT_LOCK.lock().unwrap();
    // config-rs puts the `__` separator after the prefix too: `TEST__...`, not `TEST_...`.
    unsafe {
        env::set_var("TEST__CORE_NETWORKING__INITIAL_PORT", "9999");
    }

    // No file: the environment defaults stand in for it, and the overrides still apply.
    let result = ConfigLoader::new()
        .with_search_paths(vec!["/nonexistent/config.toml".to_string()])
        .with_env_prefix("TEST")
        .allow_missing_files()
        .load_for_environment(Environment::Development);

    unsafe {
        env::remove_var("TEST__CORE_NETWORKING__INITIAL_PORT");
    }

    let config = result?;
    assert_eq!(config.environment, Environment::Development);
    assert_eq!(config.core_networking.initial_port, 9999);

    Ok(())
}

#[test]
fn test_environment_overrides_of_defaults_are_validated() {
    let _guard = ENVIRONMENT_LOCK.lock().unwrap();
    unsafe {
        env::set_var("TESTINVALID__CORE_NETWORKING__INITIAL_PORT", "0");
    }

    let result = ConfigLoader::new()
        .with_search_paths(vec!["/nonexistent/config.toml".to_string()])
        .with_env_prefix("TESTINVALID")
        .allow_missing_files()
        .load_for_environment(Environment::Development);

    unsafe {
        env::remove_var("TESTINVALID__CORE_NETWORKING__INITIAL_PORT");
    }

    let error = result.unwrap_err();
    assert!(matches!(error, ConfigError::LoadError { .. }), "{error}");
    assert!(
        error.to_string().contains("Initial port cannot be 0"),
        "{error}"
    );
}

#[test]
fn test_missing_file_defaults_load_unchanged() -> Result<(), Box<dyn std::error::Error>> {
    // The defaults reach the loader as a config source; nothing may be lost in that round trip.
    for environment in [Environment::Development, Environment::Test] {
        let loaded = ConfigLoader::new()
            .with_search_paths(vec!["/nonexistent/config.yaml".to_string()])
            .without_env_prefix()
            .allow_missing_files()
            .load_for_environment(environment.clone())?;

        assert_eq!(
            serde_json::to_value(loaded)?,
            serde_json::to_value(VexConfig::new(environment))?
        );
    }

    Ok(())
}

#[test]
fn test_production_config_without_schema_registry_url_fails_to_load()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("config.prod.json");
    let mut config = VexConfig::new(Environment::Production);
    config.symbols = VexConfig::new(Environment::Development).symbols;
    config.core_networking.gateway_authentication_key = GatewayAuthenticationKey::from(
        "5f0c3a9e7d2b4816c4e1a7f39b0d2c6e8a1f4b7d3e9c0a2f6b8d1e4c7a3f9b05",
    );
    std::fs::write(&path, serde_json::to_string(&config)?)?;
    let load = || {
        ConfigLoader::new()
            .with_search_paths(vec![path.display().to_string()])
            .without_env_prefix()
            .load_for_environment(Environment::Production)
    };
    assert_eq!(load()?.schema_registry_url, "http://localhost:8081");

    // Without the key, and with no override, the file does not load: production gets no default.
    let mut without_registry = serde_json::to_value(&config)?;
    without_registry
        .as_object_mut()
        .unwrap()
        .remove("schema_registry_url");
    std::fs::write(&path, without_registry.to_string())?;

    let error = load().unwrap_err();
    assert!(matches!(error, ConfigError::LoadError { .. }), "{error}");
    assert!(error.to_string().contains("schema_registry_url"), "{error}");

    Ok(())
}

/// Loads the development defaults with `variable` set to `value`, reading overrides under `prefix`.
fn load_with_variable(prefix: &str, variable: &str, value: &str) -> vex_config::Result<VexConfig> {
    let _guard = ENVIRONMENT_LOCK.lock().unwrap();
    unsafe {
        env::set_var(variable, value);
    }
    let result = ConfigLoader::new()
        .with_search_paths(vec!["/nonexistent/config.yaml".to_string()])
        .with_env_prefix(prefix)
        .allow_missing_files()
        .load_for_environment(Environment::Development);
    unsafe {
        env::remove_var(variable);
    }
    result
}

#[test]
fn test_schema_registry_url_environment_override() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_with_variable("VEX", "VEX__SCHEMA_REGISTRY_URL", "http://redpanda:8081")?;
    assert_eq!(config.schema_registry_url, "http://redpanda:8081");
    Ok(())
}

#[test]
fn gateway_authentication_key_is_a_core_networking_setting()
-> Result<(), Box<dyn std::error::Error>> {
    let key = "0123456789abcdef".repeat(4);
    let config = load_with_variable(
        "AUTHKEY",
        "AUTHKEY__CORE_NETWORKING__GATEWAY_AUTHENTICATION_KEY",
        &key,
    )?;
    assert_eq!(
        config.core_networking.gateway_authentication_key.as_str(),
        key
    );
    Ok(())
}

#[test]
fn test_schema_registry_url_must_be_an_http_url() {
    // A prefix of its own: an invalid `VEX__` value would fail every concurrent `VEX__` load.
    for invalid in ["", "  ", "redpanda:8081"] {
        let error = load_with_variable("BADREGISTRY", "BADREGISTRY__SCHEMA_REGISTRY_URL", invalid)
            .unwrap_err();
        assert!(matches!(error, ConfigError::LoadError { .. }), "{error}");
        assert!(
            error
                .to_string()
                .contains("schema_registry_url must be an http:// or https:// URL"),
            "{error}"
        );
    }

    let mut config = VexConfig::new(Environment::Development);
    config.schema_registry_url = "https://registry.internal:8081".to_string();
    assert!(config.validate().is_ok());
}

#[test]
fn test_first_existing_config_file_wins() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let first = temp_dir.path().join("first.json");
    let second = temp_dir.path().join("second.json");

    let mut config = VexConfig::new(Environment::Development);
    config.kafka_broker = "first-kafka:9092".to_string();
    std::fs::write(&first, serde_json::to_string(&config)?)?;
    config.kafka_broker = "second-kafka:9092".to_string();
    std::fs::write(&second, serde_json::to_string(&config)?)?;

    let loaded = ConfigLoader::new()
        .with_search_paths(vec![
            first.display().to_string(),
            second.display().to_string(),
        ])
        .without_env_prefix()
        .load_for_environment(Environment::Development)?;

    assert_eq!(loaded.kafka_broker, "first-kafka:9092");

    Ok(())
}

#[test]
fn test_custom_loader_with_missing_files() {
    let result = ConfigLoader::new()
        .with_search_paths(vec![
            "/nonexistent/config1.toml".to_string(),
            "/nonexistent/config2.toml".to_string(),
        ])
        .allow_missing_files()
        .load_for_environment(Environment::Development);

    // Should succeed with default configuration
    assert!(result.is_ok());

    let config = result.unwrap();
    assert_eq!(config.environment, Environment::Development);
}

#[test]
fn test_custom_loader_without_allow_missing_fails() {
    let result = ConfigLoader::new()
        .with_search_paths(vec!["/nonexistent/config.toml".to_string()])
        .load_for_environment(Environment::Development);

    // Should fail when files are missing and not allowed
    assert!(result.is_err());

    if let Err(ConfigError::NotFound(_)) = result {
        // Expected error type
    } else {
        panic!("Expected NotFound error");
    }
}

#[test]
fn test_environment_detection() {
    let _guard = ENVIRONMENT_LOCK.lock().unwrap();

    unsafe {
        env::set_var("VEX_ENV", "production");
    }
    assert_eq!(
        Environment::detect_explicit().unwrap(),
        Some(Environment::Production)
    );

    // Only the canonical spellings select an environment; anything else is an error, never a
    // silent fall-through to some other selection.
    for invalid in ["dev", "prod", "testing", "Production", ""] {
        unsafe {
            env::set_var("VEX_ENV", invalid);
        }
        let error = Environment::detect_explicit().unwrap_err().to_string();
        assert!(error.contains("VEX_ENV"), "{error}");
        assert!(VexConfig::load_auto().is_err());
    }

    // VEX_ENV is the only selector: other variables do not choose an environment.
    unsafe {
        env::remove_var("VEX_ENV");
        env::set_var("ENVIRONMENT", "test");
    }
    assert_eq!(Environment::detect_explicit().unwrap(), None);
    let error = VexConfig::load_auto().unwrap_err().to_string();
    assert!(error.contains("Set VEX_ENV to development, test, or production"));
    unsafe {
        env::remove_var("ENVIRONMENT");
    }
}

#[test]
fn test_auto_loading() {
    let _guard = ENVIRONMENT_LOCK.lock().unwrap();

    // Explicit development selects and loads the shipped config.dev.yaml.
    unsafe {
        env::set_var("VEX_ENV", "development");
    }

    let config = VexConfig::load_auto().unwrap();
    assert_eq!(config.environment, Environment::Development);
    assert!(!config.symbols.is_empty());

    unsafe {
        env::remove_var("VEX_ENV");
    }
}
