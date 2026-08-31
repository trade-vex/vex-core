//! Integration tests for VEX configuration

use std::env;
use std::sync::Mutex;
use tempfile::TempDir;
use vex_config::{ConfigError, ConfigLoader, Environment, VexConfig};

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

    // Different logging configurations
    assert_eq!(
        dev_config.logging.level,
        vex_config::logging::LogLevel::Debug
    );
    assert_eq!(
        test_config.logging.level,
        vex_config::logging::LogLevel::Info
    );
    assert_eq!(
        prod_config.logging.level,
        vex_config::logging::LogLevel::Info
    );
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

#[test]
fn test_file_loading_and_saving() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config_path = temp_dir.path().join("test_config.toml");

    // Create and save a configuration
    let original_config = VexConfig::new(Environment::Test);
    original_config.save_to_file(&config_path)?;

    // Load the configuration back
    let loaded_config = VexConfig::load_from_file(&config_path)?;

    assert_eq!(loaded_config.environment, Environment::Test);
    assert_eq!(
        loaded_config.core_networking.core_id,
        original_config.core_networking.core_id
    );
    assert_eq!(loaded_config.logging.level, original_config.logging.level);

    Ok(())
}

#[test]
fn test_different_file_formats() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let mut config = VexConfig::new(Environment::Production);
    config.symbols = VexConfig::new(Environment::Development).symbols;

    let toml_path = temp_dir.path().join("config.toml");
    let json_path = temp_dir.path().join("config.json");

    // Save in different formats
    config.save_to_file(&toml_path)?;
    config.save_to_file(&json_path)?;

    // Load from different formats
    let toml_config = VexConfig::load_from_file(&toml_path)?;
    let json_config = VexConfig::load_from_file(&json_path)?;

    // All should have the same core values
    assert_eq!(toml_config.environment, Environment::Production);
    assert_eq!(json_config.environment, Environment::Production);

    assert_eq!(
        toml_config.core_networking.core_id,
        config.core_networking.core_id
    );
    assert_eq!(
        json_config.core_networking.core_id,
        config.core_networking.core_id
    );

    Ok(())
}

#[test]
fn test_shipped_development_config_matches_defaults() -> Result<(), Box<dyn std::error::Error>> {
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
        env::set_var("TEST__LOGGING__LEVEL", "error");
        env::set_var("TEST__GATEWAY_NETWORKING__MAX_MESSAGE_SIZE", "32768");
    }

    // No file: the environment defaults stand in for it, and the overrides still apply.
    let result = ConfigLoader::new()
        .with_search_paths(vec!["/nonexistent/config.toml".to_string()])
        .with_env_prefix("TEST")
        .allow_missing_files()
        .load_for_environment(Environment::Development);

    unsafe {
        env::remove_var("TEST__CORE_NETWORKING__INITIAL_PORT");
        env::remove_var("TEST__LOGGING__LEVEL");
        env::remove_var("TEST__GATEWAY_NETWORKING__MAX_MESSAGE_SIZE");
    }

    let config = result?;
    assert_eq!(config.environment, Environment::Development);
    assert_eq!(config.core_networking.initial_port, 9999);
    assert_eq!(config.logging.level, vex_config::logging::LogLevel::Error);
    assert_eq!(config.gateway_networking.max_message_size, 32768);

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
fn test_first_existing_config_file_wins() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let first = temp_dir.path().join("first.toml");
    let second = temp_dir.path().join("second.toml");

    let mut config = VexConfig::new(Environment::Development);
    config.kafka_broker = "first-kafka:9092".to_string();
    config.save_to_file(&first)?;
    config.kafka_broker = "second-kafka:9092".to_string();
    config.save_to_file(&second)?;

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
fn test_configuration_merging() -> Result<(), Box<dyn std::error::Error>> {
    let mut base_config = VexConfig::new(Environment::Development);
    let mut override_config = VexConfig::new(Environment::Development);

    // Modify the override config
    override_config.core_networking.initial_port = 12345;
    override_config.core_networking.enable_authentication = true;
    override_config.logging.level = vex_config::logging::LogLevel::Error;

    // Original values
    let original_port = base_config.core_networking.initial_port;
    let original_auth = base_config.core_networking.enable_authentication;
    let original_level = base_config.logging.level.clone();

    // Merge configurations
    base_config.merge_with(&override_config)?;

    // Check that override values were applied
    assert_ne!(base_config.core_networking.initial_port, original_port);
    assert_ne!(
        base_config.core_networking.enable_authentication,
        original_auth
    );
    assert_ne!(base_config.logging.level, original_level);

    assert_eq!(base_config.core_networking.initial_port, 12345);
    assert!(base_config.core_networking.enable_authentication);
    assert_eq!(
        base_config.logging.level,
        vex_config::logging::LogLevel::Error
    );

    Ok(())
}

#[test]
fn test_merge_different_environments_fails() {
    let mut dev_config = VexConfig::new(Environment::Development);
    let prod_config = VexConfig::new(Environment::Production);

    let result = dev_config.merge_with(&prod_config);
    assert!(result.is_err());

    if let Err(ConfigError::ValidationError(msg)) = result {
        assert!(msg.contains("different environments"));
    } else {
        panic!("Expected ValidationError about different environments");
    }
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
fn test_logging_configuration() {
    let mut config = VexConfig::new(Environment::Development);

    // Test module-specific log levels
    config.logging.set_module_level(
        "test_module".to_string(),
        vex_config::logging::LogLevel::Trace,
    );
    config.logging.set_module_level(
        "another_module".to_string(),
        vex_config::logging::LogLevel::Error,
    );

    assert_eq!(
        config.logging.get_module_level("test_module"),
        &vex_config::logging::LogLevel::Trace
    );
    assert_eq!(
        config.logging.get_module_level("another_module"),
        &vex_config::logging::LogLevel::Error
    );
    assert_eq!(
        config.logging.get_module_level("unknown_module"),
        &config.logging.level
    );

    // Test custom fields
    config
        .logging
        .add_custom_field("service".to_string(), "test".to_string());
    config
        .logging
        .add_custom_field("version".to_string(), "1.0.0".to_string());

    assert_eq!(
        config.logging.custom_fields.get("service"),
        Some(&"test".to_string())
    );
    assert_eq!(
        config.logging.custom_fields.get("version"),
        Some(&"1.0.0".to_string())
    );

    // Test log level checking
    assert!(
        config
            .logging
            .is_enabled(&vex_config::logging::LogLevel::Info, None)
    );
    assert!(
        config
            .logging
            .is_enabled(&vex_config::logging::LogLevel::Trace, Some("test_module"))
    );
    assert!(!config.logging.is_enabled(
        &vex_config::logging::LogLevel::Debug,
        Some("another_module")
    ));
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
