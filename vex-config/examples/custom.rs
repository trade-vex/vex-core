//! Custom configuration loading with search paths and environment variables

use std::env;
use std::fs;
use tempfile::TempDir;
use vex_config::{ConfigLoader, Environment, VexConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== VEX Config Custom Loading Example ===\n");

    // Create temporary directory for example configs
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path();

    // 1. Create example configuration files
    println!("1. Creating example configuration files...");

    let base_config = r#"
environment = "development"

[core_networking]
context_dir = "/tmp/aeron-core-custom"
local_address = "127.0.0.1"
initial_port = 45001
core_id = "vex-core-custom"
enable_authentication = false

[gateway_networking]
gateway_id = "custom-gateway"
max_message_size = 8192

[logging]
level = "debug"
format = "pretty"
include_location = true

[[logging.outputs]]
type = "stdout"
"#;

    let override_config = r#"
[core_networking]
initial_port = 46001
enable_authentication = true
max_gateways = 50

[logging]
level = "info"
async_logging = true

[logging.custom_fields]
service = "vex-core-custom"
version = "1.0.0"
"#;

    let base_config_path = temp_path.join("base.toml");
    let override_config_path = temp_path.join("override.toml");

    fs::write(&base_config_path, base_config)?;
    fs::write(&override_config_path, override_config)?;

    println!("Created base config: {}", base_config_path.display());
    println!(
        "Created override config: {}",
        override_config_path.display()
    );

    println!();

    // 2. Load configuration with custom search paths
    println!("2. Loading configuration with custom search paths...");

    let config = ConfigLoader::new()
        .with_search_paths(vec![base_config_path.to_string_lossy().to_string()])
        .allow_missing_files()
        .load_for_environment(Environment::Development)?;

    println!("Loaded base configuration");
    println!("Core port: {}", config.core_networking.initial_port);
    println!("Core ID: {}", config.core_networking.core_id);
    println!(
        "Authentication: {}",
        config.core_networking.enable_authentication
    );
    println!("Gateway ID: {}", config.gateway_networking.gateway_id);
    println!("Logging level: {}", config.logging.level);

    println!();

    // 3. Demonstrate configuration merging
    println!("3. Demonstrating configuration merging...");

    let mut base_config = VexConfig::load_from_file(&base_config_path)?;
    let override_config = VexConfig::load_from_file(&override_config_path)?;

    println!("Before merge:");
    println!("Port: {}", base_config.core_networking.initial_port);
    println!(
        "Authentication: {}",
        base_config.core_networking.enable_authentication
    );
    println!("Max gateways: {}", base_config.core_networking.max_gateways);
    println!("Async logging: {}", base_config.logging.async_logging);

    base_config.merge_with(&override_config)?;

    println!("After merge:");
    println!("Port: {}", base_config.core_networking.initial_port);
    println!(
        "Authentication: {}",
        base_config.core_networking.enable_authentication
    );
    println!("Max gateways: {}", base_config.core_networking.max_gateways);
    println!("Async logging: {}", base_config.logging.async_logging);

    println!();

    // 4. Demonstrate environment variable overrides
    println!("4. Testing environment variable overrides...");

    // Set some environment variables
    unsafe {
        env::set_var("VEX_CORE_NETWORKING__INITIAL_PORT", "47001");
    }
    unsafe {
        env::set_var("VEX_LOGGING__LEVEL", "trace");
    }
    unsafe {
        env::set_var("VEX_GATEWAY_NETWORKING__MAX_MESSAGE_SIZE", "16384");
    }

    let config_with_env = ConfigLoader::new()
        .with_search_paths(vec![base_config_path.to_string_lossy().to_string()])
        .with_env_prefix("VEX")
        .allow_missing_files()
        .load_for_environment(Environment::Development)?;

    println!("Environment variables applied:");
    println!(
        "Port (from env): {}",
        config_with_env.core_networking.initial_port
    );
    println!(
        "Logging level (from env): {}",
        config_with_env.logging.level
    );
    println!(
        "Max message size (from env): {}",
        config_with_env.gateway_networking.max_message_size
    );

    // Clean up environment variables
    unsafe {
        env::remove_var("VEX_CORE_NETWORKING__INITIAL_PORT");
    }
    unsafe {
        env::remove_var("VEX_LOGGING__LEVEL");
    }
    unsafe {
        env::remove_var("VEX_GATEWAY_NETWORKING__MAX_MESSAGE_SIZE");
    }

    println!();

    // 5. Demonstrate custom configuration builder
    println!("5. Building custom configuration programmatically...");

    let mut custom_config = VexConfig::new(Environment::Development);

    // Customize core networking
    custom_config.core_networking.core_id = "my-custom-core".to_string();
    custom_config.core_networking.max_gateways = 25;
    custom_config.core_networking.enable_authentication = true;

    // Customize logging
    custom_config.logging.set_module_level(
        "networking".to_string(),
        vex_config::logging::LogLevel::Trace,
    );
    custom_config
        .logging
        .set_module_level("orderbook".to_string(), vex_config::logging::LogLevel::Warn);
    custom_config
        .logging
        .add_custom_field("component".to_string(), "custom".to_string());

    println!("Custom configuration created:");
    println!("Core ID: {}", custom_config.core_networking.core_id);
    println!(
        "Max gateways: {}",
        custom_config.core_networking.max_gateways
    );
    println!(
        "Networking log level: {:?}",
        custom_config.logging.get_module_level("networking")
    );
    println!(
        "Orderbook log level: {:?}",
        custom_config.logging.get_module_level("orderbook")
    );
    println!("Custom fields: {:?}", custom_config.logging.custom_fields);

    // Validate the custom configuration
    match custom_config.validate() {
        Ok(_) => println!("Custom configuration is valid"),
        Err(e) => println!("Custom configuration validation failed: {e}"),
    }

    println!();

    // 6. Save configuration in different formats
    println!("6. Saving configuration in different formats...");

    let toml_path = temp_path.join("output.toml");
    let json_path = temp_path.join("output.json");
    let yaml_path = temp_path.join("output.yaml");

    custom_config.save_to_file(&toml_path)?;
    custom_config.save_to_file(&json_path)?;
    custom_config.save_to_file(&yaml_path)?;

    println!("Saved TOML: {}", toml_path.display());
    println!("Saved JSON: {}", json_path.display());
    println!("Saved YAML: {}", yaml_path.display());

    // Show file sizes
    println!("   File sizes:");
    println!("     - TOML: {} bytes", fs::metadata(&toml_path)?.len());
    println!("     - JSON: {} bytes", fs::metadata(&json_path)?.len());
    println!("     - YAML: {} bytes", fs::metadata(&yaml_path)?.len());

    println!("\n=== Custom configuration example completed ===");

    Ok(())
}
