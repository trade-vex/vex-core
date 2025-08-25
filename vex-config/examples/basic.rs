//! Basic configuration loading example

use tracing_subscriber::fmt;
use vex_config::{Environment, VexConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for the example
    fmt::init();

    println!("=== VEX Config Basic Example ===\n");

    // 1. Load configuration with auto-detected environment
    println!("1. Loading configuration with auto-detected environment...");
    match VexConfig::load_auto() {
        Ok(config) => {
            println!("Loaded config for environment: {}", config.environment());
            println!(
                "Core networking port: {}",
                config.core_networking.initial_port
            );
            println!("Logging level: {}", config.logging.level);
        }
        Err(e) => {
            println!("Failed to load auto config: {}", e);
            println!("This is expected if no config files exist");
        }
    }

    println!();

    // 2. Load configuration for specific environments
    for env in [
        Environment::Development,
        Environment::Test,
        Environment::Production,
    ] {
        println!("2. Loading configuration for {} environment...", env);

        let config = VexConfig::load_for_environment(env.clone()).unwrap_or_else(|_| {
            println!("   → Using default configuration (no files found)");
            VexConfig::new(env)
        });

        println!("Environment: {}", config.environment());
        println!("Core ID: {}", config.core_networking.core_id);
        println!(
            "Authentication enabled: {}",
            config.core_networking.enable_authentication
        );
        println!("Max gateways: {}", config.core_networking.max_gateways);
        println!("Log format: {:?}", config.logging.format);
        println!("Gateway ID: {}", config.gateway_networking.gateway_id);
        println!();
    }

    // 3. Demonstrate configuration validation
    println!("3. Testing configuration validation...");

    let mut config = VexConfig::new(Environment::Development);

    // Valid configuration
    match config.validate() {
        Ok(_) => println!("Configuration is valid"),
        Err(e) => println!("Configuration validation failed: {}", e),
    }

    // Invalid configuration
    config.core_networking.initial_port = 0; // Invalid port
    match config.validate() {
        Ok(_) => println!("Expected validation to fail"),
        Err(e) => println!("Validation correctly failed: {}", e),
    }

    println!();

    // 4. Demonstrate environment checks
    println!("4. Environment-specific behavior...");

    for env in [
        Environment::Development,
        Environment::Test,
        Environment::Production,
    ] {
        let config = VexConfig::new(env);

        println!("   {} environment:", config.environment());
        println!("     - Is development: {}", config.is_development());
        println!("     - Is test: {}", config.is_test());
        println!("     - Is production: {}", config.is_production());
        println!(
            "     - Authentication: {}",
            config.core_networking.enable_authentication
        );
        println!(
            "     - Max connections per address: {}",
            config.core_networking.max_connections_per_address
        );
    }

    println!("\n=== Example completed successfully ===");

    Ok(())
}
