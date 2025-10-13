use std::env;
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
use tracing::{error, info, warn};
use tracing_subscriber::fmt;
use vex_config::{CoreNetworkingConfig, Environment, VexConfig};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    fmt::init();

    // Load configuration
    let mut config = VexConfig::load_auto().or_else(|e| {
        warn!("Failed to load configuration from files: {e}");
        info!("Using default Test configuration");
        Ok::<_, Box<dyn std::error::Error>>(VexConfig::new(Environment::Test))
    })?;

    let server_host = env::var("VEX_SERVER_HOST").unwrap_or("127.0.0.1".to_string());
    let listen_port: u16 = env::var("VEX_SERVER_PORT")?.parse()?;
    println!("Server starting on port {listen_port}");

    let mut server_config = CoreNetworkingConfig::test_defaults();
    server_config.local_address = server_host;
    server_config.context_dir =
        env::var("VEX_CONTEXT_DIR").unwrap_or("/dev/shm/aeron-test-server".to_string());
    server_config.initial_port = listen_port;
    server_config.initial_control_port = listen_port + 1;
    server_config.max_gateways = 15;
    server_config.max_connections_per_address = 10;
    config.core_networking = server_config;
    info!(
        "Loaded configuration for environment: {}",
        config.environment
    );

    // Validate configuration
    config.validate().map_err(|e| {
        error!("Configuration validation failed: {e}");
        e
    })?;

    info!("Configuration validated successfully");
    info!("Core ID: {}", config.core_networking.core_id);
    info!("Network port: {}", config.core_networking.initial_port);
    info!("Max gateways: {}", config.core_networking.max_gateways);
    info!("Loading {} symbols", config.symbols.symbols.len());

    let rengine = vex_server::start(config).map_err(|e| {
        error!("Failed to start the engine: {e}");
        e
    })?;

    info!("Server started successfully. Press Ctrl+C to shutdown.");

    // Setup shutdown handler
    ctrlc::set_handler(|| {
        info!("Shutdown signal received. Terminating...");
        std::process::exit(0);
    })?;

    rengine.join()?;

    Ok(())
}
