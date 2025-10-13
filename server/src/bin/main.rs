#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
use tracing::{error, info, warn};
use tracing_subscriber::fmt;
use vex_config::{Environment, VexConfig};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    fmt::init();

    // Load configuration
    let config = VexConfig::load_auto().or_else(|e| {
        warn!("Failed to load configuration from files: {e}");
        info!("Using default Test configuration");
        Ok::<_, Box<dyn std::error::Error>>(VexConfig::new(Environment::Test))
    })?;

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
    info!("Config: {}", config);

    let engine = vex_server::start(config).map_err(|e| {
        error!("Failed to start the engine: {e}");
        e
    })?;

    info!("Server started successfully. Press Ctrl+C to shutdown.");

    // Setup shutdown handler
    ctrlc::set_handler(|| {
        info!("Shutdown signal received. Terminating...");
        std::process::exit(0);
    })?;

    engine.join()?;

    Ok(())
}
