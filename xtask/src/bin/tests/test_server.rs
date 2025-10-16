use tracing::{debug, error, info, warn};
use tracing_subscriber::fmt;
use vex_config::{VexConfig, environment::Environment};

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
        error!(
            target: "server_main",
            action = "config_validation_failed",
            error = %e
        );
        e
    })?;

    info!(
        target: "server_main",
        action = "config_validated"
    );
    debug!(target: "server_main", action = "config_snapshot", config = ?config);

    let engine = vex_server::start(config, false).map_err(|e| {
        error!(
            target: "server_main",
            action = "engine_start_failed",
            error = %e
        );
        e
    })?;

    info!(
        target: "server_main",
        action = "server_started"
    );

    // Setup shutdown handler
    ctrlc::set_handler(|| {
        info!(
            target: "server_main",
            action = "shutdown_signal_received"
        );
        std::process::exit(0);
    })?;

    engine.join()?;

    Ok(())
}
