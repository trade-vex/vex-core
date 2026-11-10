use std::sync::atomic::Ordering;
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
use tracing::{debug, error, info};
use tracing_subscriber::fmt;
use vex_config::VexConfig;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    fmt::init();

    // As for the server, VEX_ENV must name the environment: unset or invalid is an error, never a
    // silent Development fallback. Development and test load their defaults when no file exists.
    let mut config = VexConfig::load_auto()?;

    // Override server networking from environment for Docker deployment
    if let Ok(server_port) = std::env::var("VEX_SERVER_PORT")
        && let Ok(port) = server_port.parse::<u16>()
    {
        info!("Using server port from environment: {}", port);
        config.core_networking.initial_port = port;
        config.core_networking.initial_control_port = port + 1;
    }

    // In Docker, we need to listen on all interfaces to be reachable from other containers
    if std::env::var("VEX_SERVER_HOST").is_ok() {
        info!("Running in Docker mode, binding to 0.0.0.0");
        config.core_networking.local_address = "0.0.0.0".to_string();
    }

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

    let engine = vex_server::start(config).map_err(|e| {
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

    let shutdown_trigger = engine.shutdown_handle();

    ctrlc::set_handler(move || {
        info!(
            target: "server_main",
            action = "shutdown_signal_received"
        );
        shutdown_trigger.store(true, Ordering::Release);
    })?;

    engine.join()?;

    Ok(())
}
