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

    // Load configuration
    let config = VexConfig::load_auto()?;

    if config.core_networking.enable_core_pinning {
        info!(
            target: "server_main",
            action = "cpu_pinning_enabled",
            "CPU core pinning is ENABLED for optimal performance"
        );
    } else {
        info!(
            target: "server_main",
            action = "cpu_pinning_disabled",
            "CPU core pinning is DISABLED (suitable for development/testing)"
        );
    }

    info!(
        "Loaded configuration for environment: {}",
        config.environment
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
