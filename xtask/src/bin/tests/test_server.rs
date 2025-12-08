use std::sync::atomic::Ordering;
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
use tracing::{debug, error, info, warn};
use tracing_subscriber::fmt;
use vex_config::{VexConfig, environment::Environment};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    fmt::init();

    // Load configuration
    let mut config = VexConfig::load_auto().or_else(|e| {
        warn!("Failed to load configuration from files: {e}");
        info!("Using default Development configuration for e2e tests");
        Ok::<_, Box<dyn std::error::Error>>(VexConfig::new(Environment::Development))
    })?;

    // Override Kafka broker from environment if set
    if let Ok(kafka_broker) = std::env::var("VEX_KAFKA_BROKER") {
        info!("Using Kafka broker from environment: {}", kafka_broker);
        config.kafka_broker = kafka_broker;
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

    let args: Vec<String> = std::env::args().collect();
    let engine = if args.contains(&"--replay".to_string()) {
        info!(target: "server_main", action = "starting_with_replay");
        vex_server::start(config, true).map_err(|e| {
            error!(
                target: "server_main",
                action = "engine_start_with_replay_failed",
                error = %e
            );
            e
        })?
    } else {
        vex_server::start(config, false).map_err(|e| {
            error!(
                target: "server_main",
                action = "engine_start_failed",
                error = %e
            );
            e
        })?
    };

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
