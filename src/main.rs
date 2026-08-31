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
    let mut config = VexConfig::load_auto()?;

    apply_container_networking_overrides(&mut config);

    if let Ok(pinning_str) = std::env::var("ENABLE_CORE_PINNING") {
        if let Ok(pinning_value) = pinning_str.parse::<bool>() {
            config.core_networking.enable_core_pinning = pinning_value;
        }
    }

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

    info!("CORE CONFIG 226: {:?}", config);

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

fn apply_container_networking_overrides(config: &mut VexConfig) {
    config.core_networking.local_address = "0.0.0.0".to_string();
    config.core_networking.context_dir = "/dev/shm/aeron".to_string();
}

#[cfg(test)]
mod tests {
    use super::*;
    // #166 removed the crate-level Environment import; #180's tests still need it.
    use vex_config::Environment;

    #[test]
    fn container_networking_overrides_preserve_configured_kafka_broker() {
        let mut config = VexConfig::new(Environment::Development);
        config.kafka_broker = "configured-kafka:19092".to_string();

        apply_container_networking_overrides(&mut config);

        assert_eq!(config.kafka_broker, "configured-kafka:19092");
        assert_eq!(config.core_networking.local_address, "0.0.0.0");
        assert_eq!(config.core_networking.context_dir, "/dev/shm/aeron");
    }
}
