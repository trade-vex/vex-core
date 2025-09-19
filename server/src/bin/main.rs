#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
use tracing::{error, info, warn};
use tracing_subscriber::fmt;
use vex_config::{Environment, VexConfig};
use vex_server::init_exchange;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber for logging
    fmt::init();

    // Load configuration with auto-detected environment
    let config = match VexConfig::load_auto() {
        Ok(config) => {
            info!(
                "Loaded configuration for environment: {}",
                config.environment
            );
            config
        }
        Err(e) => {
            warn!("Failed to load configuration from files: {}", e);
            info!("Using default Development configuration");
            VexConfig::new(Environment::Development)
        }
    };

    // Validate configuration
    if let Err(e) = config.validate() {
        error!("Configuration validation failed: {}", e);
        return Err(e.into());
    }

    info!("Configuration validated successfully");
    info!("Core ID: {}", config.core_networking.core_id);
    info!("Network port: {}", config.core_networking.initial_port);
    info!("Max gateways: {}", config.core_networking.max_gateways);
    info!(
        "Authentication enabled: {}",
        config.core_networking.enable_authentication
    );

    // Initialize the exchange core with symbol specifications from config
    info!("Initializing exchange core...");
    let symbol_specs = config.symbols.symbols.clone();
    info!("Loading {} symbols from configuration", symbol_specs.len());

    let (mut core_engine, producer) = init_exchange(symbol_specs.clone());

    info!("Exchange core initialized successfully");
    for symbol_id in symbol_specs.keys() {
        info!(
            "Added symbol {} with Naive order book implementation",
            symbol_id
        );
    }

    // Start the core engine with networking
    info!("Starting core engine with networking...");
    core_engine.run(producer, config.core_networking);

    // The run() method spawns a thread and starts the server loop
    // The main thread will continue here, so we should keep it alive
    info!("Core engine started. Press Ctrl+C to shutdown.");

    // Wait for shutdown signal
    ctrlc::set_handler(|| {
        info!("Shutdown signal received. Terminating...");
        std::process::exit(0);
    })?;

    // Keep the main thread alive
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}
