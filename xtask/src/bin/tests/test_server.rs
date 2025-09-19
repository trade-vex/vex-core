use common::CoreMarketSpecificationBuilder;
use hashbrown::HashMap;
use std::env;
use vex_config::CoreNetworkingConfig;
use vex_networking::server::VexCoreServer;
use vex_server::init_exchange;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read configuration from environment variables
    tracing_subscriber::fmt::init();
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

    let mut symbol_specs = HashMap::new();
    let btcusd_market = CoreMarketSpecificationBuilder::default()
        .market_id(10)
        .build()
        .unwrap();
    symbol_specs.insert(10_u32, btcusd_market);
    let (_, producer) = init_exchange(symbol_specs);
    let mut server = VexCoreServer::new(server_config, producer)?;

    // Start the server's event loop
    println!("Server listening for messages...");
    server.start()?;
    Ok(())
}
