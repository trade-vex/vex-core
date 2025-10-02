use common::OrderCommand;
use disruptor::{BusySpin, ProcessorSettings, build_multi_producer};
use std::io::Write;
use std::{
    env,
    fs::OpenOptions,
    sync::{Arc, Mutex},
};
use tracing::info;
use vex_config::CoreNetworkingConfig;
use vex_networking::server::VexCoreServer;

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
    let results_path = "/results/received_ids.txt";
    let file = Arc::new(Mutex::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(results_path)?,
    ));

    // A dummy consumer that just logs the received command
    let producer = build_multi_producer(1024, OrderCommand::default, BusySpin)
        .pin_at_core(1)
        .handle_events_with({
            move |cmd: &mut OrderCommand, _, _| {
                info!("Server received OrderCommand Core 1: {:?}", cmd);
            }
        })
        .pin_at_core(2)
        .handle_events_with({
            move |cmd: &mut OrderCommand, _, _| {
                let mut f = file.lock().unwrap();
                writeln!(f, "{}", cmd.order_id).expect("Failed to write to results file");
                info!("Server processing OrderCommand Core 2: {:?}", cmd);
            }
        })
        .and_then()
        .pin_at_core(3)
        .handle_events_with({
            move |cmd: &mut OrderCommand, _, _| {
                info!("Server processing OrderCommand Core 3: {:?}", cmd);
            }
        })
        .build();

    let mut server = VexCoreServer::new(server_config, producer)?;

    // Start the server's event loop
    println!("Server listening for messages...");
    server.start()?; // This will run indefinitely

    Ok(())
}
// use common::CoreMarketSpecificationBuilder;
// use hashbrown::HashMap;
// use std::env;
// use vex_config::CoreNetworkingConfig;
// use vex_networking::server::VexCoreServer;
// use vex_server::init_exchange;

// fn main() -> Result<(), Box<dyn std::error::Error>> {
//     // Read configuration from environment variables
//     tracing_subscriber::fmt::init();
//     let server_host = env::var("VEX_SERVER_HOST").unwrap_or("127.0.0.1".to_string());
//     let listen_port: u16 = env::var("VEX_SERVER_PORT")?.parse()?;
//     println!("Server starting on port {listen_port}");

//     let mut server_config = CoreNetworkingConfig::test_defaults();
//     server_config.local_address = server_host;
//     server_config.context_dir =
//         env::var("VEX_CONTEXT_DIR").unwrap_or("/dev/shm/aeron-test-server".to_string());
//     server_config.initial_port = listen_port;
//     server_config.initial_control_port = listen_port + 1;
//     server_config.max_gateways = 15;
//     server_config.max_connections_per_address = 10;

//     let mut symbol_specs = HashMap::new();
//     let btcusd_market = CoreMarketSpecificationBuilder::default()
//         .market_id(10)
//         .build()
//         .unwrap();
//     symbol_specs.insert(10_u32, btcusd_market);
//     let (_, producer) = init_exchange(symbol_specs);
//     let mut server = VexCoreServer::new(server_config, producer)?;

//     // Start the server's event loop
//     println!("Server listening for messages...");
//     server.start()?;
//     Ok(())
// }
