use common::{CoreMarketSpecification, MarketType};
use hashbrown::HashMap;
use std::env;
use vex_config::CoreNetworkingConfig;
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
    let _results_path = "/results/received_ids.txt";
    // let file = Arc::new(Mutex::new(
    //     OpenOptions::new()
    //         .create(true)
    //         .write(true)
    //         .truncate(true)
    //         .open(results_path)?,
    // ));

    // let publications = Arc::new(vex_networking::server::GatewayPublications::new());

    // A dummy consumer that just logs the received command
    let mut specs = HashMap::new();
    let base_asset_id = 1;
    let quote_asset_id = 2;
    // Market ID: base asset in lower 16 bits, quote in upper 16
    let market_id = ((quote_asset_id as u32) << 16) | (base_asset_id as u32);
    add_spec(market_id, &mut specs);

    let (core_engine, producer) = init_exchange(specs);
    let t = core_engine.run(producer, server_config);

    t.join().unwrap()?;

    Ok(())
}

fn add_spec(market_id: u32, specs: &mut HashMap<u32, CoreMarketSpecification>) {
    specs.insert(
        market_id,
        CoreMarketSpecification::builder()
            .market_id(market_id)
            .market_type(MarketType::Spot)
            .maker_fee(10) // 0.1%
            .taker_fee(20) // 0.2%
            .slippage(5)
            .build()
            .unwrap(),
    );
}
