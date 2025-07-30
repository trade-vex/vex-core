use common::cmd::{OrderCommand, OrderCommandType, decode_order_command};
use common::model::enums::{OrderType, Side};
use disruptor::{BusySpin, ProcessorSettings, build_multi_producer};
use rusteron_client::{AeronFragmentHandlerCallback, AeronHeader, find_unused_udp_port};
use std::time::Duration;
use std::{net::SocketAddr, thread};
use tracing::{error, info};
use vex_config::{CoreNetworkingConfig, GatewayNetworkingConfig};
use vex_networking::client::{GatewayError, VexGateway};
use vex_networking::server::VexCoreServer;

/// Fragment handler for processing OrderCommand messages from core
struct OrderCommandHandler {
    gateway_id: String,
}

impl AeronFragmentHandlerCallback for OrderCommandHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
        // Deserialize OrderCommand
        match decode_order_command(buffer) {
            Ok(order_command) => {
                info!(
                    "Gateway {}: Received OrderCommand: {:?}",
                    self.gateway_id, order_command
                );
                // Call the callback to handle the order command
                // (self.callback)(order_command);
            }
            Err(e) => {
                error!(
                    "Gateway {}: Failed to decode OrderCommand: {:?}",
                    self.gateway_id, e
                );
            }
        }
    }
}

/// Helper to create test addresses
fn create_test_addresses() -> (SocketAddr, SocketAddr) {
    let server_port = find_unused_udp_port(40300).unwrap();
    let client_port = find_unused_udp_port(40350).unwrap();

    let server_addr = format!("127.0.0.1:{server_port}").parse().unwrap();
    let client_addr = format!("127.0.0.1:{client_port}").parse().unwrap();
    info!("server_addr: {}", server_addr);
    info!("client_addr: {}", client_addr);

    (server_addr, client_addr)
}

#[test_log::test]
fn test_client_server_communication() {
    // This test demonstrates the actual usage of run() methods
    let (server_addr, _client_addr) = create_test_addresses();

    // start aeron media driver in background, and create context directories for client and server in test-data directory
    let client_handle = thread::spawn(move || -> Result<(), GatewayError> {
        let mut client_config = GatewayNetworkingConfig::test_defaults();
        client_config.core_port = server_addr.port();
        client_config.core_control_port = server_addr.port() + 1;
        info!("client_config: {:?}", client_config);
        let mut client = VexGateway::new(client_config)?;

        let handler = OrderCommandHandler {
            gateway_id: client.gateway_id().to_string(),
        };

        match client.start(handler) {
            Ok(()) => println!("Client run() completed successfully"),
            Err(e) => println!("Client run() error: {e}"),
        }

        let mut order_command = OrderCommand {
            command: OrderCommandType::PlaceLimitOrder,
            user_id: 1,
            reserve_bid_price: 150,
            size: 100,
            order_type: OrderType::Gtc,
            timestamp: 1,
            matcher_event: None,
            side: Side::Ask,
            order_id: 1,
            symbol_id: 3124,
            price: 150,
        };
        for i in 0..10 {
            order_command.order_id = i;
            client.send_order_command(&order_command)?;
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    });

    let _ = thread::spawn(move || {
        let mut server_config = CoreNetworkingConfig::test_defaults();
        server_config.initial_port = server_addr.port();
        server_config.initial_control_port = server_addr.port() + 1;
        info!("server_config: {:?}", server_config);
        let producer = build_multi_producer(1024, || OrderCommand::default(), BusySpin)
            .pin_at_core(1)
            .handle_events_with({
                move |cmd: &OrderCommand, _, _| {
                    info!("Server received OrderCommand Core 1: {:?}", cmd);
                }
            })
            .pin_at_core(2)
            .handle_events_with({
                move |cmd: &OrderCommand, _, _| {
                    info!("Server processing OrderCommand Core 2: {:?}", cmd);
                }
            })
            .build();
        let mut server = VexCoreServer::new(server_config, producer).unwrap();
        match server.start() {
            Ok(()) => println!("Server run() completed successfully (unexpected)"),
            Err(e) => println!("Server run() error: {e}"),
        }
    });
    let client_result = client_handle.join();

    match client_result {
        Ok(Ok(())) => println!("✓ Client run() test passed"),
        Ok(Err(e)) => panic!("Client run() test failed: {e}"),
        Err(_) => panic!("Client run() test panicked"),
    }
}
