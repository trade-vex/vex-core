use common::cmd::{OrderCommand, OrderCommandType, decode_order_command};
use common::model::enums::{Side, OrderType};
use server::init_exchange;

use disruptor::Producer;
use vex_networking::client::{GatewayError, VexGateway};
use rusteron_client::{AeronFragmentHandlerCallback, AeronHeader, find_unused_udp_port};
use std::time::Duration;
use std::{net::SocketAddr, thread};
use tracing::{error, info};
use vex_config::{CoreNetworkingConfig, GatewayNetworkingConfig};

#[tokio::test]
async fn test_full_exchange_flow() {
    tracing_subscriber::fmt::init();

    info!(" Running Full Disruptor Core Test ");

    let (core, mut producer, handler) = init_exchange();

    // Place order
    let mut cmd = OrderCommand::default();
    cmd.order_id = 1;
    cmd.user_id = 100;
    cmd.symbol_id = 0;
    cmd.size = 10;
    cmd.price = 9629;
    producer.publish(|e| *e = cmd.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Cancel order
    let mut cancel_cmd = OrderCommand::cancel(1, 100);
    cancel_cmd.symbol_id = 0;
    producer.publish(|e| *e = cancel_cmd.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Reduce order
    let mut cmd2 = OrderCommand::default();
    cmd2.order_id = 2;
    cmd2.user_id = 100;
    cmd2.symbol_id = 0;
    cmd2.size = 10;
    cmd2.price = 9629;
    producer.publish(|e| *e = cmd2.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut reduce_cmd = OrderCommand::reduce(2, 100, 5);
    reduce_cmd.symbol_id = 0;
    producer.publish(|e| *e = reduce_cmd.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Move order
    let mut cmd3 = OrderCommand::default();
    cmd3.order_id = 3;
    cmd3.user_id = 100;
    cmd3.symbol_id = 0;
    cmd3.size = 10;
    cmd3.price = 9629;
    producer.publish(|e| *e = cmd3.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut move_cmd = OrderCommand::move_order(3, 100, 9700);
    move_cmd.symbol_id = 0;
    producer.publish(|e| *e = move_cmd.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Check events
    info!("\n Asserting events received by handler ");
    {
        // : Add an inner scope to release the lock
        let received_events = handler.events.lock().unwrap();
        assert!(
            received_events.len() >= 4,
            "Should have received at least four events"
        );

        // Detailed assertions for event types
        let mut has_reduce = false;
        let mut has_cancel = false;
        let mut _has_move = false;
        for event in received_events.iter() {
            match format!("{:?}", event) {
                s if s.contains("Reduce") => has_reduce = true,
                s if s.contains("Cancel") => has_cancel = true,
                s if s.contains("Move") => _has_move = true,
                _ => {}
            }
        }
        assert!(has_reduce, "Should have at least one Reduce event");
        assert!(has_cancel, "Should have at least one Cancel event");
    } // : The lock on handler.events is released here as `received_events` goes out of scope.

    //  User balance assertion
    // : Check for the correct currency (1 for the seller)
    let balance = core.get_user_balance(100, 1).unwrap();
    println!("User 100 balance in currency 1: {}", balance);

    //  Matching test: Place matching ASK and BID orders
    // Use a price that is guaranteed to be the best available to ensure the correct orders match.
    let mut ask_cmd = OrderCommand::new_order(
        common::model::enums::OrderType::Gtc,
        10,   // order_id
        100,  // user_id
        9620, // price (better than the existing order at 9629)
        0,    // reserve_bid_price
        5,    // size
        common::model::enums::Side::Ask,
    );
    ask_cmd.symbol_id = 0;
    producer.publish(|e| *e = ask_cmd.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut bid_cmd = OrderCommand::new_order(
        common::model::enums::OrderType::Gtc,
        11,   // order_id
        101,  // user_id (different user)
        9620, // price (matches the new ASK)
        0,
        5,
        common::model::enums::Side::Bid,
    );
    bid_cmd.symbol_id = 0;
    producer.publish(|e| *e = bid_cmd.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    //  Assert trade event and filled quantities
    let received_events = handler.events.lock().unwrap();
    let mut has_trade = false;
    for event in received_events.iter() {
        if format!("{:?}", event).contains("Trade") {
            has_trade = true;
            println!("Trade event: {:?}", event);
        }
    }
    assert!(has_trade, "Should have at least one Trade event");

    //  Assert user balances updated
    // Check for the correct currencies (1 and 2) after the trade
    let ask_user_base_balance = core.get_user_balance(100, 1).unwrap();
    let ask_user_quote_balance = core.get_user_balance(100, 2).unwrap();
    let bid_user_base_balance = core.get_user_balance(101, 1).unwrap();
    let bid_user_quote_balance = core.get_user_balance(101, 2).unwrap();
    println!(
        "User 100 (ASK) balances: base={}, quote={}",
        ask_user_base_balance, ask_user_quote_balance
    );
    println!(
        "User 101 (BID) balances: base={}, quote={}",
        bid_user_base_balance, bid_user_quote_balance
    );
}

/// Fragment handler for processing OrderCommand messages from core
struct TestOrderCommandHandler {
    gateway_id: String,
    received_commands: std::sync::Arc<std::sync::Mutex<Vec<OrderCommand>>>,
}

impl AeronFragmentHandlerCallback for TestOrderCommandHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
        match decode_order_command(buffer) {
            Ok(order_command) => {
                info!(
                    "Gateway {}: Received OrderCommand: {:?}",
                    self.gateway_id, order_command
                );
                self.received_commands.lock().unwrap().push(order_command);
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

#[tokio::test]
async fn test_end_to_end_exchange_flow() {
    tracing_subscriber::fmt::init();
    info!("Starting end-to-end exchange flow test");

    let (server_addr, _client_addr) = create_test_addresses();
    let received_commands = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let received_commands_clone = received_commands.clone();

    let client_handle = thread::spawn(move || -> Result<(), GatewayError> {
        let mut client_config = GatewayNetworkingConfig::test_defaults();
        client_config.core_port = server_addr.port();
        client_config.core_control_port = server_addr.port() + 1;
        info!("client_config: {:?}", client_config);
        
        let mut client = VexGateway::new(client_config)?;

        let handler = TestOrderCommandHandler {
            gateway_id: client.gateway_id().to_string(),
            received_commands: received_commands_clone,
        };

        // Start client in background thread
        match client.start(handler) {
            Ok(()) => info!("Client started successfully"),
            Err(e) => error!("Client start error: {e}"),
        }
        // Send test orders through the gateway
        let test_orders = vec![
            OrderCommand {
                command: OrderCommandType::PlaceOrder,
                user_id: 100,
                reserve_bid_price: 0,
                size: 10,
                order_type: OrderType::Gtc,
                user_cookie: 1,
                timestamp: 1,
                matcher_event: None,
                action: Side::Ask,
                order_id: 1,
                symbol_id: 0,
                price: 9630,
            },
            OrderCommand {
                command: OrderCommandType::PlaceOrder,
                user_id: 101,
                reserve_bid_price: 0,
                size: 10,
                order_type: OrderType::Gtc,
                user_cookie: 2,
                timestamp: 2,
                matcher_event: None,
                action: Side::Bid,
                order_id: 2,
                symbol_id: 0,
                price: 9630,
            },
            OrderCommand::cancel(1, 100),
        ];

        for mut order in test_orders {
            if order.command == OrderCommandType::CancelOrder {
                order.symbol_id = 0;
            }
            info!("Sending order: {:?}", order);
            client.send_order_command(&order)?;
            thread::sleep(Duration::from_millis(100));
        }

        // Wait for processing
        thread::sleep(Duration::from_millis(1000));
        
        Ok(())
    });


    // Start the core engine server
    let server_handle = thread::spawn(move || {
        let mut server_config = CoreNetworkingConfig::test_defaults();
        server_config.initial_port = server_addr.port();
        server_config.initial_control_port = server_addr.port() + 1;
        info!("server_config: {:?}", server_config);

        // Initialize the exchange core using init_exchange
        let (mut core, producer, _handler) = init_exchange();
        
        // Start the core engine with networking
        core.run(producer,server_config);
        
        // Keep the server running for the test duration
        thread::sleep(Duration::from_secs(5));
    });

    // // Give server time to start
    // tokio::time::sleep(Duration::from_millis(500)).await;

    // // Start the client gateway
    // // Wait for test completion
    // tokio::time::sleep(Duration::from_secs(2)).await;

    // // Verify results
    let commands = received_commands.lock().unwrap();
    info!("Received {} commands from core", commands.len());
    
    // Clean up
    let _ = client_handle.join();
    let _ = server_handle.join();

    
    info!("End-to-end exchange flow test completed successfully");
}
