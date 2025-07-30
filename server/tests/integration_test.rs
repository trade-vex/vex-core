use common::cmd::{OrderCommand, OrderCommandType, decode_order_command};
use common::model::enums::{Side, OrderType};
use server::init_exchange;

use vex_networking::client::{GatewayError, VexGateway};
use rusteron_client::{AeronFragmentHandlerCallback, AeronHeader, find_unused_udp_port};
use std::time::Duration;
use std::{net::SocketAddr, thread};
use tracing::{error, info};
use vex_config::{CoreNetworkingConfig, GatewayNetworkingConfig};

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
                command: OrderCommandType::PlaceLimitOrder,
                user_id: 100,
                reserve_bid_price: 0,
                size: 10,
                order_type: OrderType::Gtc,
                timestamp: 1,
                matcher_event: None,
                side: Side::Ask,
                order_id: 1,
                symbol_id: 0,
                price: 9630,
            },
            OrderCommand {
                command: OrderCommandType::PlaceLimitOrder,
                user_id: 101,
                reserve_bid_price: 0,
                size: 10,
                order_type: OrderType::Gtc,
                timestamp: 2,
                matcher_event: None,
                side: Side::Bid,
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
        use common::model::symbol_specification::TestConstants;
        let mut symbol_specs = hashbrown::HashMap::new();
        symbol_specs.insert(0, TestConstants::symbol_spec_eth_xbt());
        let (mut core, producer, _handler) = init_exchange(symbol_specs);

        // Start the core engine with networking
        core.run(producer, server_config);

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

#[test]
fn end_to_end_test_no_aeron() {
    use common::cmd::{OrderCommand, OrderCommandType};
    use common::model::enums::{OrderType, Side};
    use common::model::symbol_specification::TestConstants;
    use disruptor::Producer;
    use hashbrown::HashMap;
    use std::thread;
    use std::time::Duration;
    use tracing::info;

    // Initialize logging for the test
    tracing_subscriber::fmt::init();
    info!("Starting end-to-end test without Aeron networking");

    // Create symbol specifications
    let mut symbol_specs = HashMap::new();
    symbol_specs.insert(0, TestConstants::symbol_spec_eth_xbt());

    // Initialize the exchange core using init_exchange
    let (_core_engine, mut producer, events_handler) = init_exchange(symbol_specs);

    // Create test orders
    let test_orders = vec![
        // Place a limit buy order
        OrderCommand {
            command: OrderCommandType::PlaceLimitOrder,
            order_id: 1,
            symbol_id: 0,
            user_id: 100,
            price: 9630, // 9630 satoshi per szabo
            reserve_bid_price: 0,
            size: 100_000, // 1 lot (0.1 ETH)
            side: Side::Bid,
            order_type: OrderType::Gtc,
            timestamp: 1,
            matcher_event: None,
        },
        // Place a limit sell order at the same price (should match)
        OrderCommand {
            command: OrderCommandType::PlaceLimitOrder,
            order_id: 2,
            symbol_id: 0,
            user_id: 101,
            price: 9630, // Same price as buy order
            reserve_bid_price: 0,
            size: 50_000, // 0.5 lot (0.05 ETH) - partial fill
            side: Side::Ask,
            order_type: OrderType::Gtc,
            timestamp: 2,
            matcher_event: None,
        },
        // Place another sell order at a higher price (should not match)
        OrderCommand {
            command: OrderCommandType::PlaceLimitOrder,
            order_id: 3,
            symbol_id: 0,
            user_id: 102,
            price: 9640, // Higher price than buy order
            reserve_bid_price: 0,
            size: 100_000, // 1 lot
            side: Side::Ask,
            order_type: OrderType::Gtc,
            timestamp: 3,
            matcher_event: None,
        },
        // Cancel the first buy order
        OrderCommand::cancel(1, 100),
    ];

    // Publish orders to the exchange
    info!("Publishing {} orders to the exchange", test_orders.len());
    for (i, order) in test_orders.iter().enumerate() {
        info!("Publishing order {}: {:?}", i + 1, order);

        // Publish the order to the disruptor
        producer.publish(|event| {
            *event = order.clone();
        });

        info!("Published order {}", i + 1);

        // Small delay between orders to allow processing
        thread::sleep(Duration::from_millis(10));
    }

    // Wait for all orders to be processed
    info!("Waiting for order processing to complete...");
    thread::sleep(Duration::from_millis(500));

    // Verify the results
    let events = events_handler.events.lock().unwrap();
    info!("Received {} events from the exchange", events.len());

    // Print all events for debugging
    for (i, event) in events.iter().enumerate() {
        info!("Event {}: {:?}", i + 1, event);
    }

    // Verify that we received trade events
    assert!(!events.is_empty(), "Expected to receive trade events");

    // Check for specific trade events
    let trade_events: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == common::model::enums::MatcherEventType::Trade)
        .collect();

    info!("Found {} trade events", trade_events.len());

    // Verify that we have at least one trade (from the matching orders)
    assert!(
        trade_events.len() >= 1,
        "Expected at least one trade event from matching orders"
    );

    // Verify the trade details
    if let Some(trade) = trade_events.first() {
        assert_eq!(trade.symbol_id, 0, "Trade should be for symbol 0");
        assert_eq!(trade.price, 9630, "Trade price should be 9630");
        assert_eq!(
            trade.size, 50_000,
            "Trade size should be 50,000 (partial fill)"
        );
        assert_eq!(
            trade.active_order_user_id, 101,
            "Active order user should be 101 (seller)"
        );
        assert_eq!(trade.maker_user_id, 100, "Maker user should be 100 (buyer)");
        assert!(
            trade.active_order_completed,
            "Seller order should be completed (full fill)"
        );
        assert!(
            !trade.matched_order_completed,
            "Buyer order should not be completed (partial fill)"
        );
    }

    info!("End-to-end test completed successfully!");
}
