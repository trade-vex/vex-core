use common::OrderCommandType;
use common::{OrderCommand, decode_order_command};
use common::{Side, TimeInForce};
use disruptor::{BusySpin, ProcessorSettings, build_multi_producer};
use rusteron_archive::{AeronFragmentHandlerCallback, AeronHeader, find_unused_udp_port};
use rusteron_media_driver::{AeronDriver, AeronDriverContext};
use std::ffi::CString;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use std::{net::SocketAddr, thread};
use tracing::{error, info};
use vex_config::{CoreNetworkingConfig, GatewayNetworkingConfig};
use vex_networking::client::{GatewayError, VexGateway};
use vex_networking::server::{Publications, VexCoreServer};

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

/// Both sides share it, so the handshake runs with authentication enabled.
const GATEWAY_AUTHENTICATION_KEY: &str = "123456789";

#[test_log::test]
#[ignore = "This test needs media drivers running, Tests will be added in x-tasks"]
fn test_client_server_communication() {
    // This test demonstrates the actual usage of run() methods
    let (server_addr, _client_addr) = create_test_addresses();

    // start aeron media driver in background, and create context directories for client and server in test-data directory
    let client_handle = thread::spawn(move || -> Result<(), GatewayError> {
        let mut client_config = GatewayNetworkingConfig::test_defaults();
        client_config.core_port = server_addr.port();
        client_config.core_control_port = server_addr.port() + 1;
        client_config.gateway_authentication_key = GATEWAY_AUTHENTICATION_KEY.into();
        info!("client_config: {:?}", client_config);
        let mut client = VexGateway::new(client_config)?;

        let handler = OrderCommandHandler {
            gateway_id: client.gateway_id().to_string(),
        };

        let publisher = client.start(handler).expect("Failed to start VexGateway");

        let mut order_command = OrderCommand {
            command: OrderCommandType::PlaceOrder,
            user_id: 1,
            size: 100,
            timestamp: 1,
            side: Side::Ask,
            order_id: 1,
            market_id: 3124,
            price: 150,
            time_in_force: TimeInForce::Gtc,
            status: common::Status::Processing,
            events: None,
            balance: [common::UserBalance::default(); 2],
            client_order_id: 0,
            l2_data: None,
            route_gateway_id: 0,
            original_size: 0,
        };
        for i in 0..10 {
            order_command.order_id = i;
            publisher.send_order_command(&order_command)?;
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    });

    let _ = thread::spawn(move || {
        let mut server_config = CoreNetworkingConfig::test_defaults();
        server_config.initial_port = server_addr.port();
        server_config.initial_control_port = server_addr.port() + 1;
        info!("server_config: {:?}", server_config);
        let producer = build_multi_producer(
            1024,
            || OrderCommand::place_order(TimeInForce::Gtc, 1, 23, 32, Side::Ask, 1, 10),
            BusySpin,
        )
        .pin_at_core(1)
        .handle_events_with({
            move |cell: &std::cell::UnsafeCell<OrderCommand>, _, _| {
                // SAFETY: This test handler is the sole handler in its barrier group, so
                // creating an exclusive reference cannot alias another handler's reference.
                let cmd = unsafe { &mut *cell.get() };
                info!("Server received OrderCommand Core 1: {:?}", cmd);
            }
        })
        .and_then()
        .pin_at_core(2)
        .handle_events_with({
            move |cell: &std::cell::UnsafeCell<OrderCommand>, _, _| {
                // SAFETY: This test handler is the sole handler in its barrier group, so
                // creating an exclusive reference cannot alias another handler's reference.
                let cmd = unsafe { &mut *cell.get() };
                info!("Server processing OrderCommand Core 2: {:?}", cmd);
            }
        })
        .build();
        let publications = Arc::new(Publications::new());
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let mut server = VexCoreServer::new(
            server_config,
            GATEWAY_AUTHENTICATION_KEY.into(),
            producer,
            publications,
            false,
            shutdown_flag,
        )
        .unwrap();
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

/// The core answers each HELLO exactly once over its MDC handshake publication. Once one gateway
/// is subscribed, that publication counts as connected, so a later gateway that sent HELLO before
/// its own reply subscription had joined lost its ACCEPT or REJECT and timed out instead.
#[test_log::test]
#[ignore = "Starts an embedded Aeron media driver and binds UDP ports"]
fn test_every_gateway_receives_its_handshake_reply() {
    let aeron_dir = format!("/dev/shm/vex-handshake-test-{}", std::process::id());
    let driver_context = AeronDriverContext::new().unwrap();
    driver_context
        .set_dir(&CString::new(aeron_dir.clone()).unwrap())
        .unwrap();
    driver_context.set_dir_delete_on_start(true).unwrap();
    driver_context.set_dir_delete_on_shutdown(true).unwrap();
    let (driver_stop, driver) = AeronDriver::launch_embedded(driver_context, false);

    let core_port = find_unused_udp_port(41000).unwrap();
    let mut server_config = CoreNetworkingConfig::test_defaults();
    server_config.context_dir = aeron_dir.clone();
    server_config.request_control_channel.clear(); // no archive
    server_config.initial_port = core_port;
    server_config.initial_control_port = core_port + 1;
    server_config.base_gateway_port = find_unused_udp_port(51000).unwrap();
    let server_shutdown = Arc::new(AtomicBool::new(false));
    let server = thread::spawn({
        let shutdown = Arc::clone(&server_shutdown);
        move || {
            let producer = build_multi_producer(1024, OrderCommand::default, BusySpin)
                .handle_events_with(|_, _, _| {})
                .build();
            VexCoreServer::new(
                server_config,
                GATEWAY_AUTHENTICATION_KEY.into(),
                producer,
                Arc::new(Publications::new()),
                false,
                shutdown,
            )
            .unwrap()
            .start()
        }
    });

    let start_gateway = |gateway_id: u8, key: &str| {
        let mut config = GatewayNetworkingConfig::test_defaults();
        config.context_dir = aeron_dir.clone();
        config.core_port = core_port;
        config.core_control_port = core_port + 1;
        config.gateway_id = gateway_id;
        config.gateway_authentication_key = key.into();
        let mut gateway = VexGateway::new(config).unwrap();
        let handler = OrderCommandHandler {
            gateway_id: gateway_id.to_string(),
        };
        let started = gateway.start(handler);
        (gateway, started)
    };

    // Accepted gateways stay connected, so every later handshake races an already-connected
    // publication.
    let mut gateways = Vec::new();
    for gateway_id in (1..=6).step_by(2) {
        let (gateway, started) = start_gateway(gateway_id, GATEWAY_AUTHENTICATION_KEY);
        let publisher = started.unwrap_or_else(|e| panic!("gateway-{gateway_id}: {e}"));
        gateways.push((gateway, publisher));

        let (rejected, started) = start_gateway(gateway_id + 1, "987654321");
        let error = started.err().expect("a wrong key must be rejected");
        assert!(
            error.to_string().contains("Handshake rejected"),
            "gateway-{}: {error}",
            gateway_id + 1
        );
        drop(rejected);
    }

    // Close every Aeron client before the driver goes away.
    for (mut gateway, publisher) in gateways {
        gateway.shutdown().unwrap();
        drop(publisher);
    }
    server_shutdown.store(true, Ordering::Release);
    let _ = server.join().unwrap();
    driver_stop.store(true, Ordering::Release);
    let _ = driver.join();
}
