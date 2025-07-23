use common::cmd::{OrderCommand, OrderCommandType};
use common::model::enums::{OrderAction, OrderType};
use networking::client::config::GatewayConfig;
use networking::client::{VexGateway, GatewayError};
use networking::server::config::CoreConfig;
use networking::server::server::VexCoreServer;
use rusteron_client::find_unused_udp_port;
use tracing::info;
use std::time::Duration;
use std::{
    net::SocketAddr,
    thread,
};

/// Helper to create test addresses
fn create_test_addresses() -> (SocketAddr, SocketAddr) {
    let server_port = find_unused_udp_port(40300).unwrap();
    let client_port = find_unused_udp_port(40350).unwrap();
    
    let server_addr = format!("127.0.0.1:{}", server_port).parse().unwrap();
    let client_addr = format!("127.0.0.1:{}", client_port).parse().unwrap();
    info!("server_addr: {}", server_addr);
    info!("client_addr: {}", client_addr);
    
    (server_addr, client_addr)
}

#[tokio::test]
#[test_log::test]
async fn test_client_server_communication() {
    // This test demonstrates the actual usage of run() methods
    let (server_addr, client_addr) = create_test_addresses();

    // start aeron media driver in background, and create context directories for client and server in test-data directory
    let mut current_dir = std::env::current_dir().unwrap();
    current_dir.pop();
    let context_path = current_dir.join("test-data").join("server");
    let context_s = context_path.to_str().unwrap();
    let context_path = current_dir.join("test-data").join("client");
    let context_c = context_path.to_str().unwrap();

    let context_c_clone = context_c.to_string();
    let client_handle = thread::spawn(move || -> Result<(), GatewayError> {
        let client_config = GatewayConfig {
            context_dir: context_c_clone,
            local_address: client_addr.ip().to_string(),
            core_address: server_addr.ip().to_string(),
            core_port: server_addr.port(),
            core_control_port: server_addr.port() + 1,
            gateway_id: "gateway-1".to_string(),
            max_message_size: 67,
            enable_heartbeat: true,
        };
        info!("client_config: {:?}", client_config);
        let mut client = VexGateway::new(client_config)?;
        
        match client.start() {
            Ok(()) => println!("Client run() completed successfully"),
            Err(e) => println!("Client run() error: {}", e),
        }

        let mut order_command = OrderCommand {
            command: OrderCommandType::PlaceOrder,
            uid: 1,
            reserve_bid_price: 150,
            size: 100,
            order_type: OrderType::Gtc,
            user_cookie: 1,
            timestamp: 1,
            matcher_event: None,
            action: OrderAction::Ask,
            order_id: 1,
            symbol: 3124,
            price: 150,
        };

        for i in 0..1000 {
            order_command.order_id = i;
            order_command.uid = i;
            client.send_order_command(&order_command)?;
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    });
    info!("client_handle spawned");
    let context_s_clone = context_s.to_string();
    let server_handle = tokio::spawn(async move {
        let server_config = CoreConfig {
            context_dir: context_s_clone,
            local_address: server_addr.ip().to_string(),
            initial_port: server_addr.port(),
            initial_control_port: server_addr.port() + 1,
            base_gateway_port: 40350,
            max_gateways: 100,
            max_connections_per_address: 10,
            reserved_session_id_low: 0,
            reserved_session_id_high: 2147483647,
            enable_authentication: true,
            enable_heartbeat: true,
            gateway_timeout_seconds: 30,
            core_id: "core-1".to_string(),
        };
        info!("server_config: {:?}", server_config);
        let server = VexCoreServer::new(server_config).unwrap();
        info!("server spawned");
        server.start().await
    });
    let (client_result, server_result) = tokio::join!(
        tokio::task::spawn_blocking(|| client_handle.join()),
        server_handle
    );
    
    match client_result {
        Ok(Ok(x)) => println!("✓ Client run() test passed: {:?}", x),
        Ok(Err(e)) => panic!("Client run() test failed: {:?}", e),
        Err(_) => panic!("Client run() test panicked"),
    }
}

#[tokio::test]
#[test_log::test]
async fn test_multiple_gateway_clients() {
    const NUM_CLIENTS: usize = 3;
    const ORDERS_PER_CLIENT: u64 = 1;
    
    // Create server address
    let server_port = find_unused_udp_port(41000).unwrap();
    let server_addr: SocketAddr = format!("127.0.0.1:{}", server_port).parse().unwrap();
    info!("Server address: {}", server_addr);
    
    // Setup server context directory
    let mut current_dir = std::env::current_dir().unwrap();
    current_dir.pop();
    let server_context_path = current_dir.join("test-data").join("server");
    let server_context = server_context_path.to_str().unwrap().to_string();
    
    // Spawn server
    let server_handle = tokio::spawn(async move {
        let server_config = CoreConfig {
            context_dir: server_context,
            local_address: server_addr.ip().to_string(),
            initial_port: server_addr.port(),
            initial_control_port: server_addr.port() + 1,
            base_gateway_port: 41100,
            max_gateways: 100,
            max_connections_per_address: 10,
            reserved_session_id_low: 0,
            reserved_session_id_high: 2147483647,
            enable_authentication: true,
            enable_heartbeat: true,
            gateway_timeout_seconds: 30,
            core_id: "multi-core-1".to_string(),
        };
        info!("Server config: {:?}", server_config);
        let server = VexCoreServer::new(server_config).unwrap();
        info!("Multiple client test server spawned");
        server.start().await
    });
    
    // Create multiple client handles
    let mut client_handles = Vec::new();
    
    for client_id in 0..NUM_CLIENTS {
        let client_addr_port = find_unused_udp_port(41200 + client_id as u16 * 10).unwrap();
        let client_addr: SocketAddr = format!("127.0.0.1:{}", client_addr_port).parse().unwrap();
        
        // Create unique context directory for each client
        let client_context_path = current_dir.join("test-data").join(format!("client-{}", client_id));
        let client_context = client_context_path.to_str().unwrap().to_string();
        
        let server_addr_clone = server_addr;
        let handle = thread::spawn(move || -> Result<(), GatewayError> {
            let client_config = GatewayConfig {
                context_dir: client_context,
                local_address: client_addr.ip().to_string(),
                core_address: server_addr_clone.ip().to_string(),
                core_port: server_addr_clone.port(),
                core_control_port: server_addr_clone.port() + 1,
                gateway_id: format!("gateway-{}", client_id),
                max_message_size: 67,
                enable_heartbeat: true,
            };
            
            info!("Client {} config: {:?}", client_id, client_config);
            let mut client = VexGateway::new(client_config)?;
            
            match client.start() {
                Ok(()) => info!("Client {} started successfully", client_id),
                Err(e) => {
                    info!("Client {} start error: {}", client_id, e);
                    return Err(e);
                }
            }
            
            // Each client sends orders with unique IDs
            let mut order_command = OrderCommand {
                command: OrderCommandType::PlaceOrder,
                uid: 0,
                reserve_bid_price: 150 + (client_id as i64 * 10), // Different prices per client
                size: 100,
                order_type: OrderType::Gtc,
                user_cookie: client_id as i32,
                timestamp: 1,
                matcher_event: None,
                action: if client_id % 2 == 0 { OrderAction::Ask } else { OrderAction::Bid }, // Alternate between Ask/Bid
                order_id: 0,
                symbol: 3124,
                price: 150 + (client_id as i64 * 10),
            };
            
            for i in 0..ORDERS_PER_CLIENT {
                // Create unique order IDs across all clients
                let unique_order_id = (client_id as i64 * 10000) + i as i64;
                let unique_uid = (client_id as i64 * 10000) + i as i64;
                
                order_command.order_id = unique_order_id;
                order_command.uid = unique_uid;
                
                client.send_order_command(&order_command)?;
                std::thread::sleep(Duration::from_millis(5)); // Smaller delay for faster test
            }
            
            info!("Client {} sent {} orders successfully", client_id, ORDERS_PER_CLIENT);
            Ok(())
        });
        
        client_handles.push(handle);
    }
    
    info!("All {} client handles spawned", NUM_CLIENTS);
    
    // Wait for all clients to complete and collect results
    let client_results_handle = tokio::task::spawn_blocking(move || {
        let mut results = Vec::new();
        for (client_id, handle) in client_handles.into_iter().enumerate() {
            match handle.join() {
                Ok(client_result) => {
                    results.push((client_id, client_result));
                }
                Err(_) => {
                    results.push((client_id, Err(GatewayError::CoreError("Thread panicked".to_string()))));
                }
            }
        }
        results
    });
    
    // Run clients and server concurrently
    let (client_results, server_result) = tokio::join!(client_results_handle, server_handle);
    
    // Verify all clients completed successfully
    match client_results {
        Ok(results) => {
            let mut success_count = 0;
            for (client_id, result) in results {
                match result {
                    Ok(()) => {
                        info!("✓ Client {} completed successfully", client_id);
                        success_count += 1;
                    }
                    Err(e) => {
                        panic!("✗ Client {} failed: {:?}", client_id, e);
                    }
                }
            }
            
            assert_eq!(success_count, NUM_CLIENTS, "All clients should complete successfully");
            info!("✓ Multiple gateway clients test passed: {}/{} clients successful", success_count, NUM_CLIENTS);
        }
        Err(e) => {
            panic!("Failed to collect client results: {:?}", e);
        }
    }
}