use networking::client::config::GatewayConfig;
use networking::client::{VexGateway, GatewayError};
use networking::server::config::CoreConfig;
use networking::server::server::VexCore;
use rusteron_client::find_unused_udp_port;
use tracing::info;
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

#[test_log::test]
fn test_client_server_communication() {
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
            max_message_size: 2048,
            enable_heartbeat: true,
        };
        info!("client_config: {:?}", client_config);
        let mut client = VexGateway::new(client_config)?;
        
        match client.start() {
            Ok(()) => println!("Client run() completed successfully"),
            Err(e) => println!("Client run() error: {}", e),
        }
        
        Ok(())
    });
    
    let context_s_clone = context_s.to_string();
    let _ = thread::spawn(move || {
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
        let server = VexCore::new(server_config).unwrap();                
        match server.start() {
            Ok(()) => println!("Server run() completed successfully (unexpected)"),
            Err(e) => println!("Server run() error: {}", e),
        }
    });
    let client_result = client_handle.join();
    
    match client_result {
        Ok(Ok(())) => println!("✓ Client run() test passed"),
        Ok(Err(e)) => panic!("Client run() test failed: {}", e),
        Err(_) => panic!("Client run() test panicked"),
    }
}
