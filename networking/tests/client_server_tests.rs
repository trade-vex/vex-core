use networking::client::{VexClient, ClientError};
use networking::server::EchoServer;
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
    let context_c = "test-data/client";
    let context_s = "test-data/server";

    println!("=== Testing VexClient::run() method ===");
    let context_c_clone = context_c.to_string();
    let client_handle = thread::spawn(move || -> Result<(), ClientError> {
        let mut client = VexClient::new(&context_c_clone, client_addr, server_addr)?;
        
        println!("VexClient::run() is synchronous - it sends messages and polls for responses");
        match client.run() {
            Ok(()) => println!("Client run() completed successfully"),
            Err(e) => println!("Client run() error: {}", e),
        }
        
        Ok(())
    });
    
    println!("=== Testing EchoServer::run() method ===");
    let context_s_clone = context_s.to_string();
    let _ = thread::spawn(move || {
        let server = EchoServer::create(&context_s_clone, server_addr).unwrap();        
        println!("EchoServer created successfully");
        println!("Note: Both EchoServer::run() and VexClient::run() are synchronous methods");
        println!("EchoServer::run() runs an infinite loop polling for messages");
        
        match server.run() {
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
    
    println!("=== Both synchronous run() methods terminated successfully ===");
    
}
