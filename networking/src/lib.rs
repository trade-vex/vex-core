//! # VEX Networking Crate
//!
//! ## Overview
//!
//! The `networking` crate serves as the high-performance communication layer for the VEX
//! trading system, facilitating ultra-low-latency message exchange between the VEX client gateway and
//! the VEX core. This crate implements Aeron-based messaging infrastructure.
//!
//! ## Architecture Context
//!
//! The VEX system operates as a distributed trading platform with the following key components:
//!
//! ```ignore
//! ┌─────────────────┐    Aeron UDP        ┌─────────────────┐
//! │   VEX Gateway   │ ◄─────────────────► │   VEX Core      │
//! │   (Client API)  │                     │   (Matching     │
//! │                 │                     │    Engine)      │
//! └─────────────────┘                     └─────────────────┘
//!         │                                        │
//!         │                                        │
//!         ▼                                        ▼
//! ┌─────────────────┐                     ┌─────────────────┐
//! │   Market Data   │                     │   Order Books   │
//! │   Feed          │                     │ Risk Engine, etc│  
//! └─────────────────┘                     └─────────────────┘
//! ```
//!
//! #### Gateway Integration
//!
//! ```ignore
//! // Gateway receives client orders and publishes to core
//! # VEX Networking Crate
//!
//! ## Overview
//!
//! The `networking` crate provides high-performance Aeron-based messaging for ultra-low-latency communication between the VEX Gateway and VEX Core.
//!
//! ## Example Usage
//!
//! ### Gateway (Client) Side
//!
//! ```rust
//! use vex_networking::client::{VexGateway, GatewayNetworkingConfig};
//! use common::cmd::OrderCommand;
//!
//! // Create a networking config (see GatewayNetworkingConfig::test_defaults for testing)
//! let mut config = GatewayNetworkingConfig::test_defaults();
//!
//! // Initialize the gateway
//! let mut gateway = VexGateway::new(config)?;
//!
//! // Implement a handler for incoming messages (see AeronFragmentHandlerCallback)
//! let handler = MyOrderHandler { /* ... */ };
//!
//! // Start the gateway event loop
//! gateway.start(handler)?;
//!
//! // Send an order command to the core
//! let order_cmd = OrderCommand::default();
//! gateway.send_order_command(&order_cmd)?;
//! ```
//!
//! ### Core (Server) Side
//!
//! ```ignore, rust
//! use vex_networking::server::{VexCoreServer, CoreNetworkingConfig};
//!
//! // Create a networking config (see CoreNetworkingConfig::test_defaults for testing)
//! let mut config = CoreNetworkingConfig::test_defaults();
//!
//! // Initialize the core server
//! let mut server = VexCoreServer::new(config)?;
//!
//! // Start the server event loop
//! server.start()?;
//! ```
//!
//! ## Integration Testing
//!
//! In integration tests, you can use `GatewayNetworkingConfig::test_defaults()` and `CoreNetworkingConfig::test_defaults()` to quickly set up client and server endpoints. The crate provides helpers for finding unused UDP ports and for handling Aeron fragments.
//!
//! ## Key Concepts
//!
//! - **VexGateway**: Client-side networking interface for sending order commands and receiving market data.
//! - **VexCoreServer**: Server-side networking interface for receiving orders and publishing market data.
//! - **AeronFragmentHandlerCallback**: Trait for handling incoming Aeron messages.
//! - **OrderCommand**: The main message that goes into diruptor in the vex-core.
//!

pub mod client;
pub mod server;
pub mod utils;
