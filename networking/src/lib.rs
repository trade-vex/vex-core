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
//! pub const ORDER_STREAM_ID: i32 = 1001;
//! let mut publisher = AeronPublisher::new("/aeron/dir")?;
//! publisher.add_publication("aeron:ipc", ORDER_STREAM_ID)?;
//! 
//! // Publish order command
//! publisher.send(&order_cmd_bytes, "aeron:ipc", ORDER_STREAM_ID)?;
//! ```
//! 
//! #### Core Engine Integration
//! ```ignore
//! // Core subscribes to orders and publishes market data
//! 
//! pub const ORDER_STREAM_ID: i32 = 1001;
//! let mut subscriber = AeronSubscriber::new("/aeron/dir", assembler)?;
//! subscriber.add_subscription("aeron:ipc", ORDER_STREAM_ID)?;
//! 
//! // Start processing loop
//! subscriber.start();
//! ```
//! 
pub mod subscriber;
pub mod publisher;
pub mod client;

// Re-export main types for convenience
pub use subscriber::{AeronSubscriber, SubscriberError};
