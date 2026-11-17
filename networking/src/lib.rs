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
//! ```text
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

pub mod client;
pub mod server;
pub mod utils;
