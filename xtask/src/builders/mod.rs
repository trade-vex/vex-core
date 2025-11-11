//! Builder patterns for creating test data
//!
//! This module provides fluent APIs for constructing OrderCommands
//! and test scenarios with type safety.

pub mod order_builder;

pub use order_builder::OrderBuilder;
