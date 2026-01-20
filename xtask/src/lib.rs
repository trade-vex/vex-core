//! VEX-CORE Integration Test Suite
//!
//! This crate provides a comprehensive, production-grade test framework
//! for validating the VEX-CORE trading system end-to-end.
//!
//! ## Architecture
//!
//! The test suite is organized into several modules:
//!
//! - `test_framework`: Core test infrastructure (TestContext, TestClient, RedisVerifier)
//! - `builders`: Fluent APIs for creating test data (OrderBuilder, ScenarioBuilder)
//! - `verifiers`: Assertion utilities for validating system state
//! - `scenarios`: Pre-built test scenarios organized by functionality
//! - `fixtures`: Reusable test data and configurations
//!
//! ## Quick Start
//!
//! ```no_run
//! use xtask::test_framework::TestContext;
//! use xtask::builders::OrderBuilder;
//! use common::Side;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create test context
//! let mut ctx = TestContext::new().await?;
//!
//! // Fund test user
//! ctx.fund_user(1, 1000000, 1).await?;  // 1M USD
//!
//! // Place an order (defaults to GTC time-in-force)
//! let order = OrderBuilder::place_limit()
//!     .user(1)
//!     .price(50000)
//!     .size(10)
//!     .side(Side::Bid)
//!     .market_id(ctx.market_id)
//!     .build();
//!
//! let response = ctx.execute_command(order)?;
//!
//! // Verify response
//! assert_eq!(response.status, common::Status::Placed);
//!
//! // Cleanup
//! ctx.cleanup().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Test Verification Strategy
//!
//! The test suite uses a two-phase verification approach:
//!
//! ### Phase 1: Response Verification (Immediate)
//! Verifies fields in the returned OrderCommand:
//! - `status` - Order execution status
//! - `order_id` - Assigned by journaling processor
//! - `timestamp` - Set by journaling processor
//! - `size` - Remaining size after matching
//! - `price` - May be adjusted for market orders
//!
//! ### Phase 2: Redis Verification (Eventual)
//! Verifies state published by the Events Handler:
//! - Balance events in `user:{user_id}:asset:{asset_id}:balance`
//! - Order events in `order:{order_id}`
//! - Trade events in `market:{market_id}:trades`
//! - Orderbook snapshots in `orderbook:market:{market_id}`
//!
//! ## Market Configuration
//!
//! Default test market:
//! - Market ID: 65538 (0x00010002)
//! - Base Asset: 2 (BTC)
//! - Quote Asset: 1 (USD)
//! - Maker Fee: 10bp (0.1%)
//! - Taker Fee: 20bp (0.2%)

pub mod builders;
pub mod fixtures;
pub mod scenarios;
pub mod test_framework;
pub mod verifiers;

// Re-export commonly used types
pub use builders::OrderBuilder;
pub use test_framework::{TestContext, types::*};
pub use verifiers::{BalanceVerifier, OrderbookVerifier, ResponseVerifier, TradeVerifier};
