//! Core types for the VEX-CORE integration test framework
//!
//! This module defines all the data structures used for test execution,
//! verification, and result reporting.

use hashbrown::HashMap;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

/// Test framework error types
#[derive(Debug, Error)]
pub enum TestError {
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Networking error: {0}")]
    Network(String),

    #[error("Timeout error: operation took longer than {timeout:?}")]
    Timeout { timeout: Duration },

    #[error("Assertion failed: {message}")]
    Assertion { message: String },

    #[error("Verification failed: {message}")]
    Verification { message: String },

    #[error("Invalid state: {message}")]
    InvalidState { message: String },

    #[error("Configuration error: {message}")]
    Configuration { message: String },

    #[error("Parse error: {message}")]
    Parse { message: String },

    #[error("Channel receive error: {0}")]
    ChannelRecv(#[from] std::sync::mpsc::RecvTimeoutError),

    #[error("Other error: {0}")]
    Other(#[from] anyhow::Error),
}

/// Test result type
pub type TestResult<T> = Result<T, TestError>;

/// Redis balance representation (from Redis HASH)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedisBalance {
    pub user_id: u64,
    pub asset_id: u16,
    pub available: u64,
    pub locked: u64,
    pub total: u64,
    pub timestamp: u64,
}

impl RedisBalance {
    /// Verify balance invariant: available + locked = total
    pub fn verify_invariant(&self) -> TestResult<()> {
        if self.available + self.locked != self.total {
            return Err(TestError::Assertion {
                message: format!(
                    "Balance invariant violated for user {} asset {}: available({}) + locked({}) != total({})",
                    self.user_id, self.asset_id, self.available, self.locked, self.total
                ),
            });
        }
        Ok(())
    }
}

/// Redis order representation (from Redis HASH)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedisOrder {
    pub order_id: u64,
    pub user_id: u64,
    pub price: u64,
    pub size: u64,
    pub side: String, // "Bid" or "Ask"
    pub timestamp: u64,
    pub market_id: u32,
    pub status: String, // "placed", "cancelled", etc.
}

/// Redis trade event representation (from Redis STREAM or ZSET)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedisTrade {
    pub trade_id: String,
    pub maker_user_id: u64,
    pub taker_user_id: u64,
    pub market_id: u32,
    pub price: u64,
    pub size: u64,
    pub maker_order_id: u64,
    pub taker_order_id: u64,
    pub timestamp: u64,
}

/// Redis orderbook snapshot representation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedisOrderbook {
    pub market_id: u32,
    pub bids: Vec<OrderbookLevel>,
    pub asks: Vec<OrderbookLevel>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderbookLevel {
    pub price: u64,
    pub size: u64,
}

/// User state tracker for test context
#[derive(Debug, Clone)]
pub struct UserState {
    pub user_id: u64,
    /// Tracks expected balances for verification
    pub expected_balances: HashMap<u16, Balance>,
    /// Tracks active orders
    pub active_orders: Vec<u64>,
}

impl UserState {
    pub fn new(user_id: u64) -> Self {
        Self {
            user_id,
            expected_balances: HashMap::new(),
            active_orders: Vec::new(),
        }
    }

    pub fn set_expected_balance(&mut self, asset_id: u16, balance: Balance) {
        self.expected_balances.insert(asset_id, balance);
    }

    pub fn add_active_order(&mut self, order_id: u64) {
        self.active_orders.push(order_id);
    }

    pub fn remove_active_order(&mut self, order_id: u64) {
        self.active_orders.retain(|&id| id != order_id);
    }
}

/// Balance expectation for verification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Balance {
    pub available: u64,
    pub locked: u64,
    pub total: u64,
}

impl Balance {
    pub fn new(available: u64, locked: u64) -> Self {
        Self {
            available,
            locked,
            total: available + locked,
        }
    }

    pub fn from_total(total: u64) -> Self {
        Self {
            available: total,
            locked: 0,
            total,
        }
    }
}

/// Trade matching criteria for verification
#[derive(Debug, Clone)]
pub struct TradeCriteria {
    pub market_id: Option<u32>,
    pub maker_user_id: Option<u64>,
    pub taker_user_id: Option<u64>,
    pub price: Option<u64>,
    pub size: Option<u64>,
    pub maker_order_id: Option<u64>,
    pub taker_order_id: Option<u64>,
}

impl TradeCriteria {
    pub fn new() -> Self {
        Self {
            market_id: None,
            maker_user_id: None,
            taker_user_id: None,
            price: None,
            size: None,
            maker_order_id: None,
            taker_order_id: None,
        }
    }

    pub fn market_id(mut self, market_id: u32) -> Self {
        self.market_id = Some(market_id);
        self
    }

    pub fn maker_user_id(mut self, user_id: u64) -> Self {
        self.maker_user_id = Some(user_id);
        self
    }

    pub fn taker_user_id(mut self, user_id: u64) -> Self {
        self.taker_user_id = Some(user_id);
        self
    }

    pub fn price(mut self, price: u64) -> Self {
        self.price = Some(price);
        self
    }

    pub fn size(mut self, size: u64) -> Self {
        self.size = Some(size);
        self
    }

    pub fn maker_order_id(mut self, order_id: u64) -> Self {
        self.maker_order_id = Some(order_id);
        self
    }

    pub fn taker_order_id(mut self, order_id: u64) -> Self {
        self.taker_order_id = Some(order_id);
        self
    }

    pub fn matches(&self, trade: &RedisTrade) -> bool {
        if let Some(market_id) = self.market_id
            && trade.market_id != market_id
        {
            return false;
        }
        if let Some(maker_user_id) = self.maker_user_id
            && trade.maker_user_id != maker_user_id
        {
            return false;
        }
        if let Some(taker_user_id) = self.taker_user_id
            && trade.taker_user_id != taker_user_id
        {
            return false;
        }
        if let Some(price) = self.price
            && trade.price != price
        {
            return false;
        }
        if let Some(size) = self.size
            && trade.size != size
        {
            return false;
        }
        if let Some(maker_order_id) = self.maker_order_id
            && trade.maker_order_id != maker_order_id
        {
            return false;
        }
        if let Some(taker_order_id) = self.taker_order_id
            && trade.taker_order_id != taker_order_id
        {
            return false;
        }
        true
    }
}

impl Default for TradeCriteria {
    fn default() -> Self {
        Self::new()
    }
}

/// Test configuration
#[derive(Debug, Clone)]
pub struct TestConfig {
    pub redis_host: String,
    pub redis_port: u16,
    pub market_id: u32,
    pub base_asset_id: u16,
    pub quote_asset_id: u16,
    pub default_timeout: Duration,
    pub redis_event_timeout: Duration,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            redis_host: "localhost".to_string(),
            redis_port: 6380,
            market_id: ((1u32 << 16) | 2u32), // quote=1 (USD), base=2 (BTC)
            base_asset_id: 2,
            quote_asset_id: 1,
            default_timeout: Duration::from_secs(5),
            redis_event_timeout: Duration::from_secs(60), // reset
        }
    }
}

/// Test scenario result
#[derive(Debug, Clone)]
pub struct ScenarioResult {
    pub name: String,
    pub success: bool,
    pub duration: Duration,
    pub error: Option<String>,
}

impl ScenarioResult {
    pub fn success(name: String, duration: Duration) -> Self {
        Self {
            name,
            success: true,
            duration,
            error: None,
        }
    }

    pub fn failure(name: String, duration: Duration, error: TestError) -> Self {
        Self {
            name,
            success: false,
            duration,
            error: Some(error.to_string()),
        }
    }
}

/// Test suite result summary
#[derive(Debug, Clone)]
pub struct TestSuiteResult {
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub duration: Duration,
    pub scenarios: Vec<ScenarioResult>,
}

impl TestSuiteResult {
    pub fn new() -> Self {
        Self {
            total: 0,
            passed: 0,
            failed: 0,
            duration: Duration::ZERO,
            scenarios: Vec::new(),
        }
    }

    pub fn add_result(&mut self, result: ScenarioResult) {
        self.total += 1;
        if result.success {
            self.passed += 1;
        } else {
            self.failed += 1;
        }
        self.duration += result.duration;
        self.scenarios.push(result);
    }

    pub fn is_success(&self) -> bool {
        self.failed == 0
    }
}

impl Default for TestSuiteResult {
    fn default() -> Self {
        Self::new()
    }
}
