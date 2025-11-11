use std::sync::atomic::{AtomicU64, Ordering};

use borsh::{BorshDeserialize, BorshSerialize};
use hashbrown::{HashMap, hash_map::Keys};
use serde::{Deserialize, Serialize};

use crate::{CoreMarketSpecification, Side, Status, TimeInForce};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize, Deserialize,
)]
pub struct Order {
    pub order_id: u64,
    pub user_id: u64,
    pub price: u64,
    pub size: u64,
    pub side: Side,
    pub time_in_force: TimeInForce,
    pub status: Status,
    pub timestamp: u64,
}

impl Order {
    pub fn price(&self) -> u64 {
        self.price
    }
    pub fn size(&self) -> u64 {
        self.size
    }
    pub fn user_id(&self) -> u64 {
        self.user_id
    }
    pub fn side(&self) -> Side {
        self.side
    }
    pub fn order_id(&self) -> u64 {
        self.order_id
    }
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

// Holds the top-of-book prices for a single symbol
pub struct MarketPrice {
    pub best_bid: AtomicU64,
    pub best_ask: AtomicU64,
}

impl Default for MarketPrice {
    fn default() -> Self {
        Self {
            best_bid: AtomicU64::new(u64::MAX),
            best_ask: AtomicU64::new(0),
        }
    }
}

// The cache shared across vex-core
pub struct PriceCache {
    prices: HashMap<u32, MarketPrice>,
}

impl PriceCache {
    pub fn new(symbol_spec: Keys<u32, CoreMarketSpecification>) -> Self {
        let mut prices = HashMap::new();
        for symbol in symbol_spec {
            prices.insert(*symbol, MarketPrice::default());
        }
        Self { prices }
    }

    /// Get the best bid price for a symbol
    /// Retruns u64::MAX if no bid price is available
    pub fn get_best_bid(&self, symbol: u32) -> u64 {
        match self.prices.get(&symbol) {
            Some(market_price) => market_price.best_bid.load(Ordering::Acquire),
            None => u64::MAX,
        }
    }

    /// Get the best ask price for a symbol    
    /// Returns 0 if no ask price is available
    pub fn get_best_ask(&self, symbol: u32) -> u64 {
        match self.prices.get(&symbol) {
            Some(market_price) => market_price.best_ask.load(Ordering::Acquire),
            None => 0,
        }
    }

    /// Update the best bid price for a symbol
    /// If the symbol does not exist, it will be created
    pub fn update_prices(&self, symbol: u32, best_bid: u64, best_ask: u64) {
        let market_price = self.prices.get(&symbol).unwrap();
        market_price.best_bid.store(best_bid, Ordering::Release);
        market_price.best_ask.store(best_ask, Ordering::Release);
    }
}
