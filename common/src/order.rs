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
    pub original_size: u64,
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
    sequence: AtomicU64,
    pub best_bid: AtomicU64,
    pub best_ask: AtomicU64,
}

impl Default for MarketPrice {
    fn default() -> Self {
        // Sentinels match orderbook semantics:
        // - No bid means price floor (0) - can't sell
        // - No ask means price ceiling (MAX) - can't buy
        Self {
            sequence: AtomicU64::new(0),
            best_bid: AtomicU64::new(0),
            best_ask: AtomicU64::new(u64::MAX),
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

    /// Get a consistent best bid and ask snapshot for a symbol.
    pub fn get_prices(&self, symbol: u32) -> Option<(u64, u64)> {
        let market_price = self.prices.get(&symbol)?;

        loop {
            let sequence = market_price.sequence.load(Ordering::SeqCst);
            if sequence % 2 != 0 {
                std::hint::spin_loop();
                continue;
            }

            let best_bid = market_price.best_bid.load(Ordering::SeqCst);
            let best_ask = market_price.best_ask.load(Ordering::SeqCst);
            if sequence == market_price.sequence.load(Ordering::SeqCst) {
                return Some((best_bid, best_ask));
            }
        }
    }

    /// Get the best bid price for a symbol
    /// Returns 0 for an empty book and `u64::MAX` for an unknown symbol.
    pub fn get_best_bid(&self, symbol: u32) -> u64 {
        self.get_prices(symbol)
            .map_or(u64::MAX, |(best_bid, _)| best_bid)
    }

    /// Get the best ask price for a symbol
    /// Returns `u64::MAX` for an empty book and 0 for an unknown symbol.
    pub fn get_best_ask(&self, symbol: u32) -> u64 {
        self.get_prices(symbol).map_or(0, |(_, best_ask)| best_ask)
    }

    /// Update the best bid price for a symbol
    /// Missing symbols are ignored.
    pub fn update_prices(&self, symbol: u32, best_bid: u64, best_ask: u64) {
        let Some(market_price) = self.prices.get(&symbol) else {
            return;
        };

        market_price.sequence.fetch_add(1, Ordering::SeqCst);
        market_price.best_bid.store(best_bid, Ordering::SeqCst);
        market_price.best_ask.store(best_ask, Ordering::SeqCst);
        market_price.sequence.fetch_add(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CoreMarketSpecification, MarketType};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    fn price_cache(market_id: u32) -> PriceCache {
        let mut specs = HashMap::new();
        specs.insert(
            market_id,
            CoreMarketSpecification::builder()
                .market_id(market_id)
                .market_type(MarketType::Spot)
                .build()
                .unwrap(),
        );
        PriceCache::new(specs.keys())
    }

    #[test]
    fn price_snapshot_cannot_be_torn() {
        let market_id = 1;
        let cache = Arc::new(price_cache(market_id));
        let writer_cache = Arc::clone(&cache);
        let writer_done = Arc::new(AtomicBool::new(false));
        let writer_done_clone = Arc::clone(&writer_done);

        let writer = std::thread::spawn(move || {
            for best_bid in 1..=100_000 {
                writer_cache.update_prices(market_id, best_bid, !best_bid);
            }
            writer_done_clone.store(true, Ordering::Release);
        });

        while !writer_done.load(Ordering::Acquire) {
            let (best_bid, best_ask) = cache.get_prices(market_id).unwrap();
            assert_eq!(best_ask, !best_bid);
        }

        writer.join().unwrap();
    }

    #[test]
    fn updating_a_missing_market_does_not_panic() {
        let cache = price_cache(1);
        cache.update_prices(2, 10, 11);
        assert_eq!(cache.get_prices(2), None);
    }
}
