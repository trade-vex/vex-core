//! Orderbook snapshot verification utilities
//!
//! This module provides verification functions for orderbook snapshots in Redis.

use crate::test_framework::redis::RedisVerifier;
use crate::test_framework::types::*;
use common::Side;
use std::time::Duration;
use tokio::time::sleep;
use tracing::debug;

/// Verifier for orderbook snapshots in Redis
///
/// Verifies orderbook snapshots that are published to Redis by the Events Handler.
/// Key format: `orderbook:market:{market_id}`
pub struct OrderbookVerifier<'a> {
    redis: &'a mut RedisVerifier,
}

impl<'a> OrderbookVerifier<'a> {
    /// Create a new orderbook verifier
    pub fn new(redis: &'a mut RedisVerifier) -> Self {
        Self { redis }
    }

    /// Assert orderbook depth (number of levels)
    pub async fn assert_orderbook_depth(
        &mut self,
        market_id: u32,
        expected_bid_levels: usize,
        expected_ask_levels: usize,
    ) -> TestResult<()> {
        let orderbook = self.redis.get_orderbook(market_id).await?;

        if orderbook.bids.len() != expected_bid_levels {
            return Err(TestError::Verification {
                message: format!(
                    "Bid depth mismatch for market {}: expected {} levels, got {}",
                    market_id,
                    expected_bid_levels,
                    orderbook.bids.len()
                ),
            });
        }

        if orderbook.asks.len() != expected_ask_levels {
            return Err(TestError::Verification {
                message: format!(
                    "Ask depth mismatch for market {}: expected {} levels, got {}",
                    market_id,
                    expected_ask_levels,
                    orderbook.asks.len()
                ),
            });
        }

        debug!(
            "Orderbook depth verified for market {}: {} bids, {} asks",
            market_id,
            orderbook.bids.len(),
            orderbook.asks.len()
        );

        Ok(())
    }

    /// Assert best bid and ask prices
    pub async fn assert_best_prices(
        &mut self,
        market_id: u32,
        expected_best_bid: Option<u64>,
        expected_best_ask: Option<u64>,
    ) -> TestResult<()> {
        let orderbook = self.redis.get_orderbook(market_id).await?;

        let actual_best_bid = orderbook.bids.first().map(|level| level.price);
        let actual_best_ask = orderbook.asks.first().map(|level| level.price);

        if actual_best_bid != expected_best_bid {
            return Err(TestError::Verification {
                message: format!(
                    "Best bid mismatch for market {}: expected {:?}, got {:?}",
                    market_id, expected_best_bid, actual_best_bid
                ),
            });
        }

        if actual_best_ask != expected_best_ask {
            return Err(TestError::Verification {
                message: format!(
                    "Best ask mismatch for market {}: expected {:?}, got {:?}",
                    market_id, expected_best_ask, actual_best_ask
                ),
            });
        }

        debug!(
            "Best prices verified for market {}: bid={:?}, ask={:?}",
            market_id, actual_best_bid, actual_best_ask
        );

        Ok(())
    }

    /// Assert specific price level exists with expected volume
    pub async fn assert_level(
        &mut self,
        market_id: u32,
        side: Side,
        price: u64,
        expected_volume: u64,
    ) -> TestResult<()> {
        let orderbook = self.redis.get_orderbook(market_id).await?;

        let levels = match side {
            Side::Bid => &orderbook.bids,
            Side::Ask => &orderbook.asks,
        };

        let level = levels
            .iter()
            .find(|level| level.price == price)
            .ok_or_else(|| TestError::Verification {
                message: format!(
                    "Price level not found: market={}, side={:?}, price={}",
                    market_id, side, price
                ),
            })?;

        if level.size != expected_volume {
            return Err(TestError::Verification {
                message: format!(
                    "Volume mismatch at price level: market={}, side={:?}, price={}, expected={}, got={}",
                    market_id, side, price, expected_volume, level.size
                ),
            });
        }

        debug!(
            "Price level verified: market={}, side={:?}, price={}, volume={}",
            market_id, side, price, level.size
        );

        Ok(())
    }

    /// Wait for orderbook update and then verify depth
    pub async fn wait_and_assert_depth(
        &mut self,
        market_id: u32,
        expected_bid_levels: usize,
        expected_ask_levels: usize,
        timeout: Duration,
    ) -> TestResult<()> {
        let orderbook = self
            .redis
            .wait_for_orderbook_update(market_id, timeout)
            .await?;

        if orderbook.bids.len() != expected_bid_levels
            || orderbook.asks.len() != expected_ask_levels
        {
            return Err(TestError::Verification {
                message: format!(
                    "Orderbook depth mismatch after wait: expected {} bids and {} asks, got {} bids and {} asks",
                    expected_bid_levels,
                    expected_ask_levels,
                    orderbook.bids.len(),
                    orderbook.asks.len()
                ),
            });
        }

        debug!(
            "Orderbook depth verified after wait for market {}: {} bids, {} asks",
            market_id,
            orderbook.bids.len(),
            orderbook.asks.len()
        );

        Ok(())
    }

    /// Wait for orderbook update
    pub async fn wait_for_orderbook_update(
        &mut self,
        market_id: u32,
        timeout: Duration,
    ) -> TestResult<RedisOrderbook> {
        self.redis
            .wait_for_orderbook_update(market_id, timeout)
            .await
    }

    /// Get current orderbook
    pub async fn get_orderbook(&mut self, market_id: u32) -> TestResult<RedisOrderbook> {
        self.redis.get_orderbook(market_id).await
    }

    /// Assert orderbook is empty (no bids or asks)
    pub async fn assert_empty(&mut self, market_id: u32) -> TestResult<()> {
        self.assert_orderbook_depth(market_id, 0, 0).await
    }

    /// Assert spread (difference between best bid and best ask)
    pub async fn assert_spread(&mut self, market_id: u32, expected_spread: u64) -> TestResult<()> {
        let orderbook = self.redis.get_orderbook(market_id).await?;

        if orderbook.bids.is_empty() || orderbook.asks.is_empty() {
            return Err(TestError::Verification {
                message: format!(
                    "Cannot calculate spread: orderbook has empty side for market {}",
                    market_id
                ),
            });
        }

        let best_bid = orderbook.bids[0].price;
        let best_ask = orderbook.asks[0].price;
        let actual_spread = best_ask.saturating_sub(best_bid);

        if actual_spread != expected_spread {
            return Err(TestError::Verification {
                message: format!(
                    "Spread mismatch for market {}: expected {}, got {} (bid={}, ask={})",
                    market_id, expected_spread, actual_spread, best_bid, best_ask
                ),
            });
        }

        debug!(
            "Spread verified for market {}: {}",
            market_id, actual_spread
        );

        Ok(())
    }

    /// Calculate total volume on one side of the book
    pub async fn calculate_side_volume(&mut self, market_id: u32, side: Side) -> TestResult<u64> {
        let orderbook = self.redis.get_orderbook(market_id).await?;

        let levels = match side {
            Side::Bid => &orderbook.bids,
            Side::Ask => &orderbook.asks,
        };

        let total: u64 = levels.iter().map(|level| level.size).sum();

        debug!(
            "Total volume on {:?} side for market {}: {}",
            side, market_id, total
        );

        Ok(total)
    }

    /// Assert bids are sorted in descending order (highest price first)
    pub async fn assert_bids_sorted(&mut self, market_id: u32) -> TestResult<()> {
        let orderbook = self.redis.get_orderbook(market_id).await?;

        for window in orderbook.bids.windows(2) {
            if window[0].price < window[1].price {
                return Err(TestError::Verification {
                    message: format!(
                        "Bids not sorted correctly for market {}: {} < {}",
                        market_id, window[0].price, window[1].price
                    ),
                });
            }
        }

        debug!("Bids verified to be sorted for market {}", market_id);
        Ok(())
    }

    /// Assert asks are sorted in ascending order (lowest price first)
    pub async fn assert_asks_sorted(&mut self, market_id: u32) -> TestResult<()> {
        let orderbook = self.redis.get_orderbook(market_id).await?;

        for window in orderbook.asks.windows(2) {
            if window[0].price > window[1].price {
                return Err(TestError::Verification {
                    message: format!(
                        "Asks not sorted correctly for market {}: {} > {}",
                        market_id, window[0].price, window[1].price
                    ),
                });
            }
        }

        debug!("Asks verified to be sorted for market {}", market_id);
        Ok(())
    }

    /// Wait for a specific price level to appear in the orderbook with expected volume
    pub async fn wait_and_assert_level(
        &mut self,
        market_id: u32,
        side: Side,
        price: u64,
        expected_volume: u64,
        timeout: Duration,
    ) -> TestResult<()> {
        let start = std::time::Instant::now();

        loop {
            match self.redis.get_orderbook(market_id).await {
                Ok(orderbook) => {
                    let levels = match side {
                        Side::Bid => &orderbook.bids,
                        Side::Ask => &orderbook.asks,
                    };

                    if let Some(level) = levels.iter().find(|level| level.price == price)
                        && level.size == expected_volume
                    {
                        debug!(
                            "Price level found: market={}, side={:?}, price={}, volume={}",
                            market_id, side, price, level.size
                        );
                        return Ok(());
                    }
                }
                Err(e) => {
                    debug!("Error fetching orderbook: {}", e);
                }
            }

            if start.elapsed() > timeout {
                return Err(TestError::Timeout { timeout });
            }

            sleep(Duration::from_millis(50)).await;
        }
    }
}
