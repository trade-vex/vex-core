//! Trade event verification utilities
//!
//! This module provides verification functions for trade events in Redis.
//! Trade events are published by the Events Handler.

use crate::test_framework::redis::RedisVerifier;
use crate::test_framework::types::*;
use std::time::Duration;
use tracing::{debug, info};

/// Verifier for trade events in Redis
///
/// Verifies trades that are published to Redis by the Events Handler.
/// Key formats:
/// - Stream: `trades:market:{market_id}`
/// - ZSet: `market:{market_id}:trades` (sorted by timestamp)
pub struct TradeVerifier<'a> {
    redis: &'a mut RedisVerifier,
}

impl<'a> TradeVerifier<'a> {
    /// Create a new trade verifier
    pub fn new(redis: &'a mut RedisVerifier) -> Self {
        Self { redis }
    }

    /// Assert a trade exists matching the given criteria
    pub async fn assert_trade_exists(
        &mut self,
        market_id: u32,
        criteria: &TradeCriteria,
    ) -> TestResult<()> {
        let trades = self.redis.get_trades(market_id, 100).await?;
        info!("Trades fetched for market {}: {:?}", market_id, trades);

        let found = trades.iter().any(|trade| criteria.matches(trade));

        if !found {
            return Err(TestError::Verification {
                message: format!(
                    "No trade found matching criteria for market {}: {:?}",
                    market_id, criteria
                ),
            });
        }

        debug!("Trade found matching criteria for market {}", market_id);
        Ok(())
    }

    /// Assert exact trade count for a market
    pub async fn assert_trade_count(
        &mut self,
        market_id: u32,
        expected_count: usize,
    ) -> TestResult<()> {
        let trades = self.redis.get_trades(market_id, 1000).await?;

        if trades.len() != expected_count {
            return Err(TestError::Verification {
                message: format!(
                    "Trade count mismatch for market {}: expected {}, got {}",
                    market_id,
                    expected_count,
                    trades.len()
                ),
            });
        }

        debug!(
            "Trade count verified for market {}: {}",
            market_id, expected_count
        );
        Ok(())
    }

    /// Assert at least N trades exist
    pub async fn assert_min_trade_count(
        &mut self,
        market_id: u32,
        min_count: usize,
    ) -> TestResult<()> {
        let trades = self.redis.get_trades(market_id, min_count + 10).await?;

        if trades.len() < min_count {
            return Err(TestError::Verification {
                message: format!(
                    "Insufficient trades for market {}: expected at least {}, got {}",
                    market_id,
                    min_count,
                    trades.len()
                ),
            });
        }

        debug!(
            "Minimum trade count verified for market {}: {} >= {}",
            market_id,
            trades.len(),
            min_count
        );
        Ok(())
    }

    /// Assert trade details match expected values
    pub async fn assert_trade_details(
        &mut self,
        market_id: u32,
        maker_order_id: u64,
        taker_order_id: u64,
        expected_price: u64,
        expected_size: u64,
    ) -> TestResult<()> {
        let criteria = TradeCriteria::new()
            .market_id(market_id)
            .maker_order_id(maker_order_id)
            .taker_order_id(taker_order_id)
            .price(expected_price)
            .size(expected_size);

        self.assert_trade_exists(market_id, &criteria).await
    }

    /// Get all recent trades for a market
    pub async fn get_recent_trades(
        &mut self,
        market_id: u32,
        count: usize,
    ) -> TestResult<Vec<RedisTrade>> {
        self.redis.get_trades(market_id, count).await
    }

    /// Wait for trade event and verify it matches criteria
    pub async fn wait_and_assert_trade(
        &mut self,
        market_id: u32,
        criteria: &TradeCriteria,
        timeout: Duration,
    ) -> TestResult<RedisTrade> {
        let trade = self
            .redis
            .wait_for_trade(market_id, criteria, timeout)
            .await?;

        if !criteria.matches(&trade) {
            return Err(TestError::Verification {
                message: format!(
                    "Trade found but doesn't match criteria: got {:?}, expected {:?}",
                    trade, criteria
                ),
            });
        }

        debug!("Trade verified after wait for market {}", market_id);
        Ok(trade)
    }

    /// Assert trade has specific maker and taker
    pub async fn assert_trade_participants(
        &mut self,
        market_id: u32,
        maker_user_id: u64,
        taker_user_id: u64,
    ) -> TestResult<()> {
        let criteria = TradeCriteria::new()
            .market_id(market_id)
            .maker_user_id(maker_user_id)
            .taker_user_id(taker_user_id);

        self.assert_trade_exists(market_id, &criteria).await
    }

    /// Assert specific price was traded at
    pub async fn assert_trade_price(
        &mut self,
        market_id: u32,
        expected_price: u64,
    ) -> TestResult<()> {
        let criteria = TradeCriteria::new()
            .market_id(market_id)
            .price(expected_price);

        self.assert_trade_exists(market_id, &criteria).await
    }

    /// Assert specific size was traded
    pub async fn assert_trade_size(
        &mut self,
        market_id: u32,
        expected_size: u64,
    ) -> TestResult<()> {
        let criteria = TradeCriteria::new()
            .market_id(market_id)
            .size(expected_size);

        self.assert_trade_exists(market_id, &criteria).await
    }

    /// Assert NO trades exist (useful for FOK rejection, self-match prevention, etc.)
    pub async fn assert_no_trades(&mut self, market_id: u32) -> TestResult<()> {
        let trades = self.redis.get_trades(market_id, 10).await?;

        if !trades.is_empty() {
            return Err(TestError::Verification {
                message: format!(
                    "Expected no trades for market {}, but found {}",
                    market_id,
                    trades.len()
                ),
            });
        }

        debug!("Verified no trades exist for market {}", market_id);
        Ok(())
    }

    /// Verify trades are sorted by timestamp (ascending)
    pub async fn assert_trades_sorted_by_timestamp(&mut self, market_id: u32) -> TestResult<()> {
        let trades = self.redis.get_trades(market_id, 100).await?;

        for window in trades.windows(2) {
            if window[0].timestamp > window[1].timestamp {
                return Err(TestError::Verification {
                    message: format!(
                        "Trades not sorted by timestamp for market {}: {} > {}",
                        market_id, window[0].timestamp, window[1].timestamp
                    ),
                });
            }
        }

        debug!(
            "Trades verified to be sorted by timestamp for market {}",
            market_id
        );
        Ok(())
    }

    /// Get trade by order IDs
    pub async fn get_trade_by_orders(
        &mut self,
        market_id: u32,
        maker_order_id: u64,
        taker_order_id: u64,
    ) -> TestResult<Option<RedisTrade>> {
        let trades = self.redis.get_trades(market_id, 100).await?;

        Ok(trades.into_iter().find(|trade| {
            trade.maker_order_id == maker_order_id && trade.taker_order_id == taker_order_id
        }))
    }

    /// Calculate total traded volume for a market
    pub async fn calculate_total_volume(&mut self, market_id: u32) -> TestResult<u64> {
        let trades = self.redis.get_trades(market_id, 1000).await?;
        let total = trades.iter().map(|trade| trade.size).sum();

        debug!("Total traded volume for market {}: {}", market_id, total);
        Ok(total)
    }
}
