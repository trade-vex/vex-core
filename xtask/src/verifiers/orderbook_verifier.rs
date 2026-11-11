//! Orderbook snapshot verification utilities
//!
//! This module provides verification functions for orderbook snapshots.

use crate::test_framework::kafka::KafkaVerifier;
use crate::test_framework::types::*;
use common::Side;
use std::time::Duration;
use tokio::time::sleep;
use tracing::debug;

/// Verifier for orderbook snapshots
///
/// Verifies orderbook snapshots that are published to Kafka by the Events Handler.
/// Topic `orderbook`, key `{market_id}`
pub struct OrderbookVerifier<'a> {
    kafka: &'a mut KafkaVerifier,
}

impl<'a> OrderbookVerifier<'a> {
    /// Create a new orderbook verifier
    pub fn new(kafka: &'a mut KafkaVerifier) -> Self {
        Self { kafka }
    }

    /// Assert orderbook depth (number of levels)
    pub async fn assert_orderbook_depth(
        &mut self,
        market_id: u32,
        expected_bid_levels: usize,
        expected_ask_levels: usize,
    ) -> TestResult<()> {
        let orderbook = self.kafka.get_orderbook(market_id).await?;

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

    /// Assert specific price level exists with expected volume
    pub async fn assert_level(
        &mut self,
        market_id: u32,
        side: Side,
        price: u64,
        expected_volume: u64,
    ) -> TestResult<()> {
        let orderbook = self.kafka.get_orderbook(market_id).await?;

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

    /// Wait for the orderbook as of the command stamped `after`, then verify its depth
    pub async fn wait_and_assert_depth(
        &mut self,
        market_id: u32,
        expected_bid_levels: usize,
        expected_ask_levels: usize,
        after: u64,
        timeout: Duration,
    ) -> TestResult<()> {
        let orderbook = self
            .kafka
            .wait_for_orderbook_after(market_id, after, timeout)
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

    /// Get current orderbook
    pub async fn get_orderbook(&mut self, market_id: u32) -> TestResult<OrderbookSnapshot> {
        self.kafka.get_orderbook(market_id).await
    }

    /// Assert orderbook is empty (no bids or asks)
    pub async fn assert_empty(&mut self, market_id: u32) -> TestResult<()> {
        self.assert_orderbook_depth(market_id, 0, 0).await
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
            let orderbook = self.kafka.get_orderbook(market_id).await?;
            let levels = match side {
                Side::Bid => &orderbook.bids,
                Side::Ask => &orderbook.asks,
            };
            if levels
                .iter()
                .any(|level| level.price == price && level.size == expected_volume)
            {
                debug!(
                    "Price level found: market={}, side={:?}, price={}, volume={}",
                    market_id, side, price, expected_volume
                );
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(TestError::Timeout { timeout });
            }

            sleep(Duration::from_millis(50)).await;
        }
    }
}
