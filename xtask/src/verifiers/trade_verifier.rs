//! Trade event verification utilities
//!
//! This module provides verification functions for trade events.
//! Trade events are published to Kafka by the Events Handler.

use crate::test_framework::kafka::KafkaVerifier;
use crate::test_framework::types::*;
use std::time::Duration;
use tracing::debug;

/// Verifier for trade events
///
/// Verifies trades that are published to Kafka by the Events Handler.
/// Topic `trades`, key `{taker_order_id}:{maker_order_id}`
pub struct TradeVerifier<'a> {
    kafka: &'a mut KafkaVerifier,
}

impl<'a> TradeVerifier<'a> {
    /// Create a new trade verifier
    pub fn new(kafka: &'a mut KafkaVerifier) -> Self {
        Self { kafka }
    }

    /// Get all recent trades for a market
    pub async fn get_recent_trades(
        &mut self,
        market_id: u32,
        count: usize,
    ) -> TestResult<Vec<TradeEvent>> {
        self.kafka.get_trades(market_id, count).await
    }

    /// Wait for trade event and verify it matches criteria
    pub async fn wait_and_assert_trade(
        &mut self,
        market_id: u32,
        criteria: &TradeCriteria,
        timeout: Duration,
    ) -> TestResult<TradeEvent> {
        let trade = self
            .kafka
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
}
