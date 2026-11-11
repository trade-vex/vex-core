//! Balance verification utilities
//!
//! This module provides verification functions for balance state.
//! Balances are published to Kafka by the Events Handler.

use crate::test_framework::kafka::KafkaVerifier;
use crate::test_framework::types::*;
use std::time::Duration;
use tracing::debug;

/// Verifier for balance state
///
/// Verifies balances that are published to Kafka by the Events Handler.
/// Topic `balances`, key `{user_id}:{asset_id}`
pub struct BalanceVerifier<'a> {
    kafka: &'a mut KafkaVerifier,
}

impl<'a> BalanceVerifier<'a> {
    /// Create a new balance verifier
    pub fn new(kafka: &'a mut KafkaVerifier) -> Self {
        Self { kafka }
    }

    /// Assert locked funds equal expected value
    pub async fn assert_locked_eq(
        &mut self,
        user_id: u64,
        asset_id: u16,
        expected: u64,
    ) -> TestResult<()> {
        let actual = self.kafka.get_balance(user_id, asset_id).await?;

        if actual.locked != expected {
            return Err(TestError::Verification {
                message: format!(
                    "Locked balance mismatch for user {} asset {}: expected {}, got {}",
                    user_id, asset_id, expected, actual.locked
                ),
            });
        }

        debug!(
            "Locked balance verified for user {} asset {}: {}",
            user_id, asset_id, actual.locked
        );

        Ok(())
    }

    /// Assert available funds equal expected value
    pub async fn assert_available_eq(
        &mut self,
        user_id: u64,
        asset_id: u16,
        expected: u64,
    ) -> TestResult<()> {
        let actual = self.kafka.get_balance(user_id, asset_id).await?;

        if actual.available != expected {
            return Err(TestError::Verification {
                message: format!(
                    "Available balance mismatch for user {} asset {}: expected {}, got {}",
                    user_id, asset_id, expected, actual.available
                ),
            });
        }

        debug!(
            "Available balance verified for user {} asset {}: {}",
            user_id, asset_id, actual.available
        );

        Ok(())
    }

    /// Assert total balance equals expected value
    pub async fn assert_total_eq(
        &mut self,
        user_id: u64,
        asset_id: u16,
        expected: u64,
    ) -> TestResult<()> {
        let actual = self.kafka.get_balance(user_id, asset_id).await?;

        if actual.total != expected {
            return Err(TestError::Verification {
                message: format!(
                    "Total balance mismatch for user {} asset {}: expected {}, got {}",
                    user_id, asset_id, expected, actual.total
                ),
            });
        }

        debug!(
            "Total balance verified for user {} asset {}: {}",
            user_id, asset_id, actual.total
        );

        Ok(())
    }

    /// Verify balance invariant: available + locked = total
    pub async fn assert_balance_invariant(
        &mut self,
        user_id: u64,
        asset_id: u16,
    ) -> TestResult<()> {
        let balance = self.kafka.get_balance(user_id, asset_id).await?;
        verify_balance_invariant(&balance)?;

        debug!(
            "Balance invariant verified for user {} asset {}",
            user_id, asset_id
        );

        Ok(())
    }

    /// Wait for the balance as of the command stamped `after`, then verify it
    pub async fn wait_and_assert_balance(
        &mut self,
        user_id: u64,
        asset_id: u16,
        expected: Balance,
        after: u64,
        timeout: Duration,
    ) -> TestResult<()> {
        let actual = self
            .kafka
            .wait_for_balance_after(user_id, asset_id, after, timeout)
            .await?;

        if actual.available != expected.available
            || actual.locked != expected.locked
            || actual.total != expected.total
        {
            return Err(TestError::Verification {
                message: format!(
                    "Balance mismatch after wait for user {} asset {}: expected (available={}, locked={}, total={}), got (available={}, locked={}, total={})",
                    user_id,
                    asset_id,
                    expected.available,
                    expected.locked,
                    expected.total,
                    actual.available,
                    actual.locked,
                    actual.total
                ),
            });
        }

        debug!(
            "Balance verified after wait for user {} asset {}: available={}, locked={}, total={}",
            user_id, asset_id, actual.available, actual.locked, actual.total
        );

        Ok(())
    }

    /// Get current balance
    pub async fn get_balance(&mut self, user_id: u64, asset_id: u16) -> TestResult<BalanceEvent> {
        self.kafka.get_balance(user_id, asset_id).await
    }
}
