//! Balance verification utilities
//!
//! This module provides verification functions for balance state in Redis.
//! Balances are published by the Events Handler and stored in Redis.

use crate::test_framework::redis::RedisVerifier;
use crate::test_framework::types::*;
use std::time::Duration;
use tracing::debug;

/// Verifier for balance state in Redis
///
/// Verifies balances that are published to Redis by the Events Handler.
/// Key format: `user:{user_id}:asset:{asset_id}:balance`
pub struct BalanceVerifier<'a> {
    redis: &'a mut RedisVerifier,
}

impl<'a> BalanceVerifier<'a> {
    /// Create a new balance verifier
    pub fn new(redis: &'a mut RedisVerifier) -> Self {
        Self { redis }
    }

    /// Assert balance equals expected value
    pub async fn assert_balance_eq(
        &mut self,
        user_id: u64,
        asset_id: u16,
        expected: Balance,
    ) -> TestResult<()> {
        let actual = self.redis.get_balance(user_id, asset_id).await?;

        if actual.available != expected.available {
            return Err(TestError::Verification {
                message: format!(
                    "Available balance mismatch for user {} asset {}: expected {}, got {}",
                    user_id, asset_id, expected.available, actual.available
                ),
            });
        }

        if actual.locked != expected.locked {
            return Err(TestError::Verification {
                message: format!(
                    "Locked balance mismatch for user {} asset {}: expected {}, got {}",
                    user_id, asset_id, expected.locked, actual.locked
                ),
            });
        }

        if actual.total != expected.total {
            return Err(TestError::Verification {
                message: format!(
                    "Total balance mismatch for user {} asset {}: expected {}, got {}",
                    user_id, asset_id, expected.total, actual.total
                ),
            });
        }

        debug!(
            "Balance verified for user {} asset {}: available={}, locked={}, total={}",
            user_id, asset_id, actual.available, actual.locked, actual.total
        );

        Ok(())
    }

    /// Assert locked funds equal expected value
    pub async fn assert_locked_eq(
        &mut self,
        user_id: u64,
        asset_id: u16,
        expected: u64,
    ) -> TestResult<()> {
        let actual = self.redis.get_balance(user_id, asset_id).await?;

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
        let actual = self.redis.get_balance(user_id, asset_id).await?;

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
        let actual = self.redis.get_balance(user_id, asset_id).await?;

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
        let balance = self.redis.get_balance(user_id, asset_id).await?;
        balance.verify_invariant()?;

        debug!(
            "Balance invariant verified for user {} asset {}",
            user_id, asset_id
        );

        Ok(())
    }

    /// Wait for balance update and then verify
    pub async fn wait_and_assert_balance(
        &mut self,
        user_id: u64,
        asset_id: u16,
        expected: Balance,
        timeout: Duration,
    ) -> TestResult<()> {
        let actual = self
            .redis
            .wait_for_balance_update(user_id, asset_id, timeout)
            .await?;

        if actual.available != expected.available
            || actual.locked != expected.locked
            || actual.total != expected.total
        {
            return Err(TestError::Verification {
                message: format!(
                    "Balance mismatch after wait for user {} asset {}: expected (available={}, locked={}, total={}), got (available={}, locked={}, total={})",
                    user_id, asset_id,
                    expected.available, expected.locked, expected.total,
                    actual.available, actual.locked, actual.total
                ),
            });
        }

        debug!(
            "Balance verified after wait for user {} asset {}: available={}, locked={}, total={}",
            user_id, asset_id, actual.available, actual.locked, actual.total
        );

        Ok(())
    }

    /// Wait for balance update (any value)
    pub async fn wait_for_balance_update(
        &mut self,
        user_id: u64,
        asset_id: u16,
        timeout: Duration,
    ) -> TestResult<RedisBalance> {
        self.redis
            .wait_for_balance_update(user_id, asset_id, timeout)
            .await
    }

    /// Assert available balance increased by expected amount
    pub async fn assert_available_increased_by(
        &mut self,
        user_id: u64,
        asset_id: u16,
        previous: u64,
        increase: u64,
    ) -> TestResult<()> {
        let actual = self.redis.get_balance(user_id, asset_id).await?;
        let expected = previous + increase;

        if actual.available != expected {
            return Err(TestError::Verification {
                message: format!(
                    "Available balance did not increase correctly for user {} asset {}: previous={}, increase={}, expected={}, got={}",
                    user_id, asset_id, previous, increase, expected, actual.available
                ),
            });
        }

        debug!(
            "Available balance increased correctly for user {} asset {}: {} -> {}",
            user_id, asset_id, previous, actual.available
        );

        Ok(())
    }

    /// Assert available balance decreased by expected amount
    pub async fn assert_available_decreased_by(
        &mut self,
        user_id: u64,
        asset_id: u16,
        previous: u64,
        decrease: u64,
    ) -> TestResult<()> {
        let actual = self.redis.get_balance(user_id, asset_id).await?;
        let expected = previous - decrease;

        if actual.available != expected {
            return Err(TestError::Verification {
                message: format!(
                    "Available balance did not decrease correctly for user {} asset {}: previous={}, decrease={}, expected={}, got={}",
                    user_id, asset_id, previous, decrease, expected, actual.available
                ),
            });
        }

        debug!(
            "Available balance decreased correctly for user {} asset {}: {} -> {}",
            user_id, asset_id, previous, actual.available
        );

        Ok(())
    }

    /// Assert locked balance increased by expected amount
    pub async fn assert_locked_increased_by(
        &mut self,
        user_id: u64,
        asset_id: u16,
        previous: u64,
        increase: u64,
    ) -> TestResult<()> {
        let actual = self.redis.get_balance(user_id, asset_id).await?;
        let expected = previous + increase;

        if actual.locked != expected {
            return Err(TestError::Verification {
                message: format!(
                    "Locked balance did not increase correctly for user {} asset {}: previous={}, increase={}, expected={}, got={}",
                    user_id, asset_id, previous, increase, expected, actual.locked
                ),
            });
        }

        debug!(
            "Locked balance increased correctly for user {} asset {}: {} -> {}",
            user_id, asset_id, previous, actual.locked
        );

        Ok(())
    }

    /// Assert locked balance decreased by expected amount (e.g., after cancel)
    pub async fn assert_locked_decreased_by(
        &mut self,
        user_id: u64,
        asset_id: u16,
        previous: u64,
        decrease: u64,
    ) -> TestResult<()> {
        let actual = self.redis.get_balance(user_id, asset_id).await?;
        let expected = previous - decrease;

        if actual.locked != expected {
            return Err(TestError::Verification {
                message: format!(
                    "Locked balance did not decrease correctly for user {} asset {}: previous={}, decrease={}, expected={}, got={}",
                    user_id, asset_id, previous, decrease, expected, actual.locked
                ),
            });
        }

        debug!(
            "Locked balance decreased correctly for user {} asset {}: {} -> {}",
            user_id, asset_id, previous, actual.locked
        );

        Ok(())
    }

    /// Get current balance
    pub async fn get_balance(&mut self, user_id: u64, asset_id: u16) -> TestResult<RedisBalance> {
        self.redis.get_balance(user_id, asset_id).await
    }
}
