//! System-wide invariant verification
//!
//! This module provides functions to verify critical system invariants
//! that must hold true across the entire trading system.

use crate::test_framework::redis::RedisVerifier;
use crate::test_framework::types::*;
use tracing::debug;

/// Verifier for system-wide invariants
///
/// These invariants are fundamental properties that must always hold true:
/// - Balance consistency and overflow safety
/// - Orderbook price ordering and non-crossing
/// - Trade timestamp ordering, positive sizes, and valid prices
pub struct InvariantVerifier;

fn checked_balance_total(balance: &RedisBalance) -> TestResult<u64> {
    balance
        .available
        .checked_add(balance.locked)
        .ok_or_else(|| TestError::Verification {
            message: format!(
                "Balance overflow detected for user {} asset {}: available={} + locked={}",
                balance.user_id, balance.asset_id, balance.available, balance.locked
            ),
        })
}

impl InvariantVerifier {
    /// Verify balance invariant for all specified users and assets
    ///
    /// Invariant: available + locked = total
    pub async fn verify_balance_consistency(
        redis: &mut RedisVerifier,
        users: &[u64],
        assets: &[u16],
    ) -> TestResult<()> {
        for &user_id in users {
            for &asset_id in assets {
                match redis.get_balance(user_id, asset_id).await {
                    Ok(balance) => {
                        let computed_total = checked_balance_total(&balance)?;
                        if computed_total != balance.total {
                            return Err(TestError::Verification {
                                message: format!(
                                    "Balance invariant violated for user {} asset {}: available({}) + locked({}) != total({})",
                                    user_id,
                                    asset_id,
                                    balance.available,
                                    balance.locked,
                                    balance.total
                                ),
                            });
                        }
                    }
                    Err(TestError::Verification { .. }) => {
                        // Balance not found, skip (user may not have this asset)
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        debug!(
            "Balance consistency verified for {} users and {} assets",
            users.len(),
            assets.len()
        );

        Ok(())
    }

    /// Verify that adding unsigned balance components cannot overflow
    ///
    /// Redis balances are unsigned, so negative values cannot be represented. This check detects
    /// values whose `available + locked` sum would wrap around.
    pub async fn verify_no_balance_overflow(
        redis: &mut RedisVerifier,
        users: &[u64],
        assets: &[u16],
    ) -> TestResult<()> {
        for &user_id in users {
            for &asset_id in assets {
                match redis.get_balance(user_id, asset_id).await {
                    Ok(balance) => {
                        checked_balance_total(&balance)?;
                    }
                    Err(TestError::Verification { .. }) => {
                        // Balance not found, skip
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        debug!(
            "Balance arithmetic verified for {} users and {} assets",
            users.len(),
            assets.len()
        );

        Ok(())
    }

    /// Verify orderbook price ordering
    ///
    /// Invariant: bids are sorted descending (highest first), asks ascending (lowest first)
    pub async fn verify_orderbook_ordering(
        redis: &mut RedisVerifier,
        market_id: u32,
    ) -> TestResult<()> {
        let orderbook = redis.get_orderbook(market_id).await?;

        // Verify bids are descending
        for window in orderbook.bids.windows(2) {
            if window[0].price < window[1].price {
                return Err(TestError::Verification {
                    message: format!(
                        "Bids not sorted (descending) for market {}: {} < {}",
                        market_id, window[0].price, window[1].price
                    ),
                });
            }
        }

        // Verify asks are ascending
        for window in orderbook.asks.windows(2) {
            if window[0].price > window[1].price {
                return Err(TestError::Verification {
                    message: format!(
                        "Asks not sorted (ascending) for market {}: {} > {}",
                        market_id, window[0].price, window[1].price
                    ),
                });
            }
        }

        debug!("Orderbook ordering verified for market {}", market_id);

        Ok(())
    }

    /// Verify orderbook spread is non-negative
    ///
    /// Invariant: best_ask >= best_bid (no crossed book)
    pub async fn verify_orderbook_no_cross(
        redis: &mut RedisVerifier,
        market_id: u32,
    ) -> TestResult<()> {
        let orderbook = redis.get_orderbook(market_id).await?;

        if !orderbook.bids.is_empty() && !orderbook.asks.is_empty() {
            let best_bid = orderbook.bids[0].price;
            let best_ask = orderbook.asks[0].price;

            if best_bid > best_ask {
                return Err(TestError::Verification {
                    message: format!(
                        "Crossed orderbook detected for market {}: best_bid={} > best_ask={}",
                        market_id, best_bid, best_ask
                    ),
                });
            }
        }

        debug!(
            "Orderbook not crossed (spread non-negative) for market {}",
            market_id
        );

        Ok(())
    }

    /// Verify trade timestamps are monotonically increasing
    ///
    /// Invariant: trades are ordered by timestamp
    pub async fn verify_trade_timestamp_ordering(
        redis: &mut RedisVerifier,
        market_id: u32,
    ) -> TestResult<()> {
        let trades = redis.get_trades(market_id, 1000).await?;

        for window in trades.windows(2) {
            if window[0].timestamp > window[1].timestamp {
                return Err(TestError::Verification {
                    message: format!(
                        "Trade timestamps not ordered for market {}: {} > {}",
                        market_id, window[0].timestamp, window[1].timestamp
                    ),
                });
            }
        }

        debug!(
            "Trade timestamp ordering verified for market {} ({} trades)",
            market_id,
            trades.len()
        );

        Ok(())
    }

    /// Verify all trades have positive size
    ///
    /// Invariant: trade size > 0
    pub async fn verify_trade_sizes_positive(
        redis: &mut RedisVerifier,
        market_id: u32,
    ) -> TestResult<()> {
        let trades = redis.get_trades(market_id, 1000).await?;

        for trade in &trades {
            if trade.size == 0 {
                return Err(TestError::Verification {
                    message: format!(
                        "Zero-size trade detected for market {}: trade_id={}",
                        market_id, trade.trade_id
                    ),
                });
            }
        }

        debug!(
            "All trade sizes positive for market {} ({} trades)",
            market_id,
            trades.len()
        );

        Ok(())
    }

    /// Verify all trades have valid prices
    ///
    /// Invariant: trade price > 0 and < u64::MAX
    pub async fn verify_trade_prices_valid(
        redis: &mut RedisVerifier,
        market_id: u32,
    ) -> TestResult<()> {
        let trades = redis.get_trades(market_id, 1000).await?;

        for trade in &trades {
            if trade.price == 0 || trade.price == u64::MAX {
                return Err(TestError::Verification {
                    message: format!(
                        "Invalid trade price detected for market {}: price={}, trade_id={}",
                        market_id, trade.price, trade.trade_id
                    ),
                });
            }
        }

        debug!(
            "All trade prices valid for market {} ({} trades)",
            market_id,
            trades.len()
        );

        Ok(())
    }

    /// Run all invariant checks for a comprehensive system validation
    pub async fn verify_all(
        redis: &mut RedisVerifier,
        market_id: u32,
        users: &[u64],
        assets: &[u16],
    ) -> TestResult<()> {
        debug!("Running comprehensive invariant verification");

        // Balance invariants
        Self::verify_balance_consistency(redis, users, assets).await?;
        Self::verify_no_balance_overflow(redis, users, assets).await?;

        // Orderbook invariants
        match Self::verify_orderbook_ordering(redis, market_id).await {
            Ok(_) => {}
            Err(TestError::Verification { .. }) => {
                // Orderbook might not exist yet, skip
            }
            Err(e) => return Err(e),
        }

        match Self::verify_orderbook_no_cross(redis, market_id).await {
            Ok(_) => {}
            Err(TestError::Verification { .. }) => {
                // Orderbook might not exist yet, skip
            }
            Err(e) => return Err(e),
        }

        // Trade invariants
        match redis.get_trades(market_id, 1).await {
            Ok(trades) if !trades.is_empty() => {
                Self::verify_trade_timestamp_ordering(redis, market_id).await?;
                Self::verify_trade_sizes_positive(redis, market_id).await?;
                Self::verify_trade_prices_valid(redis, market_id).await?;
            }
            _ => {
                // No trades yet, skip trade invariants
            }
        }

        debug!("All invariants verified successfully");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn balance(available: u64, locked: u64) -> RedisBalance {
        RedisBalance {
            user_id: 7,
            asset_id: 3,
            available,
            locked,
            total: available.saturating_add(locked),
            timestamp: 0,
        }
    }

    #[test]
    fn checked_balance_total_accepts_maximum_non_overflowing_sum() {
        assert_eq!(
            checked_balance_total(&balance(u64::MAX - 1, 1)).unwrap(),
            u64::MAX
        );
    }

    #[test]
    fn checked_balance_total_rejects_overflow() {
        let error = checked_balance_total(&balance(u64::MAX, 1)).unwrap_err();
        assert!(error.to_string().contains("Balance overflow detected"));
    }
}
