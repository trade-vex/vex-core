//! Response verification utilities
//!
//! This module provides verification functions for OrderCommand response fields.
//! These are the fields that are encoded/decoded and sent back to the client.

use crate::test_framework::types::*;
use common::{OrderCommand, Status};
use tracing::debug;

/// Verifier for OrderCommand response fields
///
/// Verifies the fields that are included in the client response:
/// - status, order_id, timestamp, size, price, etc.
///
/// Does NOT verify internal fields like events, balance, l2_data
/// (those are verified through Redis)
pub struct ResponseVerifier;

impl ResponseVerifier {
    /// Verify the status field matches expected
    ///
    /// This is the most critical field to verify as it indicates
    /// whether the order was accepted, rejected, or executed.
    pub fn assert_status(response: &OrderCommand, expected: Status) -> TestResult<()> {
        if response.status != expected {
            return Err(TestError::Assertion {
                message: format!(
                    "Status mismatch: expected {:?}, got {:?}",
                    expected, response.status
                ),
            });
        }
        debug!("Status verified: {:?}", response.status);
        Ok(())
    }

    /// Verify that order_id was assigned by journaling processor
    ///
    /// The journaling processor assigns unique order IDs using snowflake algorithm.
    /// A valid order_id should be non-zero.
    pub fn assert_order_id_assigned(response: &OrderCommand) -> TestResult<()> {
        if response.order_id == 0 {
            return Err(TestError::Assertion {
                message: "Order ID was not assigned (still 0)".to_string(),
            });
        }
        debug!("Order ID assigned: {}", response.order_id);
        Ok(())
    }

    /// Verify that timestamp was set by journaling processor
    ///
    /// The timestamp should be non-zero for any processed order.
    pub fn assert_timestamp_set(response: &OrderCommand) -> TestResult<()> {
        if response.timestamp == 0 {
            return Err(TestError::Assertion {
                message: "Timestamp was not set (still 0)".to_string(),
            });
        }
        debug!("Timestamp set: {}", response.timestamp);
        Ok(())
    }

    /// Verify the size field matches expected value
    ///
    /// For GTC orders: size = remaining size after matching
    /// For IOC/FOK: size = unfilled size
    pub fn assert_size(response: &OrderCommand, expected: u64) -> TestResult<()> {
        if response.size != expected {
            return Err(TestError::Assertion {
                message: format!(
                    "Size mismatch: expected {}, got {}",
                    expected, response.size
                ),
            });
        }
        debug!("Size verified: {}", response.size);
        Ok(())
    }

    /// Verify that size was reduced from original (indicating a match occurred)
    ///
    /// Used for partial fills or full fills.
    pub fn assert_size_reduced(original_size: u64, response: &OrderCommand) -> TestResult<()> {
        if response.size >= original_size {
            return Err(TestError::Assertion {
                message: format!(
                    "Size was not reduced: original={}, response={}",
                    original_size, response.size
                ),
            });
        }
        debug!("Size reduced: {} -> {}", original_size, response.size);
        Ok(())
    }

    /// Verify size is zero (complete fill)
    pub fn assert_size_zero(response: &OrderCommand) -> TestResult<()> {
        if response.size != 0 {
            return Err(TestError::Assertion {
                message: format!("Expected size=0 (filled), got {}", response.size),
            });
        }
        debug!("Size is zero (fully filled)");
        Ok(())
    }

    /// Verify size is unchanged (no fill)
    pub fn assert_size_unchanged(original_size: u64, response: &OrderCommand) -> TestResult<()> {
        if response.size != original_size {
            return Err(TestError::Assertion {
                message: format!(
                    "Size changed unexpectedly: original={}, response={}",
                    original_size, response.size
                ),
            });
        }
        debug!("Size unchanged: {}", response.size);
        Ok(())
    }

    /// Verify price field matches expected value
    pub fn assert_price(response: &OrderCommand, expected: u64) -> TestResult<()> {
        if response.price != expected {
            return Err(TestError::Assertion {
                message: format!(
                    "Price mismatch: expected {}, got {}",
                    expected, response.price
                ),
            });
        }
        debug!("Price verified: {}", response.price);
        Ok(())
    }

    /// Verify that price was adjusted (for market orders)
    ///
    /// Market buy orders start with price=u64::MAX
    /// Market sell orders start with price=0
    /// Risk engine R1 adjusts these to actual limits
    pub fn assert_price_adjusted_from_market(response: &OrderCommand) -> TestResult<()> {
        if response.price == u64::MAX || response.price == 0 {
            return Err(TestError::Assertion {
                message: format!(
                    "Market order price was not adjusted, still at boundary: {}",
                    response.price
                ),
            });
        }
        debug!("Market order price adjusted to: {}", response.price);
        Ok(())
    }

    /// Verify user_id matches expected
    pub fn assert_user_id(response: &OrderCommand, expected: u64) -> TestResult<()> {
        if response.user_id != expected {
            return Err(TestError::Assertion {
                message: format!(
                    "User ID mismatch: expected {}, got {}",
                    expected, response.user_id
                ),
            });
        }
        debug!("User ID verified: {}", response.user_id);
        Ok(())
    }

    /// Verify market_id matches expected
    pub fn assert_market_id(response: &OrderCommand, expected: u32) -> TestResult<()> {
        if response.market_id != expected {
            return Err(TestError::Assertion {
                message: format!(
                    "Market ID mismatch: expected {}, got {}",
                    expected, response.market_id
                ),
            });
        }
        debug!("Market ID verified: {}", response.market_id);
        Ok(())
    }

    /// Verify client_order_id matches expected
    pub fn assert_client_order_id(response: &OrderCommand, expected: u64) -> TestResult<()> {
        if response.client_order_id != expected {
            return Err(TestError::Assertion {
                message: format!(
                    "Client order ID mismatch: expected {}, got {}",
                    expected, response.client_order_id
                ),
            });
        }
        debug!("Client order ID verified: {}", response.client_order_id);
        Ok(())
    }

    /// Verify order was placed successfully (GTC order that rests on book)
    pub fn assert_placed(response: &OrderCommand) -> TestResult<()> {
        Self::assert_status(response, Status::Placed)?;
        Self::assert_order_id_assigned(response)?;
        Self::assert_timestamp_set(response)?;
        Ok(())
    }

    /// Verify order was fully filled
    pub fn assert_filled(response: &OrderCommand) -> TestResult<()> {
        Self::assert_status(response, Status::Filled)?;
        Self::assert_order_id_assigned(response)?;
        Self::assert_timestamp_set(response)?;
        Self::assert_size_zero(response)?;
        Ok(())
    }

    /// Verify order was partially filled
    pub fn assert_partially_filled(response: &OrderCommand, original_size: u64) -> TestResult<()> {
        Self::assert_status(response, Status::PartiallyFilled)?;
        Self::assert_order_id_assigned(response)?;
        Self::assert_timestamp_set(response)?;
        Self::assert_size_reduced(original_size, response)?;
        Ok(())
    }

    /// Verify order was cancelled
    pub fn assert_cancelled(response: &OrderCommand) -> TestResult<()> {
        Self::assert_status(response, Status::Cancelled)?;
        Ok(())
    }

    /// Verify order was rejected
    pub fn assert_rejected(response: &OrderCommand) -> TestResult<()> {
        Self::assert_status(response, Status::Rejected)?;
        Ok(())
    }

    /// Comprehensive verification for a successful GTC limit order placement (no match)
    pub fn assert_gtc_placed_no_match(
        response: &OrderCommand,
        expected_user: u64,
        expected_market: u32,
        expected_price: u64,
        expected_size: u64,
    ) -> TestResult<()> {
        Self::assert_placed(response)?;
        Self::assert_user_id(response, expected_user)?;
        Self::assert_market_id(response, expected_market)?;
        Self::assert_price(response, expected_price)?;
        Self::assert_size(response, expected_size)?;
        Ok(())
    }

    /// Comprehensive verification for a successful full fill
    pub fn assert_order_fully_filled(
        response: &OrderCommand,
        expected_user: u64,
        expected_market: u32,
    ) -> TestResult<()> {
        Self::assert_filled(response)?;
        Self::assert_user_id(response, expected_user)?;
        Self::assert_market_id(response, expected_market)?;
        Ok(())
    }

    /// Comprehensive verification for a partial fill
    pub fn assert_order_partially_filled(
        response: &OrderCommand,
        original_size: u64,
        expected_user: u64,
        expected_market: u32,
    ) -> TestResult<()> {
        Self::assert_partially_filled(response, original_size)?;
        Self::assert_user_id(response, expected_user)?;
        Self::assert_market_id(response, expected_market)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{OrderCommandType, Side, TimeInForce};

    fn create_test_response(status: Status, order_id: u64, size: u64) -> OrderCommand {
        OrderCommand {
            command: OrderCommandType::PlaceOrder,
            order_id,
            client_order_id: 1,
            market_id: 65538,
            user_id: 1,
            price: 50000,
            size,
            side: Side::Bid,
            time_in_force: TimeInForce::Gtc,
            timestamp: 123456789,
            status,
            events: None,
            balance: [common::UserBalance::default(); 2],
            l2_data: None,
            route_gateway_id: 0,
            original_size: 0,
        }
    }

    #[test]
    fn test_assert_status() {
        let response = create_test_response(Status::Filled, 1, 0);
        assert!(ResponseVerifier::assert_status(&response, Status::Filled).is_ok());
        assert!(ResponseVerifier::assert_status(&response, Status::Placed).is_err());
    }

    #[test]
    fn test_assert_order_id_assigned() {
        let response = create_test_response(Status::Filled, 123, 0);
        assert!(ResponseVerifier::assert_order_id_assigned(&response).is_ok());

        let response_no_id = create_test_response(Status::Filled, 0, 0);
        assert!(ResponseVerifier::assert_order_id_assigned(&response_no_id).is_err());
    }

    #[test]
    fn test_assert_filled() {
        let response = create_test_response(Status::Filled, 123, 0);
        assert!(ResponseVerifier::assert_filled(&response).is_ok());

        let response_partial = create_test_response(Status::PartiallyFilled, 123, 5);
        assert!(ResponseVerifier::assert_filled(&response_partial).is_err());
    }

    #[test]
    fn test_assert_size_reduced() {
        let response = create_test_response(Status::PartiallyFilled, 123, 5);
        assert!(ResponseVerifier::assert_size_reduced(10, &response).is_ok());
        assert!(ResponseVerifier::assert_size_reduced(5, &response).is_err());
        assert!(ResponseVerifier::assert_size_reduced(3, &response).is_err());
    }
}
