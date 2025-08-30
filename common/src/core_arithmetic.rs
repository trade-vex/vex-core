//! This module contains core arithmetic functions for financial calculations,
//! mirroring the logic from the Java `CoreArithmeticUtils` class.
use crate::market_specification::CoreMarketSpecification;

pub struct CoreArithmetic;

impl CoreArithmetic {
    /// Calculate the required amount for an ASK order based on its size and the symbol spec.
    pub fn calculate_amount_ask(size: u64, spec: &CoreMarketSpecification) -> u64 {
        size * spec.base_scale_k
    }

    /// Calculate the required amount for a BID order based on its size, price, and the symbol spec.
    pub fn calculate_amount_bid(size: u64, price: u64, spec: &CoreMarketSpecification) -> u64 {
        size * (price * spec.quote_scale_k)
    }

    /// Calculate the amount for a BID order including the taker fee.
    pub fn calculate_amount_bid_taker_fee(
        size: u64,
        price: u64,
        spec: &CoreMarketSpecification,
    ) -> u64 {
        size * (price * spec.quote_scale_k + spec.taker_fee)
    }

    /// Calculate the correction amount to be released for a BID order when a maker is involved.
    pub fn calculate_amount_bid_release_corr_maker(
        size: u64,
        price_diff: u64,
        spec: &CoreMarketSpecification,
    ) -> u64 {
        size * (price_diff * spec.quote_scale_k + (spec.taker_fee - spec.maker_fee))
    }

    /// Calculate the required budget for a BID order including the taker fee.
    pub fn calculate_amount_bid_taker_fee_for_budget(
        size: u64,
        budget_in_steps: u64,
        spec: &CoreMarketSpecification,
    ) -> u64 {
        budget_in_steps * spec.quote_scale_k + size * spec.taker_fee
    }
}
