//! This module contains core arithmetic functions for financial calculations,
//! mirroring the logic from the Java `CoreArithmeticUtils` class.
use crate::model::symbol_specification::CoreSymbolSpecification;

pub struct CoreArithmetic;

impl CoreArithmetic {
    /// Calculate the required amount for an ASK order based on its size and the symbol spec.
    pub fn calculate_amount_ask(size: i64, spec: &CoreSymbolSpecification) -> i64 {
        size * spec.base_scale_k
    }

    /// Calculate the required amount for a BID order based on its size, price, and the symbol spec.
    pub fn calculate_amount_bid(size: i64, price: i64, spec: &CoreSymbolSpecification) -> i64 {
        size * (price * spec.quote_scale_k)
    }

    /// Calculate the amount for a BID order including the taker fee.
    pub fn calculate_amount_bid_taker_fee(size: i64, price: i64, spec: &CoreSymbolSpecification) -> i64 {
        size * (price * spec.quote_scale_k + spec.taker_fee)
    }

    /// Calculate the correction amount to be released for a BID order when a maker is involved.
    pub fn calculate_amount_bid_release_corr_maker(size: i64, price_diff: i64, spec: &CoreSymbolSpecification) -> i64 {
        size * (price_diff * spec.quote_scale_k + (spec.taker_fee - spec.maker_fee))
    }

    /// Calculate the required budget for a BID order including the taker fee.
    pub fn calculate_amount_bid_taker_fee_for_budget(size: i64, budget_in_steps: i64, spec: &CoreSymbolSpecification) -> i64 {
        budget_in_steps * spec.quote_scale_k + size * spec.taker_fee
    }
}
