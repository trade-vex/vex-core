use crate::model::symbol_position_record::SymbolPositionRecord;
use borsh::{BorshDeserialize, BorshSerialize};
use hashbrown::HashMap;

use crate::model::enums::OrderAction;
use crate::model::symbol_specification::CoreSymbolSpecification;
// TODO ...
// positions: IntObjectHashMap<SymbolPositionRecord>
// accounts: IntLongHashMap

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct UserProfile {
    pub uid: i64,
    pub adjustments_counter: i64,
    pub user_status: UserStatus,
    pub positions: HashMap<i32, SymbolPositionRecord>,
    pub accounts: HashMap<i32, i64>,
}

impl UserProfile {
    pub fn new(uid: i64, user_status: UserStatus) -> Self {
        Self {
            uid,
            adjustments_counter: 0,
            user_status,
            positions: HashMap::new(),
            accounts: HashMap::new(),
        }
    }

    /// Puts funds on hold for a new order.
    /// Returns true if successful, false if insufficient funds.
    pub fn hold_funds(
        &mut self,
        spec: &CoreSymbolSpecification,
        amount: i64,
        action: OrderAction,
    ) -> bool {
        let position = self.positions.entry(spec.symbol_id).or_insert_with(|| {
            SymbolPositionRecord::new(self.uid, spec.symbol_id, spec.base_currency, spec.quote_currency)
        });

        let currency = if action == OrderAction::Bid {
            position.quote_currency
        } else {
            position.base_currency
        };

        let account_balance = self.accounts.entry(currency).or_insert(0);

        if *account_balance >= amount {
            *account_balance -= amount;
            position.hold(amount, action);
            true
        } else {
            false
        }
    }

    /// Releases previously held funds after an order is cancelled or reduced.
    pub fn release_funds(&mut self, symbol: i32, amount: i64, action: OrderAction) {
        if let Some(position) = self.positions.get_mut(&symbol) {
            position.release(amount, action);
            let currency = if action == OrderAction::Bid {
                position.quote_currency
            } else {
                position.base_currency
            };
            if let Some(balance) = self.accounts.get_mut(&currency) {
                *balance += amount;
            }
        }
    }

    /// Settles a trade, adjusting balances and positions.
    pub fn settle_trade(
        &mut self,
        spec: &CoreSymbolSpecification,
        price: i64,
        size: i64,
        action: OrderAction,
    ) {
        let base_currency = spec.base_currency;
        let quote_currency = spec.quote_currency;
        let trade_amount = price * size;

        match action {
            OrderAction::Bid => { // User is a BUYER
                // The quote currency was already debited by `hold_funds`.
                // We only need to credit the base currency they received.
                *self.accounts.entry(base_currency).or_insert(0) += size;
            }
            OrderAction::Ask => { // User is a SELLER
                // Credit the quote currency account for the sale.
                *self.accounts.entry(quote_currency).or_insert(0) += trade_amount;
                // The base currency was already debited by `hold_funds`.
            }
        }

        // Finally, update the position to clear the held funds for this trade.
        if let Some(position) = self.positions.get_mut(&spec.symbol_id) {
            // Pass the correct amount to settle based on the action.
            // For a BID, the held amount was the trade value (quote currency).
            // For an ASK, the held amount was the trade size (base currency).
            let amount_to_settle = match action {
                OrderAction::Bid => trade_amount,
                OrderAction::Ask => size,
            };
            position.settle(amount_to_settle, action);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum UserStatus {
    Active,
    Suspended,
}
