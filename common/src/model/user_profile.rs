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
        symbol: i32,
        trade_price: i64,
        trade_size: i64,
        taker_action: OrderAction,
    ) {
        if let Some(position) = self.positions.get_mut(&symbol) {
            let (amount, currency) = if taker_action == OrderAction::Bid {
                // Taker is buying, so this user is selling (Ask)
                // Release held base currency, receive quote currency
                position.release(trade_size, OrderAction::Ask);
                (trade_price * trade_size, position.quote_currency)
            } else {
                // Taker is selling, so this user is buying (Bid)
                // Release held quote currency, receive base currency
                position.release(trade_price * trade_size, OrderAction::Bid);
                (trade_size, position.base_currency)
            };

            if let Some(balance) = self.accounts.get_mut(&currency) {
                *balance += amount;
            }

            position.add_trade(trade_price, trade_size, taker_action);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum UserStatus {
    Active,
    Suspended,
}
