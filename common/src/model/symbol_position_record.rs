use crate::model::enums::OrderAction;
use crate::model::enums::PositionDirection;
use borsh::{BorshDeserialize, BorshSerialize};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct SymbolPositionRecord {
    pub uid: i64,
    pub symbol: i32,
    pub base_currency: i32,
    pub quote_currency: i32,
    pub direction: PositionDirection,
    pub open_volume: i64,
    pub open_price_sum: i64,
    pub profit: i64,
    pub pending_sell_size: i64,
    pub pending_buy_size: i64,
}

impl SymbolPositionRecord {
    pub fn new(uid: i64, symbol: i32, base_currency: i32, quote_currency: i32) -> Self {
        Self {
            uid,
            symbol,
            base_currency,
            quote_currency,
            direction: PositionDirection::Empty,
            open_volume: 0,
            open_price_sum: 0,
            profit: 0,
            pending_buy_size: 0,
            pending_sell_size: 0,
        }
    }

    pub fn hold(&mut self, amount: i64, action: OrderAction) {
        if action == OrderAction::Bid {
            self.pending_buy_size += amount;
        } else {
            self.pending_sell_size += amount;
        }
    }

    pub fn release(&mut self, amount: i64, action: OrderAction) {
        match action {
            OrderAction::Bid => self.pending_buy_size -= amount,
            OrderAction::Ask => self.pending_sell_size -= amount,
        }
    }

    /// Settles a trade by reducing the held amount.
    pub fn settle(&mut self, amount: i64, action: OrderAction) {
        match action {
            OrderAction::Bid => self.pending_buy_size -= amount,
            OrderAction::Ask => self.pending_sell_size -= amount,
        }
    }

    pub fn add_trade(&mut self, trade_price: i64, trade_size: i64, taker_action: OrderAction) {
        let trade_direction = if taker_action == OrderAction::Bid {
            PositionDirection::Short
        } else {
            PositionDirection::Long
        };

        if self.direction == PositionDirection::Empty {
            self.direction = trade_direction;
            self.open_volume = trade_size;
            self.open_price_sum = trade_size * trade_price;
        } else if self.direction == trade_direction {
            self.open_volume += trade_size;
            self.open_price_sum += trade_size * trade_price;
        } else {
            if self.open_volume == trade_size {
                self.direction = PositionDirection::Empty;
                self.open_volume = 0;
                self.open_price_sum = 0;
            } else if self.open_volume > trade_size {
                self.open_volume -= trade_size;
                let avg_price = self.open_price_sum / self.open_volume;
                self.profit += (trade_price - avg_price) * trade_size;
                self.open_price_sum -= trade_size * avg_price;
            } else {
                self.direction = trade_direction;
                self.profit +=
                    (trade_price - self.open_price_sum / self.open_volume) * self.open_volume;
                self.open_volume = trade_size - self.open_volume;
                self.open_price_sum = self.open_volume * trade_price;
            }
        }
    }
}
