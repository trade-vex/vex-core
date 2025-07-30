use crate::model::enums::PositionDirection;
use crate::model::enums::Side;
use borsh::{BorshDeserialize, BorshSerialize};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct SymbolPositionRecord {
    pub user_id: u64,
    pub symbol_id: u32,
    pub base_currency: u32,
    pub quote_currency: u32,
    pub direction: PositionDirection,
    pub open_volume: u64,
    pub open_price_sum: u64,
    pub profit: u64,
    pub pending_sell_size: u64,
    pub pending_buy_size: u64,
}

impl SymbolPositionRecord {
    pub fn new(user_id: u64, symbol_id: u32, base_currency: u32, quote_currency: u32) -> Self {
        Self {
            user_id,
            symbol_id,
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

    pub fn hold(&mut self, amount: u64, side: Side) {
        if side == Side::Bid {
            self.pending_buy_size += amount;
        } else {
            self.pending_sell_size += amount;
        }
    }

    pub fn release(&mut self, amount: u64, side: Side) {
        match side {
            Side::Bid => self.pending_buy_size -= amount,
            Side::Ask => self.pending_sell_size -= amount,
        }
    }

    /// Settles a trade by reducing the held amount.
    pub fn settle(&mut self, amount: u64, side: Side) {
        match side {
            Side::Bid => self.pending_buy_size -= amount,
            Side::Ask => self.pending_sell_size -= amount,
        }
    }

    pub fn add_trade(&mut self, trade_price: u64, trade_size: u64, taker_action: Side) {
        let trade_direction = if taker_action == Side::Bid {
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
