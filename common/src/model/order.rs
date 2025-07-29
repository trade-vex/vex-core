use super::enums::Side;
use borsh::{BorshDeserialize, BorshSerialize};

pub trait OrderTrait {
    fn price(&self) -> i64;
    fn size(&self) -> i64;
    fn filled(&self) -> i64;
    fn user_id(&self) -> i64;
    fn action(&self) -> Side;
    fn order_id(&self) -> i64;
    fn timestamp(&self) -> i64;
    fn reserve_bid_price(&self) -> i64;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct Order {
    pub order_id: i64,
    pub price: i64,
    pub size: i64,
    pub filled: i64,
    pub reserve_bid_price: i64,
    pub action: Side,
    pub user_id: i64,
    pub timestamp: i64,
}

impl OrderTrait for Order {
    fn price(&self) -> i64 {
        self.price
    }
    fn size(&self) -> i64 {
        self.size
    }
    fn filled(&self) -> i64 {
        self.filled
    }
    fn user_id(&self) -> i64 {
        self.user_id
    }
    fn action(&self) -> Side {
        self.action
    }
    fn order_id(&self) -> i64 {
        self.order_id
    }
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
    fn reserve_bid_price(&self) -> i64 {
        self.reserve_bid_price
    }
}
