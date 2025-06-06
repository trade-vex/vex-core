use super::enums::OrderAction;
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

pub trait IOrder {
    fn price(&self) -> i64;
    fn size(&self) -> i64;
    fn filled(&self) -> i64;
    fn uid(&self) -> i64;
    fn action(&self) -> OrderAction;
    fn order_id(&self) -> i64;
    fn timestamp(&self) -> i64;
    fn reserve_bid_price(&self) -> i64;
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
)]
pub struct Order {
    pub order_id: i64,
    pub price: i64,
    pub size: i64,
    pub filled: i64,
    pub reserve_bid_price: i64,
    pub action: OrderAction,
    pub uid: i64,
    pub timestamp: i64,
}

impl IOrder for Order {
    fn price(&self) -> i64 {
        self.price
    }
    fn size(&self) -> i64 {
        self.size
    }
    fn filled(&self) -> i64 {
        self.filled
    }
    fn uid(&self) -> i64 {
        self.uid
    }
    fn action(&self) -> OrderAction {
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