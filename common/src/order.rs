use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use crate::Side;

#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize, Deserialize)]
pub struct Order {
    pub order_id: u64,
    pub user_id: u64,
    pub price: u64,
    pub size: u64,
    pub side: Side,
    pub timestamp: u64,
}

impl Order {
    pub fn price(&self) -> u64 {
        self.price
    }
    pub fn size(&self) -> u64 {
        self.size
    }
    pub fn user_id(&self) -> u64 {
        self.user_id
    }
    pub fn side(&self) -> Side {
        self.side
    }
    pub fn order_id(&self) -> u64 {
        self.order_id
    }
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}
