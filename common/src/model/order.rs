use borsh::{BorshDeserialize, BorshSerialize};

pub trait OrderTrait {
    fn price(&self) -> u64;
    fn size(&self) -> u64;
    fn filled(&self) -> u64;
    fn user_id(&self) -> u64;
    fn side(&self) -> Side;
    fn order_id(&self) -> u64;
    fn timestamp(&self) -> u64;
    fn reserve_bid_price(&self) -> u64;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct Order {
    pub order_id: u64,
    pub price: u64,
    pub size: u64,
    pub filled: u64,
    pub reserve_bid_price: u64,
    pub side: Side,
    pub user_id: u64,
    pub timestamp: u64,
}

impl OrderTrait for Order {
    fn price(&self) -> u64 {
        self.price
    }
    fn size(&self) -> u64 {
        self.size
    }
    fn filled(&self) -> u64 {
        self.filled
    }
    fn user_id(&self) -> u64 {
        self.user_id
    }
    fn side(&self) -> Side {
        self.side
    }
    fn order_id(&self) -> u64 {
        self.order_id
    }
    fn timestamp(&self) -> u64 {
        self.timestamp
    }
    fn reserve_bid_price(&self) -> u64 {
        self.reserve_bid_price
    }
}
