use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

pub const L2_SIZE: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct L2MarketData {
    pub ask_prices: Vec<u64>,
    pub ask_volumes: Vec<u64>,
    pub ask_orders: Vec<u64>,
    pub bid_prices: Vec<u64>,
    pub bid_volumes: Vec<u64>,
    pub bid_orders: Vec<u64>,
    pub timestamp: u64,
    pub reference_seq: u64,
}

impl L2MarketData {
    pub fn new(
        ask_prices: Vec<u64>,
        ask_volumes: Vec<u64>,
        ask_orders: Vec<u64>,
        bid_prices: Vec<u64>,
        bid_volumes: Vec<u64>,
        bid_orders: Vec<u64>,
    ) -> Self {
        Self {
            ask_prices,
            ask_volumes,
            ask_orders,
            bid_prices,
            bid_volumes,
            bid_orders,
            timestamp: 0,
            reference_seq: 0,
        }
    }

    pub fn with_size(ask_size: usize, bid_size: usize) -> Self {
        Self {
            ask_prices: vec![0; ask_size],
            ask_volumes: vec![0; ask_size],
            ask_orders: vec![0; ask_size],
            bid_prices: vec![0; bid_size],
            bid_volumes: vec![0; bid_size],
            bid_orders: vec![0; bid_size],
            timestamp: 0,
            reference_seq: 0,
        }
    }

    pub fn ask_size(&self) -> usize {
        self.ask_prices.len()
    }

    pub fn bid_size(&self) -> usize {
        self.bid_prices.len()
    }

    pub fn total_order_book_volume_ask(&self) -> u64 {
        self.ask_volumes.iter().sum()
    }

    pub fn total_order_book_volume_bid(&self) -> u64 {
        self.bid_volumes.iter().sum()
    }
}
