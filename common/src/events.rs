use serde::{Deserialize, Serialize};
use super::Order;

#[derive(Serialize, Deserialize, Debug)]
pub struct BalanceEvent {
    pub user_id: u64,
    pub asset_id: u16,
    pub available: u64,
    pub locked: u64,
    pub total: u64,
    pub timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OrderEvent {
    pub order: Order,
    pub market_id: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TradeEvent {
    pub maker_user_id: u64,
    pub taker_user_id: u64,
    pub market_id: u32,
    pub price: u64,
    pub size: u64,
    pub maker_order_id: u64,
    pub taker_order_id: u64,
    pub timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CancelOrderEvent {
    pub order_id: u64,
    pub market_id: u32,
    pub user_id: u64,
    pub timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OrderbookEvent {
    pub market_id: u32,
    pub bids: Vec<OrderbookLevel>,
    pub asks: Vec<OrderbookLevel>,
    pub timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OrderbookLevel {
    pub price: u64,
    pub size: u64,
}