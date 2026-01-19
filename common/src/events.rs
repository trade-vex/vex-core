use super::{Order, Side, Status, TimeInForce};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BalanceEvent {
    pub user_id: u64,
    pub asset_id: u16,
    pub available: u64,
    pub locked: u64,
    pub total: u64,
    pub timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OrderEvent {
    pub order: Order,
    pub market_id: u32,
    pub time_in_force: TimeInForce,
    pub status: Status,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TradeEvent {
    pub maker_user_id: u64,
    pub taker_user_id: u64,
    pub market_id: u32,
    pub price: u64,
    pub size: u64,
    pub maker_order_id: u64,
    pub taker_order_id: u64,
    pub taker_side: Side,
    pub timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CancelOrderEvent {
    pub order_id: u64,
    pub market_id: u32,
    pub user_id: u64,
    pub timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OrderbookEvent {
    pub market_id: u32,
    pub bids: Vec<OrderbookLevel>,
    pub asks: Vec<OrderbookLevel>,
    pub timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OrderbookLevel {
    pub price: u64,
    pub size: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DepositEvent {
    pub user_id: u64,
    pub asset_id: u16,
    pub amount: u64,
    pub timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WithdrawEvent {
    pub user_id: u64,
    pub asset_id: u16,
    pub amount: u64,
    pub timestamp: u64,
}
