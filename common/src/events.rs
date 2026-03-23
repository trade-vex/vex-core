use super::{MarketType, Order, Side};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AssetCreatedEvent {
    pub asset_id: u16,
    pub asset_name: String,
    pub native_scale: u64,
    pub requested_by: u64,
    pub timestamp: u64,
}

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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MarketCreatedEvent {
    pub market_id: u32,
    pub requested_by: u64,
    pub market_type: MarketType,
    pub base_asset: u16,
    pub quote_asset: u16,
    pub base_scale_k: u64,
    pub quote_scale_k: u64,
    pub base_native_scale: u64,
    pub quote_native_scale: u64,
    pub taker_fee: u64,
    pub maker_fee: u64,
    pub slippage: u32,
    pub timestamp: u64,
}
