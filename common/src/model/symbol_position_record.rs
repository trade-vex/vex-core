use borsh::{BorshDeserialize, BorshSerialize};
use crate::model::enums::PositionDirection;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct SymbolPositionRecord {
    pub uid: i64,
    pub symbol: i32,
    pub currency: i32,
    pub direction: PositionDirection,
    pub open_volume: i64,
    pub open_price_sum: i64,
    pub profit: i64,
    pub pending_sell_size: i64,
    pub pending_buy_size: i64,
} 