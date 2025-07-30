use borsh::{BorshDeserialize, BorshSerialize};
use common::model::enums::Side;
use common::model::l2_market_data::L2MarketData;
use common::model::order::{Order, OrderTrait};
use common::model::symbol_specification::CoreSymbolSpecification;
use std::fmt;

pub use common::cmd::{MatcherTradeEvent, OrderCommand, OrderCommandType};
pub use common::model::enums::SymbolType;

pub mod direct_impl;
pub mod events;
pub mod naive_impl;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum OrderBookError {
    UnsupportedCommand,
    UnknownOrderId,
    DuplicateOrderId,
    MoveFailedPriceOverRiskLimit,
    ReduceFailedWrongSize,
    InvalidArguments,
    InsufficientFunds,
}

impl fmt::Display for OrderBookError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderBookError::UnsupportedCommand => write!(f, "Unsupported command"),
            OrderBookError::UnknownOrderId => write!(f, "Unknown order ID"),
            OrderBookError::DuplicateOrderId => write!(f, "Duplicate order ID"),
            OrderBookError::MoveFailedPriceOverRiskLimit => {
                write!(f, "Move failed: price is over risk limit")
            }
            OrderBookError::ReduceFailedWrongSize => write!(f, "Reduce failed: invalid size"),
            OrderBookError::InvalidArguments => write!(f, "Invalid arguments"),
            OrderBookError::InsufficientFunds => write!(f, "Insufficient funds"),
        }
    }
}

impl std::error::Error for OrderBookError {}

pub trait OrderBook<'a> {
    fn new_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError>;
    fn cancel_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError>;
    fn reduce_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError>;
    fn move_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError>;
    fn get_orders_num(&self, side: Side) -> i32;
    fn get_total_orders_volume(&self, side: Side) -> i64;
    fn get_order_by_id(&self, order_id: i64) -> Option<&dyn OrderTrait>;
    fn find_user_orders(&self, user_id: i64) -> Vec<Order>;
    fn ask_orders_stream(
        &'a self,
        sorted: bool,
    ) -> Box<dyn Iterator<Item = &'a dyn OrderTrait> + 'a>;
    fn bid_orders_stream(
        &'a self,
        sorted: bool,
    ) -> Box<dyn Iterator<Item = &'a dyn OrderTrait> + 'a>;
    fn get_l2_market_data_snapshot(&self, size: usize) -> L2MarketData;
    fn publish_l2_market_data_snapshot(&self, data: &mut L2MarketData);
    fn fill_asks(&self, size: usize, data: &mut L2MarketData);
    fn fill_bids(&self, size: usize, data: &mut L2MarketData);
    fn get_total_ask_buckets(&self, limit: usize) -> usize;
    fn get_total_bid_buckets(&self, limit: usize) -> usize;
    fn get_implementation_type(&self) -> OrderBookImplType;
    fn get_symbol_spec(&self) -> &CoreSymbolSpecification;
    fn validate_internal_state(&self);
}

pub fn from_bytes<'a>(
    bytes: &mut &'a [u8],
) -> Result<Box<dyn OrderBook<'a> + 'a>, borsh::io::Error> {
    let impl_type = OrderBookImplType::deserialize(bytes)?;
    match impl_type {
        OrderBookImplType::Naive => {
            let book = naive_impl::OrderBookNaiveImpl::from_bytes(bytes)?;
            Ok(Box::new(book))
        }
        OrderBookImplType::Direct => {
            let book = direct_impl::OrderBookDirectImpl::from_bytes(bytes)?;
            Ok(Box::new(book))
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, BorshSerialize, BorshDeserialize)]
pub enum OrderBookImplType {
    Naive,
    Direct,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct MatcherResult {
    pub volume: i64,
    pub orders_to_remove: Vec<i64>,
}
