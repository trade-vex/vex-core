mod cmd;
mod core_arithmetic;
mod events;
mod l2_market_data;
mod logging;
mod market_specification;
mod order;
mod snowflake;
mod user_profile;

pub use cmd::{
    MatcherTradeEvent, ORDERCOMMANDSIZE, OrderCommand, Status, decode_order_command,
    encode_order_command,
};
pub use core_arithmetic::CoreArithmetic;
pub use events::{
    BalanceEvent, CancelOrderEvent, OrderEvent, OrderbookEvent, OrderbookLevel, TradeEvent,
};
pub use l2_market_data::L2MarketData;
pub use market_specification::{
    CoreMarketSpecification, CoreMarketSpecificationBuilder, base_asset, quote_asset,
};
pub use order::{Order, PriceCache};
pub use snowflake::Snowflake;
pub use user_profile::{BalanceError, BalanceKey, BalanceStore, UserBalance};

use borsh::{BorshDeserialize, BorshSerialize};
use sbe_order::order_command_type::OrderCommandType as SbeOrderCommandType;
use sbe_order::side::Side as SbeSide;
use sbe_order::time_in_force::TimeInForce as SbeTimeInForce;
use serde::de::Error;
use serde::de::value::Error as SerdeError;
use serde::{Deserialize, Serialize};

pub const MAX_GATEWAYS: usize = 16;

pub const L2SIZE: usize = 10;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
)]
#[borsh(use_discriminant = true)]
#[derive(Default)]
pub enum MarketType {
    FuturesContract = 0,
    #[default]
    Spot = 1,
    Option = 2,
}

impl MarketType {
    pub fn code(&self) -> u8 {
        *self as u8
    }
}

#[derive(
    Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
#[borsh(use_discriminant = true)]
pub enum MatcherEventType {
    // Trade event
    // Can be triggered by place ORDER or for MOVE order command.
    Trade,
    // Reject event
    // Can happen only when MARKET order has to be rejected by Matcher Engine due lack of liquser_idity
    // That basically means no ASK (or BID) orders left in the order book for any price.
    // Before being rejected active order can be partially filled.
    Rejected,
    Cancelled,
    Placed, // New event type for order placement
}

/// The specific action the command represents.
///
/// This serves as the primary discriminant for the `OrderCommand` struct.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum OrderCommandType {
    /// A command to place a new order. All fields in `OrderCommand` are relevant.
    PlaceOrder,
    /// A command to cancel an existing order. Only `order_id`, `user_id`, and
    /// `symbol_id` are relevant. Other fields should be ignored.
    CancelOrder,
    /// Deposit funds to a user's account. Only `user_id` and `amount`, `market` are relevant, where market id is used as asset id.
    DepositFunds,
    /// Withdraw funds from a user's account. Only `user_id` and `amount`, `market` are relevant, where market id is used as asset id.
    WithdrawFunds,
}

impl TryFrom<SbeOrderCommandType> for OrderCommandType {
    type Error = SerdeError;

    fn try_from(value: SbeOrderCommandType) -> Result<Self, Self::Error> {
        match value {
            SbeOrderCommandType::PlaceOrder => Ok(OrderCommandType::PlaceOrder),
            SbeOrderCommandType::CancelOrder => Ok(OrderCommandType::CancelOrder),
            SbeOrderCommandType::DepositFunds => Ok(OrderCommandType::DepositFunds),
            SbeOrderCommandType::WithdrawFunds => Ok(OrderCommandType::WithdrawFunds),
            SbeOrderCommandType::NullVal => Err(SerdeError::custom("NullVal")), // Maybe handle NullVal specially
        }
    }
}

impl From<OrderCommandType> for SbeOrderCommandType {
    fn from(val: OrderCommandType) -> Self {
        match val {
            OrderCommandType::PlaceOrder => SbeOrderCommandType::PlaceOrder,
            OrderCommandType::CancelOrder => SbeOrderCommandType::CancelOrder,
            OrderCommandType::DepositFunds => SbeOrderCommandType::DepositFunds,
            OrderCommandType::WithdrawFunds => SbeOrderCommandType::WithdrawFunds,
        }
    }
}

/// The time-in-force policy for a `PlaceOrder` command.
#[derive(
    Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
#[repr(u8)]
pub enum TimeInForce {
    /// Good-Till-Canceled: The order rests on the book until filled or canceled.
    /// This policy is only valid for limit orders.
    Gtc,
    /// Immediate-Or-Cancel: The order executes against any available volume
    /// immediately and any unfilled portion is canceled.
    Ioc,
    /// Fill-Or-Kill: The order must be filled in its entirety immediately,
    /// otherwise the entire order is canceled.
    Fok,
}

impl TryFrom<SbeTimeInForce> for TimeInForce {
    type Error = SerdeError;

    fn try_from(value: SbeTimeInForce) -> Result<Self, Self::Error> {
        match value {
            SbeTimeInForce::Gtc => Ok(TimeInForce::Gtc),
            SbeTimeInForce::Ioc => Ok(TimeInForce::Ioc),
            SbeTimeInForce::Fok => Ok(TimeInForce::Fok),
            _ => Err(SerdeError::custom("Unknown TimeInForce variant")),
        }
    }
}

impl From<TimeInForce> for SbeTimeInForce {
    fn from(val: TimeInForce) -> Self {
        match val {
            TimeInForce::Gtc => SbeTimeInForce::Gtc,
            TimeInForce::Ioc => SbeTimeInForce::Ioc,
            TimeInForce::Fok => SbeTimeInForce::Fok,
        }
    }
}

/// Represents the side of the order book.
#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    BorshSerialize,
    BorshDeserialize,
    Serialize,
    Deserialize,
)]
#[repr(u8)]
pub enum Side {
    Ask,
    Bid,
}

impl Side {
    pub fn op_side(&self) -> Self {
        match self {
            Side::Ask => Side::Bid,
            Side::Bid => Side::Ask,
        }
    }
}

impl TryFrom<SbeSide> for Side {
    type Error = SerdeError;

    fn try_from(value: SbeSide) -> Result<Self, Self::Error> {
        match value {
            SbeSide::Ask => Ok(Side::Ask),
            SbeSide::Bid => Ok(Side::Bid),
            SbeSide::NullVal => Err(SerdeError::custom("NullVal")), // Maybe handle NullVal specially
        }
    }
}

impl From<Side> for SbeSide {
    fn from(val: Side) -> Self {
        match val {
            Side::Ask => SbeSide::Ask,
            Side::Bid => SbeSide::Bid,
        }
    }
}
