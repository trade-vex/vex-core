use borsh::{BorshDeserialize, BorshSerialize};
use num_enum::TryFromPrimitive;
use serde::{Deserialize, Serialize};
use sbe_order::order_action::OrderAction as SbeOrderAction;
use sbe_order::order_type::OrderType as SbeOrderType;
use serde::de::Error;
use serde::de::value::Error as SerdeError;

#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    Copy,
    TryFromPrimitive,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum OrderAction {
    Ask = 0,
    Bid = 1,
}

impl OrderAction {
    pub fn opposite(&self) -> OrderAction {
        match self {
            OrderAction::Ask => OrderAction::Bid,
            OrderAction::Bid => OrderAction::Ask,
        }
    }
}

impl From<OrderAction> for SbeOrderAction {
    fn from(value: OrderAction) -> Self {
        match value {
            OrderAction::Ask => SbeOrderAction::Ask,
            OrderAction::Bid => SbeOrderAction::Bid,
        }
    }
}

impl TryFrom<SbeOrderAction> for OrderAction {
    type Error = SerdeError;
    fn try_from(value: SbeOrderAction) -> Result<Self, Self::Error> {
        match value {
            SbeOrderAction::Ask => Ok(OrderAction::Ask),
            SbeOrderAction::Bid => Ok(OrderAction::Bid),
            SbeOrderAction::NullVal => Err(SerdeError::custom("NullVal")),
        }
    }
}

#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    Copy,
    TryFromPrimitive,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum OrderType {
    // Good till Cancel - equivalent to regular limit order
    Gtc = 0,
    // Immediate or Cancel - equivalent to strict-risk market order
    Ioc = 1,       // with price cap
    IocBudget = 2, // with total amount cap
    // Fill or Kill - execute immediately completely or not at all
    Fok = 3,       // with price cap
    FokBudget = 4, // total amount cap
}

impl From<OrderType> for SbeOrderType {
    fn from(value: OrderType) -> Self {
        match value {
            OrderType::Gtc => SbeOrderType::Gtc,
            OrderType::Ioc => SbeOrderType::Ioc,
            OrderType::IocBudget => SbeOrderType::IocBudget,
            OrderType::Fok => SbeOrderType::Fok,
            OrderType::FokBudget => SbeOrderType::FokBudget,
        }
    }
}

impl TryFrom<SbeOrderType> for OrderType {
    type Error = SerdeError;
    fn try_from(value: SbeOrderType) -> Result<Self, Self::Error> {
        match value {
            SbeOrderType::Gtc => Ok(OrderType::Gtc),
            SbeOrderType::Ioc => Ok(OrderType::Ioc),
            SbeOrderType::IocBudget => Ok(OrderType::IocBudget),
            SbeOrderType::Fok => Ok(OrderType::Fok),
            SbeOrderType::FokBudget => Ok(OrderType::FokBudget),
            SbeOrderType::NullVal => Err(SerdeError::custom("NullVal")),
        }
    }
}

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
pub enum SymbolType {
    FuturesContract = 0,
    CurrencyExchangePair = 1,
    Option = 2,
}

impl Default for SymbolType {
    fn default() -> Self {
        SymbolType::CurrencyExchangePair
    }
}

impl SymbolType {
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
    // Can happen only when MARKET order has to be rejected by Matcher Engine due lack of liquidity
    // That basically means no ASK (or BID) orders left in the order book for any price.
    // Before being rejected active order can be partially filled.
    Reject,
    // After cancel/reduce order - risk engine should unlock deposit accordingly
    Reduce,
    // Custom binary data attached
    BinaryEvent,
    Cancel,
    OrderPlaced, // New event type for order placement
}

#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    Copy,
    TryFromPrimitive,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum PositionDirection {
    Empty = 0,
    Long = 1,
    Short = 2,
}

impl PositionDirection {
    pub fn of(action: OrderAction) -> Self {
        match action {
            OrderAction::Bid => Self::Long,
            OrderAction::Ask => Self::Short,
        }
    }

    pub fn multiplier(&self) -> i8 {
        match self {
            PositionDirection::Empty => 0,
            PositionDirection::Long => 1,
            PositionDirection::Short => -1,
        }
    }
}