use borsh::{BorshDeserialize, BorshSerialize};
use num_enum::TryFromPrimitive;
use sbe_order::order_type::OrderType as SbeOrderType;
use sbe_order::side::Side as SbeSide;
use serde::{Deserialize, Serialize};

use crate::cmd::OrderCommandSerializationError;

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

impl TryFrom<SbeSide> for OrderAction {
    type Error = OrderCommandSerializationError;

    fn try_from(value: SbeSide) -> Result<Self, Self::Error> {
        match value {
            SbeSide::Bid => Ok(Self::Bid),
            SbeSide::Ask => Ok(Self::Ask),
            SbeSide::NullVal => Err(OrderCommandSerializationError::UnsupportedSbeOrderAction(value as u8)),
        }
    }
}

impl From<OrderAction> for SbeSide {
    fn from(val: OrderAction) -> Self {
        match val {
            OrderAction::Ask => SbeSide::Ask,
            OrderAction::Bid => SbeSide::Bid,
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

impl TryFrom<SbeOrderType> for OrderType {
    type Error = OrderCommandSerializationError;

    fn try_from(value: SbeOrderType) -> Result<Self, Self::Error> {
        match value {
            SbeOrderType::Gtc => Ok(Self::Gtc),
            SbeOrderType::Ioc => Ok(Self::Ioc),
            SbeOrderType::IocBudget => Ok(Self::IocBudget),
            SbeOrderType::Fok => Ok(Self::Fok),
            SbeOrderType::FokBudget => Ok(Self::FokBudget),
            SbeOrderType::NullVal => Err(OrderCommandSerializationError::UnsupportedSbeOrderType(value as u8)),
        }
    }
}

impl From<OrderType> for SbeOrderType {
    fn from(val: OrderType) -> Self {
        match val {
            OrderType::Gtc => SbeOrderType::Gtc,
            OrderType::Ioc => SbeOrderType::Ioc,
            OrderType::IocBudget => SbeOrderType::IocBudget,
            OrderType::Fok => SbeOrderType::Fok,
            OrderType::FokBudget => SbeOrderType::FokBudget,
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
pub enum SymbolType {
    CurrencyExchangePair = 0,
    FuturesContract = 1,
    Option = 2,
}

impl SymbolType {
    pub fn code(&self) -> u8 {
        *self as u8
    }

    pub fn of(code: u8) -> Result<Self, num_enum::TryFromPrimitiveError<Self>> {
        Self::try_from(code)
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

/// Type of balance modification:
///
/// * `Adjustment` — Changes account balance (deposits, withdrawals, corrections).
/// * `Suspend` — Removes inactive client profile to improve performance.
///   Balances should first be set to zero; no open margin positions allowed.
///   Profiles may resume with positions/balances if pending orders or commands
///   were unprocessed, so resume must handle merging.
///
/// Used in the BALANCE_ADJUSTMENT command (which is a TODO OrderCommand for now) by the core engine to decide how to
/// modify or remove a client’s account state.

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
pub enum BalanceAdjustmentType {
    Adjustment = 0,
    Suspend = 1,
}
