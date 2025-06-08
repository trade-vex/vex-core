use borsh::{BorshDeserialize, BorshSerialize};
use num_enum::TryFromPrimitive;
use serde::{Deserialize, Serialize};

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
    Ioc = 1, // with price cap
    IocBudget = 2, // with total amount cap
    // Fill or Kill - execute immediately completely or not at all
    Fok = 3, // with price cap
    FokBudget = 4, // total amount cap
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
    Debug,
    PartialEq,
    Eq,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
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