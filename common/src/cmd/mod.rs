use crate::model::enums::{MatcherEventType, OrderType, Side};
use crate::model::order::OrderTrait;
use borsh::{BorshDeserialize, BorshSerialize};
use sbe_order::message_header_codec::{self, MessageHeaderDecoder};
use sbe_order::order_command_message_codec::{
    OrderCommandMessageDecoder, OrderCommandMessageEncoder,
};
use sbe_order::order_command_type::OrderCommandType as SbeOrderCommandType;
use sbe_order::{ReadBuf, SbeResult, WriteBuf};
use serde::de::Error;
use serde::de::value::Error as SerdeError;

/// Order Command Type
#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum OrderCommandType {
    PlaceLimitOrder,
    PlaceMarketOrder,
    CancelOrder,
}

use std::convert::TryFrom;

impl TryFrom<SbeOrderCommandType> for OrderCommandType {
    type Error = SerdeError;

    fn try_from(value: SbeOrderCommandType) -> Result<Self, Self::Error> {
        match value {
            SbeOrderCommandType::PlaceLimitOrder => Ok(OrderCommandType::PlaceLimitOrder),
            SbeOrderCommandType::PlaceMarketOrder => Ok(OrderCommandType::PlaceMarketOrder),
            SbeOrderCommandType::CancelOrder => Ok(OrderCommandType::CancelOrder),
            SbeOrderCommandType::NullVal => Err(SerdeError::custom("NullVal")), // Maybe handle NullVal specially
        }
    }
}

impl From<OrderCommandType> for SbeOrderCommandType {
    fn from(value: OrderCommandType) -> Self {
        match value {
            OrderCommandType::PlaceLimitOrder => SbeOrderCommandType::PlaceLimitOrder,
            OrderCommandType::PlaceMarketOrder => SbeOrderCommandType::PlaceMarketOrder,
            OrderCommandType::CancelOrder => SbeOrderCommandType::CancelOrder,
        }
    }
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct OrderCommand {
    pub command: OrderCommandType,
    pub order_id: u64,
    pub symbol_id: u32,
    pub user_id: u64,
    pub price: u64,
    pub reserve_bid_price: u64,
    pub size: u64,
    pub side: Side,
    pub order_type: OrderType,
    pub timestamp: u64,
    pub matcher_event: Option<Box<MatcherTradeEvent>>,
}
impl Default for OrderCommand {
    fn default() -> Self {
        Self {
            command: OrderCommandType::PlaceLimitOrder,
            order_id: 0,
            symbol_id: 0,
            user_id: 0,
            price: 0,
            reserve_bid_price: 0,
            size: 0,
            side: Side::Ask,            // Default side
            order_type: OrderType::Gtc, // Default order type
            timestamp: 0,
            matcher_event: None,
        }
    }
}
impl OrderCommand {
    pub fn new_order(
        order_type: OrderType,
        order_id: u64,
        user_id: u64,
        price: u64,
        reserve_bid_price: u64,
        size: u64,
        side: Side,
    ) -> Self {
        Self {
            command: OrderCommandType::PlaceLimitOrder,
            order_id,
            symbol_id: 0,
            user_id,
            price,
            reserve_bid_price,
            size,
            side,
            order_type,
            timestamp: 0,
            matcher_event: None,
        }
    }

    pub fn cancel(order_id: u64, user_id: u64) -> Self {
        Self {
            command: OrderCommandType::CancelOrder,
            order_id,
            symbol_id: 0,
            user_id,
            price: 0,
            reserve_bid_price: 0,
            size: 0,
            side: Side::Ask,            // Will be ignored
            order_type: OrderType::Gtc, // Will be ignored
            timestamp: 0,
            matcher_event: None,
        }
    }

    pub fn is_mutating(&self) -> bool {
        matches!(
            self.command,
            OrderCommandType::PlaceLimitOrder
                | OrderCommandType::PlaceMarketOrder
                | OrderCommandType::CancelOrder
        )
    }

    pub fn attach_matcher_event(&mut self, event: Box<MatcherTradeEvent>) {
        if let Some(mut tail) = self.matcher_event.as_mut() {
            while tail.next_event.is_some() {
                tail = tail.next_event.as_mut().unwrap();
            }
            tail.next_event = Some(event);
        } else {
            self.matcher_event = Some(event);
        }
    }
}

impl OrderTrait for OrderCommand {
    fn price(&self) -> u64 {
        self.price
    }
    fn size(&self) -> u64 {
        self.size
    }
    fn filled(&self) -> u64 {
        // filled is not a part of command, but calculated by matching engine
        // however, for FOK_BUDGET it is possible to calculate filled size based on events
        self.matcher_event
            .as_ref()
            .map(|e| e.calc_filled_size())
            .unwrap_or(0)
    }

    fn user_id(&self) -> u64 {
        self.user_id
    }

    fn side(&self) -> Side {
        self.side
    }
    fn order_id(&self) -> u64 {
        self.order_id
    }
    fn timestamp(&self) -> u64 {
        self.timestamp
    }
    fn reserve_bid_price(&self) -> u64 {
        self.reserve_bid_price
    }
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct MatcherTradeEvent {
    pub event_type: MatcherEventType,
    pub section: u32,
    pub symbol_id: u32,
    pub active_order_user_id: u64,
    pub taker_action: Side,
    pub active_order_completed: bool,
    pub matched_order_id: u64,
    pub maker_user_id: u64,
    pub matched_order_completed: bool,
    pub price: u64,
    pub size: u64,
    pub bidder_hold_price: u64,

    // Fee data
    pub taker_fee: u64,
    pub maker_fee: u64,

    pub next_event: Option<Box<MatcherTradeEvent>>,
}

impl MatcherTradeEvent {
    pub fn calc_filled_size(&self) -> u64 {
        let mut size = 0;
        let mut current = Some(self);
        while let Some(event) = current {
            if event.event_type == MatcherEventType::Trade {
                size += event.size;
            }
            current = event.next_event.as_deref();
        }
        size
    }
}

impl Default for MatcherTradeEvent {
    fn default() -> Self {
        Self {
            event_type: MatcherEventType::Trade,
            section: 0, // TODO: What is section?
            symbol_id: 0,
            active_order_user_id: 0,
            taker_action: Side::Ask,
            active_order_completed: false,
            matched_order_id: 0,
            maker_user_id: 0,
            matched_order_completed: false,
            price: 0,
            size: 0,
            bidder_hold_price: 0,
            taker_fee: 0,
            maker_fee: 0,
            next_event: None,
        }
    }
}

pub fn encode_order_command(order_command: OrderCommand, buf: &mut [u8]) -> SbeResult<()> {
    let write_buf = WriteBuf::new(buf);
    let mut encoder = OrderCommandMessageEncoder::default();
    encoder = encoder.wrap(write_buf, message_header_codec::ENCODED_LENGTH);
    encoder = encoder.header(0).parent()?;
    encoder.command(order_command.command.into());
    encoder.order_id(order_command.order_id);
    encoder.symbol_id(order_command.symbol_id);
    encoder.user_id(order_command.user_id);
    encoder.price(order_command.price);
    encoder.reserve_bid_price(order_command.reserve_bid_price);
    encoder.size(order_command.size);
    encoder.side(order_command.side.into());
    encoder.order_type(order_command.order_type.into());
    encoder.timestamp(order_command.timestamp);
    Ok(())
}

pub fn decode_order_command(buf: &[u8]) -> Result<OrderCommand, SerdeError> {
    let buf = ReadBuf::new(buf);
    let mut decoder = OrderCommandMessageDecoder::default();
    let header = MessageHeaderDecoder::default().wrap(buf, 0);
    decoder = decoder.header(header, 0);
    Ok(OrderCommand {
        command: decoder.command().try_into()?,
        order_id: decoder.order_id(),
        symbol_id: decoder.symbol_id(),
        user_id: decoder.user_id(),
        price: decoder.price(),
        reserve_bid_price: decoder.reserve_bid_price(),
        size: decoder.size(),
        side: decoder.side().try_into()?,
        order_type: decoder.order_type().try_into()?,
        timestamp: decoder.timestamp(),
        matcher_event: None,
    })
}

#[derive(Default, Clone)]
pub struct ProcessedOrderEvent {
    pub original_order_id: u64,
    pub symbol_id: u32,
    pub matcher_events: Vec<MatcherTradeEvent>,
}