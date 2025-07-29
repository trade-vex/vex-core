use crate::model::enums::{MatcherEventType, Side, OrderType};
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
    pub order_id: i64,
    pub symbol: i32,
    pub uid: i64,
    pub price: i64,
    pub reserve_bid_price: i64,
    pub size: i64,
    pub action: Side,
    pub order_type: OrderType,
    pub user_cookie: i32,
    pub timestamp: i64,
    pub matcher_event: Option<Box<MatcherTradeEvent>>,
}
impl Default for OrderCommand {
    fn default() -> Self {
        Self {
            command: OrderCommandType::PlaceLimitOrder,
            order_id: 0,
            symbol: 0,
            uid: 0,
            price: 0,
            reserve_bid_price: 0,
            size: 0,
            action: Side::Ask,   // Default action
            order_type: OrderType::Gtc, // Default order type
            user_cookie: 0,
            timestamp: 0,
            matcher_event: None,
        }
    }
}
impl OrderCommand {
    pub fn new_order(
        order_type: OrderType,
        order_id: i64,
        uid: i64,
        price: i64,
        reserve_bid_price: i64,
        size: i64,
        action: Side,
    ) -> Self {
        Self {
            command: OrderCommandType::PlaceLimitOrder,
            order_id,
            symbol: 0,
            uid,
            price,
            reserve_bid_price,
            size,
            action,
            order_type,
            user_cookie: 0,
            timestamp: 0,
            matcher_event: None,
        }
    }

    pub fn cancel(order_id: i64, uid: i64) -> Self {
        Self {
            command: OrderCommandType::CancelOrder,
            order_id,
            symbol: 0,
            uid,
            price: 0,
            reserve_bid_price: 0,
            size: 0,
            action: Side::Ask,   // Will be ignored
            order_type: OrderType::Gtc, // Will be ignored
            user_cookie: 0,
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
    fn price(&self) -> i64 {
        self.price
    }
    fn size(&self) -> i64 {
        self.size
    }
    fn filled(&self) -> i64 {
        // filled is not a part of command, but calculated by matching engine
        // however, for FOK_BUDGET it is possible to calculate filled size based on events
        self.matcher_event
            .as_ref()
            .map(|e| e.calc_filled_size())
            .unwrap_or(0)
    }
    fn uid(&self) -> i64 {
        self.uid
    }
    fn action(&self) -> Side {
        self.action
    }
    fn order_id(&self) -> i64 {
        self.order_id
    }
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
    fn reserve_bid_price(&self) -> i64 {
        self.reserve_bid_price
    }
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct MatcherTradeEvent {
    pub event_type: MatcherEventType,
    pub section: i32,
    pub symbol: i32,
    pub active_order_uid: i64,
    pub taker_action: Side,
    pub active_order_completed: bool,
    pub matched_order_id: i64,
    pub maker_uid: i64,
    pub matched_order_completed: bool,
    pub price: i64,
    pub size: i64,
    pub bidder_hold_price: i64,

    // Fee data
    pub taker_fee: i64,
    pub maker_fee: i64,

    pub next_event: Option<Box<MatcherTradeEvent>>,
}

impl MatcherTradeEvent {
    pub fn calc_filled_size(&self) -> i64 {
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
            symbol: 0,
            active_order_uid: 0,
            taker_action: Side::Ask,
            active_order_completed: false,
            matched_order_id: 0,
            maker_uid: 0,
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
    encoder.symbol(order_command.symbol);
    encoder.uid(order_command.uid);
    encoder.price(order_command.price);
    encoder.reserve_bid_price(order_command.reserve_bid_price);
    encoder.size(order_command.size);
    encoder.action(order_command.action.into());
    encoder.order_type(order_command.order_type.into());
    encoder.user_cookie(order_command.user_cookie);
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
        symbol: decoder.symbol(),
        uid: decoder.uid(),
        price: decoder.price(),
        reserve_bid_price: decoder.reserve_bid_price(),
        size: decoder.size(),
        action: decoder.action().try_into()?,
        order_type: decoder.order_type().try_into()?,
        user_cookie: decoder.user_cookie(),
        timestamp: decoder.timestamp(),
        matcher_event: None,
    })
}
