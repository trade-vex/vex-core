//! This module contains helpers for creating and managing matcher events.
use crate::OrderCommand;
use common::cmd::MatcherTradeEvent;
use common::model::enums::{MatcherEventType, Side};
use common::model::symbol_specification::CoreSymbolSpecification;

pub struct EventHelper;

impl EventHelper {
    /// Creates and attaches a REJECT event to the command.
    /// This is used when an order (or part of it) cannot be filled.
    pub fn attach_reject_event(cmd: &mut OrderCommand, rejected_size: u64) {
        let reject_event = MatcherTradeEvent {
            event_type: MatcherEventType::Reject,
            symbol_id: cmd.symbol_id,
            active_order_user_id: cmd.user_id,
            taker_action: cmd.side,
            active_order_completed: true, // A reject always finalizes the active order
            matched_order_id: 0,          // No matched order for a reject
            maker_user_id: 0,
            matched_order_completed: false,
            price: cmd.price,
            size: rejected_size,
            ..MatcherTradeEvent::default()
        };
        cmd.attach_matcher_event(Box::new(reject_event));
    }

    /// Creates and attaches a REDUCE event to the command.
    /// This is used when an order is cancelled or reduced in size.
    pub fn send_reduce_event(
        cmd: &OrderCommand,
        reduced_size: u64,
        is_cancel: bool,
    ) -> Box<MatcherTradeEvent> {
        Box::new(MatcherTradeEvent {
            event_type: if is_cancel {
                MatcherEventType::Cancel
            } else {
                MatcherEventType::Reduce
            },
            symbol_id: cmd.symbol_id,
            active_order_user_id: cmd.user_id,
            taker_action: cmd.side,
            active_order_completed: is_cancel,
            matched_order_id: cmd.order_id,
            maker_user_id: cmd.user_id,
            price: cmd.price,
            size: reduced_size,
            ..MatcherTradeEvent::default()
        })
    }

    pub fn create_trade_event(
        active_order_cmd: &OrderCommand,
        matched_order_id: u64,
        maker_user_id: u64,
        maker_filled: bool,
        price: u64,
        size: u64,
        spec: &CoreSymbolSpecification,
    ) -> Box<MatcherTradeEvent> {
        Box::new(MatcherTradeEvent {
            event_type: MatcherEventType::Trade,
            symbol_id: active_order_cmd.symbol_id,
            active_order_user_id: active_order_cmd.user_id,
            taker_action: active_order_cmd.side,
            section: 0,                                            // TODO
            active_order_completed: active_order_cmd.size == size, // Simplified
            matched_order_id,
            maker_user_id,
            matched_order_completed: maker_filled,
            price,
            size,
            bidder_hold_price: if active_order_cmd.side == Side::Ask {
                active_order_cmd.reserve_bid_price
            } else {
                0 // In naive impl, maker order is not available to get reserve price
            },
            taker_fee: size * spec.taker_fee,
            maker_fee: size * spec.maker_fee,
            next_event: None,
        })
    }

    /// Creates an ORDER_PLACED event for when an order is successfully placed on the book.
    /// This is used when an order (or remaining part) is added to the order book.
    pub fn create_order_placed_event(
        cmd: &OrderCommand,
        placed_size: u64,
    ) -> Box<MatcherTradeEvent> {
        Box::new(MatcherTradeEvent {
            event_type: MatcherEventType::OrderPlaced, // Using OrderPlaced type for order placement
            symbol_id: cmd.symbol_id,
            active_order_user_id: cmd.user_id,
            taker_action: cmd.side,
            section: 0,
            active_order_completed: false, // Order is placed, not completed
            matched_order_id: cmd.order_id,
            maker_user_id: cmd.user_id,
            matched_order_completed: false,
            price: cmd.price,
            size: placed_size,
            bidder_hold_price: cmd.reserve_bid_price,
            taker_fee: 0, // No fees for placement
            maker_fee: 0,
            next_event: None,
        })
    }
}
