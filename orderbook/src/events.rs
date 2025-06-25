//! This module contains helpers for creating and managing matcher events.
use crate::OrderCommand;
use common::cmd::MatcherTradeEvent;
use common::model::enums::{MatcherEventType, OrderAction};
use common::model::order::OrderTrait;
use common::model::symbol_specification::CoreSymbolSpecification;

pub struct EventHelper;

impl EventHelper {
    /// Creates and attaches a REJECT event to the command.
    /// This is used when an order (or part of it) cannot be filled.
    pub fn attach_reject_event(cmd: &mut OrderCommand, rejected_size: i64) {
        let reject_event = MatcherTradeEvent {
            event_type: MatcherEventType::Reject,
            active_order_completed: true, // A reject always finalizes the active order
            matched_order_id: 0,          // No matched order for a reject
            matched_order_uid: 0,
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
        order: &dyn OrderTrait,
        reduced_size: i64,
        is_cancel: bool,
    ) -> Box<MatcherTradeEvent> {
        Box::new(MatcherTradeEvent {
            event_type: if is_cancel {
                MatcherEventType::Cancel
            } else {
                MatcherEventType::Reduce
            },
            active_order_completed: is_cancel,
            matched_order_id: order.order_id(),
            matched_order_uid: order.uid(),
            price: order.price(),
            size: reduced_size,
            ..MatcherTradeEvent::default()
        })
    }

    pub fn create_trade_event(
        active_order_cmd: &OrderCommand,
        matched_order_id: i64,
        matched_order_uid: i64,
        maker_filled: bool,
        price: i64,
        size: i64,
        spec: &CoreSymbolSpecification,
    ) -> Box<MatcherTradeEvent> {
        Box::new(MatcherTradeEvent {
            event_type: MatcherEventType::Trade,
            section: 0,                                            // TODO
            active_order_completed: active_order_cmd.size == size, // Simplified
            matched_order_id,
            matched_order_uid,
            matched_order_completed: maker_filled,
            price,
            size,
            bidder_hold_price: if active_order_cmd.action == OrderAction::Ask {
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
        placed_size: i64
    ) -> Box<MatcherTradeEvent> {
        Box::new(MatcherTradeEvent {
            event_type: MatcherEventType::Reduce, // Using Reduce type for order placement
            section: 0,
            active_order_completed: false, // Order is placed, not completed
            matched_order_id: cmd.order_id,
            matched_order_uid: cmd.uid,
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
