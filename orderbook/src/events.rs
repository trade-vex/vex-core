//! This module contains helpers for creating and managing matcher events.
use common::model::enums::MatcherEventType;
use common::model::order::IOrder;
use crate::{MatcherTradeEvent, OrderCommand};

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
    pub fn send_reduce_event<T: IOrder>(order: &T, reduced_size: i64, order_completed: bool) -> MatcherTradeEvent {
        MatcherTradeEvent {
            event_type: MatcherEventType::Reduce,
            active_order_completed: order_completed,
            matched_order_id: order.order_id(),
            matched_order_uid: order.uid(),
            matched_order_completed: order_completed,
            price: order.price(),
            size: reduced_size,
            ..MatcherTradeEvent::default()
        }
    }
}

impl Default for MatcherTradeEvent {
    fn default() -> Self {
        Self {
            event_type: MatcherEventType::Trade,
            section: 0, // TODO: What is section?
            active_order_completed: false,
            matched_order_id: 0,
            matched_order_uid: 0,
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