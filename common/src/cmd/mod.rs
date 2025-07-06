use crate::model::enums::{MatcherEventType, OrderAction, OrderType};
use crate::model::order::OrderTrait;
use borsh::{BorshDeserialize, BorshSerialize};

// TODO: translate OrderCommand
#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum OrderCommandType {
    PlaceOrder,
    CancelOrder,
    MoveOrder,
    ReduceOrder,
    // TODO
    //    ORDER_BOOK_REQUEST,
    //    ADD_USER,
    //    BALANCE_ADJUSTMENT,
    //    SUSPEND_USER,
    //    RESUME_USER,
    //
    //    BINARY_DATA_QUERY,
    //    BINARY_DATA_COMMAND,
    //
    //    PERSIST_STATE_MATCHING,
    //    PERSIST_STATE_RISK,
    //
    //    GROUPING_CONTROL,
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
    pub action: OrderAction,
    pub order_type: OrderType,
    pub user_cookie: i32,
    pub timestamp: i64,
    pub matcher_event: Option<Box<MatcherTradeEvent>>,
}

impl OrderCommand {
    pub fn new_order(
        order_type: OrderType,
        order_id: i64,
        uid: i64,
        price: i64,
        reserve_bid_price: i64,
        size: i64,
        action: OrderAction,
    ) -> Self {
        Self {
            command: OrderCommandType::PlaceOrder,
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
            action: OrderAction::Ask,   // Will be ignored
            order_type: OrderType::Gtc, // Will be ignored
            user_cookie: 0,
            timestamp: 0,
            matcher_event: None,
        }
    }

    pub fn reduce(order_id: i64, uid: i64, size: i64) -> Self {
        Self {
            command: OrderCommandType::ReduceOrder,
            order_id,
            symbol: 0,
            uid,
            price: 0,
            reserve_bid_price: 0,
            size,
            action: OrderAction::Ask,   // Will be ignored
            order_type: OrderType::Gtc, // Will be ignored
            user_cookie: 0,
            timestamp: 0,
            matcher_event: None,
        }
    }

    pub fn move_order(order_id: i64, uid: i64, price: i64) -> Self {
        Self {
            command: OrderCommandType::MoveOrder,
            order_id,
            symbol: 0,
            uid,
            price,
            reserve_bid_price: 0,
            size: 0,
            action: OrderAction::Ask,   // Will be ignored
            order_type: OrderType::Gtc, // Will be ignored
            user_cookie: 0,
            timestamp: 0,
            matcher_event: None,
        }
    }

    pub fn is_mutating(&self) -> bool {
        matches!(
            self.command,
            OrderCommandType::PlaceOrder
                | OrderCommandType::CancelOrder
                | OrderCommandType::MoveOrder
                | OrderCommandType::ReduceOrder
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
    fn action(&self) -> OrderAction {
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
    pub active_order_completed: bool,
    pub matched_order_id: i64,
    pub matched_order_uid: i64,
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
