use crate::cmd::{OrderCommand, OrderCommandType};
use crate::model::enums::{OrderAction, OrderType};

pub trait ApiCommand {
    fn into_order_command(self) -> OrderCommand;
}

pub struct ApiPlaceOrder {
    pub price: i64,
    pub size: i64,
    pub order_id: i64,
    pub action: OrderAction,
    pub order_type: OrderType,
    pub uid: i64,
    pub symbol: i32,
    pub user_cookie: i32,
    pub reserve_price: i64,
}

impl ApiCommand for ApiPlaceOrder {
    fn into_order_command(self) -> OrderCommand {
        OrderCommand {
            command: OrderCommandType::PlaceOrder,
            order_id: self.order_id,
            symbol: self.symbol,
            uid: self.uid,
            price: self.price,
            reserve_bid_price: self.reserve_price,
            size: self.size,
            action: self.action,
            order_type: self.order_type,
            user_cookie: self.user_cookie,
            timestamp: 0, // will be set by the exchange
            matcher_event: None,
        }
    }
}

pub struct ApiCancelOrder {
    pub order_id: i64,
    pub uid: i64,
    pub symbol: i32,
}

impl ApiCommand for ApiCancelOrder {
    fn into_order_command(self) -> OrderCommand {
        OrderCommand::cancel(self.order_id, self.uid)
    }
}

pub struct ApiMoveOrder {
    pub order_id: i64,
    pub new_price: i64,
    pub uid: i64,
    pub symbol: i32,
}

impl ApiCommand for ApiMoveOrder {
    fn into_order_command(self) -> OrderCommand {
        OrderCommand::move_order(self.order_id, self.uid, self.new_price)
    }
}

pub struct ApiReduceOrder {
    pub order_id: i64,
    pub uid: i64,
    pub symbol: i32,
    pub reduce_size: i64,
}

impl ApiCommand for ApiReduceOrder {
    fn into_order_command(self) -> OrderCommand {
        OrderCommand::reduce(self.order_id, self.uid, self.reduce_size)
    }
} 