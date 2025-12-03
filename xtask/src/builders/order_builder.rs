//! Type-safe fluent API for building OrderCommands
//!
//! This module provides builder patterns for creating OrderCommands
//! with compile-time guarantees that all required fields are set.

use common::{OrderCommand, OrderCommandType, Side, TimeInForce};

/// Main entry point for building OrderCommands
pub struct OrderBuilder;

impl OrderBuilder {
    /// Create a deposit funds command builder
    pub fn deposit() -> DepositBuilder {
        DepositBuilder::new()
    }

    /// Create a withdrawal funds command builder
    pub fn withdraw() -> WithdrawBuilder {
        WithdrawBuilder::new()
    }

    /// Create a limit order builder
    pub fn place_limit() -> LimitOrderBuilder<NeedsUser> {
        LimitOrderBuilder::new()
    }

    /// Create a market order builder
    pub fn place_market() -> MarketOrderBuilder<NeedsUser> {
        MarketOrderBuilder::new()
    }

    /// Create an IOC order builder
    pub fn place_ioc() -> IocOrderBuilder<NeedsUser> {
        IocOrderBuilder::new()
    }

    /// Create a FOK order builder
    pub fn place_fok() -> FokOrderBuilder<NeedsUser> {
        FokOrderBuilder::new()
    }

    /// Create a cancel order builder
    pub fn cancel() -> CancelBuilder<NeedsOrderId> {
        CancelBuilder::new()
    }
}

// ============================================================================
// Deposit Builder
// ============================================================================

pub struct DepositBuilder {
    user_id: Option<u64>,
    amount: Option<u64>,
    asset_id: Option<u16>,
}

impl DepositBuilder {
    fn new() -> Self {
        Self {
            user_id: None,
            amount: None,
            asset_id: None,
        }
    }

    pub fn user(mut self, user_id: u64) -> Self {
        self.user_id = Some(user_id);
        self
    }

    pub fn amount(mut self, amount: u64) -> Self {
        self.amount = Some(amount);
        self
    }

    pub fn asset(mut self, asset_id: u16) -> Self {
        self.asset_id = Some(asset_id);
        self
    }

    pub fn build(self) -> OrderCommand {
        OrderCommand::deposit_funds(
            self.user_id.expect("user_id is required"),
            self.amount.expect("amount is required"),
            self.asset_id.expect("asset_id is required"),
        )
    }
}

// ============================================================================
// Withdraw Builder
// ============================================================================

pub struct WithdrawBuilder {
    user_id: Option<u64>,
    amount: Option<u64>,
    asset_id: Option<u16>,
}

impl WithdrawBuilder {
    fn new() -> Self {
        Self {
            user_id: None,
            amount: None,
            asset_id: None,
        }
    }

    pub fn user(mut self, user_id: u64) -> Self {
        self.user_id = Some(user_id);
        self
    }

    pub fn amount(mut self, amount: u64) -> Self {
        self.amount = Some(amount);
        self
    }

    pub fn asset(mut self, asset_id: u16) -> Self {
        self.asset_id = Some(asset_id);
        self
    }

    pub fn build(self) -> OrderCommand {
        let user_id = self.user_id.expect("user_id is required");
        let amount = self.amount.expect("amount is required");
        let asset_id = self.asset_id.expect("asset_id is required");

        OrderCommand {
            command: OrderCommandType::WithdrawFunds,
            order_id: 0,
            market_id: asset_id as u32,
            user_id,
            price: 0,
            size: amount,
            side: Side::Ask,
            time_in_force: TimeInForce::Gtc,
            timestamp: 0,
            status: common::Status::Processing,
            client_order_id: 0,
            events: None,
            balance: [common::UserBalance::default(); 2],
            l2_data: None,
            route_gateway_id: 0,
        }
    }
}

// ============================================================================
// Limit Order Builder (Type-State Pattern)
// ============================================================================

pub struct NeedsUser;
pub struct NeedsPrice;
pub struct NeedsSize;
pub struct NeedsSide;
pub struct NeedsMarket;
pub struct Complete;

pub struct LimitOrderBuilder<State = NeedsUser> {
    user_id: u64,
    price: u64,
    size: u64,
    side: Side,
    market_id: u32,
    client_order_id: u64,
    _state: std::marker::PhantomData<State>,
}

impl LimitOrderBuilder<NeedsUser> {
    fn new() -> Self {
        Self {
            user_id: 0,
            price: 0,
            size: 0,
            side: Side::Bid,
            market_id: 0,
            client_order_id: 0,
            _state: std::marker::PhantomData,
        }
    }

    pub fn user(self, user_id: u64) -> LimitOrderBuilder<NeedsPrice> {
        LimitOrderBuilder {
            user_id,
            price: self.price,
            size: self.size,
            side: self.side,
            market_id: self.market_id,
            client_order_id: self.client_order_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl LimitOrderBuilder<NeedsPrice> {
    pub fn price(self, price: u64) -> LimitOrderBuilder<NeedsSize> {
        LimitOrderBuilder {
            user_id: self.user_id,
            price,
            size: self.size,
            side: self.side,
            market_id: self.market_id,
            client_order_id: self.client_order_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl LimitOrderBuilder<NeedsSize> {
    pub fn size(self, size: u64) -> LimitOrderBuilder<NeedsSide> {
        LimitOrderBuilder {
            user_id: self.user_id,
            price: self.price,
            size,
            side: self.side,
            market_id: self.market_id,
            client_order_id: self.client_order_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl LimitOrderBuilder<NeedsSide> {
    pub fn side(self, side: Side) -> LimitOrderBuilder<NeedsMarket> {
        LimitOrderBuilder {
            user_id: self.user_id,
            price: self.price,
            size: self.size,
            side,
            market_id: self.market_id,
            client_order_id: self.client_order_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl LimitOrderBuilder<NeedsMarket> {
    pub fn market_id(self, market_id: u32) -> LimitOrderBuilder<Complete> {
        LimitOrderBuilder {
            user_id: self.user_id,
            price: self.price,
            size: self.size,
            side: self.side,
            market_id,
            client_order_id: self.client_order_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl LimitOrderBuilder<Complete> {
    pub fn client_order_id(mut self, client_order_id: u64) -> Self {
        self.client_order_id = client_order_id;
        self
    }

    pub fn build(self) -> OrderCommand {
        OrderCommand::place_order(
            TimeInForce::Gtc,
            self.user_id,
            self.price,
            self.size,
            self.side,
            self.market_id,
            self.client_order_id,
        )
    }
}

// ============================================================================
// Market Order Builder
// ============================================================================

pub struct MarketOrderBuilder<State = NeedsUser> {
    user_id: u64,
    size: u64,
    side: Side,
    market_id: u32,
    client_order_id: u64,
    _state: std::marker::PhantomData<State>,
}

impl MarketOrderBuilder<NeedsUser> {
    fn new() -> Self {
        Self {
            user_id: 0,
            size: 0,
            side: Side::Bid,
            market_id: 0,
            client_order_id: 0,
            _state: std::marker::PhantomData,
        }
    }

    pub fn user(self, user_id: u64) -> MarketOrderBuilder<NeedsSize> {
        MarketOrderBuilder {
            user_id,
            size: self.size,
            side: self.side,
            market_id: self.market_id,
            client_order_id: self.client_order_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl MarketOrderBuilder<NeedsSize> {
    pub fn size(self, size: u64) -> MarketOrderBuilder<NeedsSide> {
        MarketOrderBuilder {
            user_id: self.user_id,
            size,
            side: self.side,
            market_id: self.market_id,
            client_order_id: self.client_order_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl MarketOrderBuilder<NeedsSide> {
    pub fn side(self, side: Side) -> MarketOrderBuilder<NeedsMarket> {
        MarketOrderBuilder {
            user_id: self.user_id,
            size: self.size,
            side,
            market_id: self.market_id,
            client_order_id: self.client_order_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl MarketOrderBuilder<NeedsMarket> {
    pub fn market_id(self, market_id: u32) -> MarketOrderBuilder<Complete> {
        MarketOrderBuilder {
            user_id: self.user_id,
            size: self.size,
            side: self.side,
            market_id,
            client_order_id: self.client_order_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl MarketOrderBuilder<Complete> {
    pub fn client_order_id(mut self, client_order_id: u64) -> Self {
        self.client_order_id = client_order_id;
        self
    }

    pub fn build(self) -> OrderCommand {
        // Market buy: price = u64::MAX
        // Market sell: price = 0
        let price = match self.side {
            Side::Bid => u64::MAX,
            Side::Ask => 0,
        };

        OrderCommand {
            command: OrderCommandType::PlaceOrder,
            order_id: 0,
            client_order_id: self.client_order_id,
            market_id: self.market_id,
            user_id: self.user_id,
            price,
            size: self.size,
            side: self.side,
            time_in_force: TimeInForce::Ioc,
            timestamp: 0,
            status: common::Status::Processing,
            events: None,
            balance: [common::UserBalance::default(); 2],
            l2_data: None,
            route_gateway_id: 0,
        }
    }
}

// ============================================================================
// IOC Order Builder
// ============================================================================

pub struct IocOrderBuilder<State = NeedsUser> {
    user_id: u64,
    price: u64,
    size: u64,
    side: Side,
    market_id: u32,
    client_order_id: u64,
    _state: std::marker::PhantomData<State>,
}

impl IocOrderBuilder<NeedsUser> {
    fn new() -> Self {
        Self {
            user_id: 0,
            price: 0,
            size: 0,
            side: Side::Bid,
            market_id: 0,
            client_order_id: 0,
            _state: std::marker::PhantomData,
        }
    }

    pub fn user(self, user_id: u64) -> IocOrderBuilder<NeedsPrice> {
        IocOrderBuilder {
            user_id,
            price: self.price,
            size: self.size,
            side: self.side,
            market_id: self.market_id,
            client_order_id: self.client_order_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl IocOrderBuilder<NeedsPrice> {
    pub fn price(self, price: u64) -> IocOrderBuilder<NeedsSize> {
        IocOrderBuilder {
            user_id: self.user_id,
            price,
            size: self.size,
            side: self.side,
            market_id: self.market_id,
            client_order_id: self.client_order_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl IocOrderBuilder<NeedsSize> {
    pub fn size(self, size: u64) -> IocOrderBuilder<NeedsSide> {
        IocOrderBuilder {
            user_id: self.user_id,
            price: self.price,
            size,
            side: self.side,
            market_id: self.market_id,
            client_order_id: self.client_order_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl IocOrderBuilder<NeedsSide> {
    pub fn side(self, side: Side) -> IocOrderBuilder<NeedsMarket> {
        IocOrderBuilder {
            user_id: self.user_id,
            price: self.price,
            size: self.size,
            side,
            market_id: self.market_id,
            client_order_id: self.client_order_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl IocOrderBuilder<NeedsMarket> {
    pub fn market_id(self, market_id: u32) -> IocOrderBuilder<Complete> {
        IocOrderBuilder {
            user_id: self.user_id,
            price: self.price,
            size: self.size,
            side: self.side,
            market_id,
            client_order_id: self.client_order_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl IocOrderBuilder<Complete> {
    pub fn client_order_id(mut self, client_order_id: u64) -> Self {
        self.client_order_id = client_order_id;
        self
    }

    pub fn build(self) -> OrderCommand {
        OrderCommand {
            command: OrderCommandType::PlaceOrder,
            order_id: 0,
            client_order_id: self.client_order_id,
            market_id: self.market_id,
            user_id: self.user_id,
            price: self.price,
            size: self.size,
            side: self.side,
            time_in_force: TimeInForce::Ioc,
            timestamp: 0,
            status: common::Status::Processing,
            events: None,
            balance: [common::UserBalance::default(); 2],
            l2_data: None,
            route_gateway_id: 0,
        }
    }
}

// ============================================================================
// FOK Order Builder
// ============================================================================

pub struct FokOrderBuilder<State = NeedsUser> {
    user_id: u64,
    price: u64,
    size: u64,
    side: Side,
    market_id: u32,
    client_order_id: u64,
    _state: std::marker::PhantomData<State>,
}

impl FokOrderBuilder<NeedsUser> {
    fn new() -> Self {
        Self {
            user_id: 0,
            price: 0,
            size: 0,
            side: Side::Bid,
            market_id: 0,
            client_order_id: 0,
            _state: std::marker::PhantomData,
        }
    }

    pub fn user(self, user_id: u64) -> FokOrderBuilder<NeedsPrice> {
        FokOrderBuilder {
            user_id,
            price: self.price,
            size: self.size,
            side: self.side,
            market_id: self.market_id,
            client_order_id: self.client_order_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl FokOrderBuilder<NeedsPrice> {
    pub fn price(self, price: u64) -> FokOrderBuilder<NeedsSize> {
        FokOrderBuilder {
            user_id: self.user_id,
            price,
            size: self.size,
            side: self.side,
            market_id: self.market_id,
            client_order_id: self.client_order_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl FokOrderBuilder<NeedsSize> {
    pub fn size(self, size: u64) -> FokOrderBuilder<NeedsSide> {
        FokOrderBuilder {
            user_id: self.user_id,
            price: self.price,
            size,
            side: self.side,
            market_id: self.market_id,
            client_order_id: self.client_order_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl FokOrderBuilder<NeedsSide> {
    pub fn side(self, side: Side) -> FokOrderBuilder<NeedsMarket> {
        FokOrderBuilder {
            user_id: self.user_id,
            price: self.price,
            size: self.size,
            side,
            market_id: self.market_id,
            client_order_id: self.client_order_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl FokOrderBuilder<NeedsMarket> {
    pub fn market_id(self, market_id: u32) -> FokOrderBuilder<Complete> {
        FokOrderBuilder {
            user_id: self.user_id,
            price: self.price,
            size: self.size,
            side: self.side,
            market_id,
            client_order_id: self.client_order_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl FokOrderBuilder<Complete> {
    pub fn client_order_id(mut self, client_order_id: u64) -> Self {
        self.client_order_id = client_order_id;
        self
    }

    pub fn build(self) -> OrderCommand {
        OrderCommand {
            command: OrderCommandType::PlaceOrder,
            order_id: 0,
            client_order_id: self.client_order_id,
            market_id: self.market_id,
            user_id: self.user_id,
            price: self.price,
            size: self.size,
            side: self.side,
            time_in_force: TimeInForce::Fok,
            timestamp: 0,
            status: common::Status::Processing,
            events: None,
            balance: [common::UserBalance::default(); 2],
            l2_data: None,
            route_gateway_id: 0,
        }
    }
}

// ============================================================================
// Cancel Order Builder
// ============================================================================

pub struct NeedsOrderId;

pub struct CancelBuilder<State = NeedsOrderId> {
    order_id: u64,
    side: Side,
    market_id: u32,
    _state: std::marker::PhantomData<State>,
}

impl CancelBuilder<NeedsOrderId> {
    fn new() -> Self {
        Self {
            order_id: 0,
            side: Side::Bid,
            market_id: 0,
            _state: std::marker::PhantomData,
        }
    }

    pub fn order_id(self, order_id: u64) -> CancelBuilder<NeedsSide> {
        CancelBuilder {
            order_id,
            side: self.side,
            market_id: self.market_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl CancelBuilder<NeedsSide> {
    pub fn side(self, side: Side) -> CancelBuilder<NeedsMarket> {
        CancelBuilder {
            order_id: self.order_id,
            side,
            market_id: self.market_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl CancelBuilder<NeedsMarket> {
    pub fn market_id(self, market_id: u32) -> CancelBuilder<Complete> {
        CancelBuilder {
            order_id: self.order_id,
            side: self.side,
            market_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl CancelBuilder<Complete> {
    pub fn build(self) -> OrderCommand {
        OrderCommand::cancel_order(self.order_id, self.side, self.market_id)
    }
}
