//! Type-safe fluent API for building OrderCommands
//!
//! This module provides builder patterns for creating OrderCommands
//! with compile-time guarantees that all required fields are set.

use common::{OrderCommand, Side, TimeInForce};

/// Main entry point for building OrderCommands
pub struct OrderBuilder;

impl OrderBuilder {
    /// Create a deposit funds command builder
    pub fn deposit() -> DepositBuilder {
        DepositBuilder::new()
    }

    /// Create a GTC limit order builder
    pub fn place_limit() -> LimitOrderBuilder<NeedsUser> {
        LimitOrderBuilder::new(TimeInForce::Gtc)
    }

    /// Create a market order builder
    pub fn place_market() -> MarketOrderBuilder<NeedsUser> {
        MarketOrderBuilder::new()
    }

    /// Create an IOC limit order builder
    pub fn place_ioc() -> LimitOrderBuilder<NeedsUser> {
        LimitOrderBuilder::new(TimeInForce::Ioc)
    }

    /// Create a FOK limit order builder
    pub fn place_fok() -> LimitOrderBuilder<NeedsUser> {
        LimitOrderBuilder::new(TimeInForce::Fok)
    }

    /// Create a cancel order builder
    pub fn cancel(user_id: u64) -> CancelBuilder<NeedsOrderId> {
        CancelBuilder::new(user_id)
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
// Limit Order Builder (Type-State Pattern)
// ============================================================================

pub struct NeedsUser;
pub struct NeedsPrice;
pub struct NeedsSize;
pub struct NeedsSide;
pub struct NeedsMarket;
pub struct Complete;

pub struct LimitOrderBuilder<State = NeedsUser> {
    tif: TimeInForce,
    user_id: u64,
    price: u64,
    size: u64,
    side: Side,
    market_id: u32,
    _state: std::marker::PhantomData<State>,
}

impl LimitOrderBuilder<NeedsUser> {
    fn new(tif: TimeInForce) -> Self {
        Self {
            tif,
            user_id: 0,
            price: 0,
            size: 0,
            side: Side::Bid,
            market_id: 0,
            _state: std::marker::PhantomData,
        }
    }

    pub fn user(self, user_id: u64) -> LimitOrderBuilder<NeedsPrice> {
        LimitOrderBuilder {
            tif: self.tif,
            user_id,
            price: self.price,
            size: self.size,
            side: self.side,
            market_id: self.market_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl LimitOrderBuilder<NeedsPrice> {
    pub fn price(self, price: u64) -> LimitOrderBuilder<NeedsSize> {
        LimitOrderBuilder {
            tif: self.tif,
            user_id: self.user_id,
            price,
            size: self.size,
            side: self.side,
            market_id: self.market_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl LimitOrderBuilder<NeedsSize> {
    pub fn size(self, size: u64) -> LimitOrderBuilder<NeedsSide> {
        LimitOrderBuilder {
            tif: self.tif,
            user_id: self.user_id,
            price: self.price,
            size,
            side: self.side,
            market_id: self.market_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl LimitOrderBuilder<NeedsSide> {
    pub fn side(self, side: Side) -> LimitOrderBuilder<NeedsMarket> {
        LimitOrderBuilder {
            tif: self.tif,
            user_id: self.user_id,
            price: self.price,
            size: self.size,
            side,
            market_id: self.market_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl LimitOrderBuilder<NeedsMarket> {
    pub fn market_id(self, market_id: u32) -> LimitOrderBuilder<Complete> {
        LimitOrderBuilder {
            tif: self.tif,
            user_id: self.user_id,
            price: self.price,
            size: self.size,
            side: self.side,
            market_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl LimitOrderBuilder<Complete> {
    pub fn build(self) -> OrderCommand {
        OrderCommand::place_order(
            self.tif,
            self.user_id,
            self.price,
            self.size,
            self.side,
            self.market_id,
            0,
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
    _state: std::marker::PhantomData<State>,
}

impl MarketOrderBuilder<NeedsUser> {
    fn new() -> Self {
        Self {
            user_id: 0,
            size: 0,
            side: Side::Bid,
            market_id: 0,
            _state: std::marker::PhantomData,
        }
    }

    pub fn user(self, user_id: u64) -> MarketOrderBuilder<NeedsSize> {
        MarketOrderBuilder {
            user_id,
            size: self.size,
            side: self.side,
            market_id: self.market_id,
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
            _state: std::marker::PhantomData,
        }
    }
}

impl MarketOrderBuilder<Complete> {
    pub fn build(self) -> OrderCommand {
        // Market buy: price = u64::MAX
        // Market sell: price = 0
        let price = match self.side {
            Side::Bid => u64::MAX,
            Side::Ask => 0,
        };
        OrderCommand::place_order(
            TimeInForce::Ioc,
            self.user_id,
            price,
            self.size,
            self.side,
            self.market_id,
            0,
        )
    }
}

// ============================================================================
// Cancel Order Builder
// ============================================================================

pub struct NeedsOrderId;

pub struct CancelBuilder<State = NeedsOrderId> {
    order_id: u64,
    user_id: u64,
    side: Side,
    market_id: u32,
    _state: std::marker::PhantomData<State>,
}

impl CancelBuilder<NeedsOrderId> {
    fn new(user_id: u64) -> Self {
        Self {
            order_id: 0,
            user_id,
            side: Side::Bid,
            market_id: 0,
            _state: std::marker::PhantomData,
        }
    }

    pub fn order_id(self, order_id: u64) -> CancelBuilder<NeedsSide> {
        CancelBuilder {
            order_id,
            user_id: self.user_id,
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
            user_id: self.user_id,
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
            user_id: self.user_id,
            side: self.side,
            market_id,
            _state: std::marker::PhantomData,
        }
    }
}

impl CancelBuilder<Complete> {
    pub fn build(self) -> OrderCommand {
        OrderCommand::cancel_order(self.order_id, self.user_id, self.side, self.market_id)
    }
}
