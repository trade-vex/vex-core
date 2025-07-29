use borsh::{BorshDeserialize, BorshSerialize};
use common::model::enums::Side;
use common::model::l2_market_data::L2MarketData;
use common::model::order::{Order, OrderTrait};
use common::model::symbol_specification::CoreSymbolSpecification;
use std::fmt;

pub mod tree;
mod unit_tests;

#[derive(Debug, Clone)]
pub struct PriceLevel {
    total_volume: u64,
    orders: VecDeque<Order>,
}

impl PriceLevel {
    fn new() -> Self {
        Self {
            total_volume: 0,
            orders: VecDeque::new(),
        }
    }

    #[inline]
    fn add_order(&mut self, order: Order) {
        self.total_volume += order.size;
        self.orders.push_back(order);
    }

    #[inline]
    fn remove_order(&mut self, order_id: u64, processed: &mut ProcessedOrderCommand) {
        if let Ok(pos) = self
            .orders
            .binary_search_by_key(&order_id, |order| order.order_id)
            && let Some(removed_order) = self.orders.remove(pos)
        {
            self.total_volume -= removed_order.size;
            processed.set_status(Status::Cancelled);
        }
    }
}

/// The concrete implementation of the `OrderBook`.
/// It is generic over the L2 market data depth.
pub struct OrderBook<Ask: BookSide, Bid: BookSide> {
    /// Bids are stored in a BTreeMap with a `Reverse` key to sort from high to low price.
    bids: Bid,
    /// Asks are stored in a BTreeMap sorted from low to high price.
    asks: Ask,
    /// Orders for fast lookups in case of cancellations
    orders: HashMap<u64, u64>,
}

impl<Ask: BookSide, Bid: BookSide> OrderBook<Ask, Bid> {
    /// Creates a new empty order book.
    pub fn new(bids: Bid, asks: Ask) -> Self {
        Self {
            bids,
            asks,
            orders: HashMap::new(),
        }
    }

    /// Matches Order
    /// The core matching logic for an incoming taker order.
    fn match_order(&mut self, cmd: &OrderCommand, events: &mut ProcessedOrderCommand) -> u64 {
        let mut remaining_size = cmd.size;
        type BookToMatch<'a> = (&'a mut dyn BookSide, Box<dyn Fn(u64, u64) -> bool>);
        let (book_to_match, price_check): BookToMatch = if cmd.side == Side::Bid {
            // Buy orders match against asks (lowest price first)
            (
                &mut self.asks,
                Box::new(|taker_price, maker_price| taker_price >= maker_price),
            )
        } else {
            // Sell orders match against bids (highest price first)
            (
                &mut self.bids,
                Box::new(|taker_price, maker_price| taker_price <= maker_price),
            )
        };

        let mut filled_price_levels = Vec::new();

        // The iterator from the BookSide trait handles the correct price priority.
        for (price, level) in book_to_match.iter_mut_for_matching() {
            if !price_check(cmd.price, price) || remaining_size == 0 {
                break;
            }

            let mut orders_to_remove = Vec::new();
            // Iterate through orders at this price level (FIFO).
            for (idx, maker_order) in level.orders.iter_mut().enumerate() {
                if remaining_size == 0 {
                    break;
                }

                let trade_size = remaining_size.min(maker_order.size);

                // Update sizes
                remaining_size -= trade_size;
                maker_order.size -= trade_size;
                level.total_volume -= trade_size;

                let maker_order_completed = maker_order.size == 0;

                // Create and attach the trade event
                let event = MatcherTradeEvent {
                    active_order_completed: remaining_size == 0,
                    matched_order_id: maker_order.order_id,
                    maker_user_id: maker_order.user_id,
                    matched_order_completed: maker_order_completed,
                    price,
                    size: trade_size,
                    next_event: None,
                };
                events.attatch_event(Box::new(event));

                if maker_order_completed {
                    orders_to_remove.push(idx);
                    self.orders.remove(&maker_order.order_id);
                }
            }

            // Remove filled orders from the queue.
            for idx in orders_to_remove.iter().rev() {
                level.orders.remove(*idx).unwrap();
            }

            if level.orders.is_empty() {
                filled_price_levels.push(price);
            }
        }

        // Clean up empty price levels from the book.
        for price in filled_price_levels {
            book_to_match.remove_level_if_empty(price);
        }

        remaining_size
    }

    /// Place Order
    /// OrderCommand for PlaceOrder
    /// OrderCommand {
    ///     command: OrderCommandType::PlaceOrder,
    ///     order_id: 0, /// ID's must be increasing order, which should be guaranteed by the snowflake algorithm
    ///     timestamp: 0, /// UNIX Timestamp recorded when order hits VEX-CORE
    ///     user_id: 0, /// Gateway set user_id
    ///     market_id: 0, /// Market ID, No explicit check is made for market id, must be guaranteed by the ORDERBOOK Router
    ///     price: 0, /// for limit order price is as is by user, for MARKET ORDER: buy: u64::MAX, sell: 0
    ///     size: 0, /// as set by user
    ///     side: Side::Bid, /// as set by user
    ///     time_in_force: TimeInForce::Gtc, /// for limit order GTC, for marker IOC/FOK
    /// }
    ///
    /// Constraints
    /// 1. Order ID's must be unique and increasing, should be guaranteed by snowflake implementation in gateway
    /// 2. Timestamp must be in monotonically increasing, guaranteed by Instant::now() when order hits vex-core
    /// 3. Price must be guaranteed u64::MAX for market buy orders, 0 for market sell orders, and TIF must be either IOC or FOK
    /// 4. Size must be > 0, guaranteed by gateway
    /// 5. The command must be PlaceOrder
    ///    All the contraints are NOT checked in the ORDERBOOK, must be guaranteed by upstream systems
    ///    They are not included here to avoid redundant checks that are already made
    pub fn place_order(&mut self, cmd: &OrderCommand) -> ProcessedOrderCommand {
        let mut processed =
            ProcessedOrderCommand::new(Status::Rejected, cmd.order_id, cmd.market_id, cmd.side);
        match cmd.time_in_force {
            TimeInForce::Gtc => {
                // Handle GTC (Good 'Til Canceled) orders
                let remaining = self.match_order(cmd, &mut processed);

                if remaining == cmd.size {
                    self.add_to_book(cmd, remaining);
                    processed.set_status(Status::Placed);
                } else if remaining > 0 {
                    // Add remaining to book
                    self.add_to_book(cmd, remaining);
                    processed.set_status(Status::PartiallyFilled);
                } else {
                    processed.set_status(Status::Filled);
                }
            }
            TimeInForce::Fok => {
                if !self.can_fill_completely(cmd) {
                    processed.set_status(Status::Cancelled);
                } else {
                    self.match_order(cmd, &mut processed);
                    processed.set_status(Status::Filled);
                }
            }
            TimeInForce::Ioc => {
                // Handle IOC (Immediate or Cancel) orders
                let remaining = self.match_order(cmd, &mut processed);

                if remaining == 0 {
                    processed.set_status(Status::Filled);
                } else if remaining < cmd.size {
                    processed.set_status(Status::PartiallyFilled);
                } else {
                    processed.set_status(Status::Cancelled);
                }
            }
        }
        processed
    }

    /// Cancel Order
    /// OrderCommand for CancelOrder
    /// OrderCommand {
    ///     command: OrderCommandType::CancelOrder,
    ///     order_id: order_id,
    ///     market_id: market_id,
    ///     user_id: 0,
    ///     price: 0,
    ///     size: 0,
    ///     side: side,
    ///     time_in_force: TimeInForce::Gtc,
    ///     timestamp: 0,
    /// }
    /// Constraints
    /// 1. Command must be CancelOrder
    /// 2. order_id must exist in the book
    /// 3. market_id must be properly set by the gateway
    /// 4. timestamp is recorded when the order hits VEX-core for the first time
    /// 5. Rest of the fields are redundant
    ///
    /// Note: This function does not check for the validity of the cancel order command.
    /// All the contraints are NOT checked in the ORDERBOOK, must be guaranteed by upstream systems
    pub fn cancel_order(&mut self, cmd: &OrderCommand) -> ProcessedOrderCommand {
        let mut processed =
            ProcessedOrderCommand::new(Status::Rejected, cmd.order_id, cmd.market_id, cmd.side);
        if let Some(price) = self.orders.remove(&cmd.order_id) {
            if let Some(best_price) = self.bids.best_price()
                && price <= best_price
            {
                if let Some(level) = self.bids.get_level_mut(price) {
                    level.remove_order(cmd.order_id, &mut processed);
                    self.bids.remove_level_if_empty(cmd.price);
                }
            } else if let Some(level) = self.asks.get_level_mut(price) {
                level.remove_order(cmd.order_id, &mut processed);
                self.asks.remove_level_if_empty(cmd.price);
            }
        }
        processed
    }

    /// Add order to the book
    fn add_to_book(&mut self, cmd: &OrderCommand, remaining_size: u64) {
        let order = Order {
            order_id: cmd.order_id,
            user_id: cmd.user_id,
            price: cmd.price,
            size: remaining_size,
            side: cmd.side,
            timestamp: cmd.timestamp,
        };
        let level = match cmd.side {
            Side::Bid => self.bids.get_or_create_level(cmd.price),
            Side::Ask => self.asks.get_or_create_level(cmd.price),
        };
        level.add_order(order);
        self.orders.insert(cmd.order_id, cmd.price);
    }

    /// Check if an order can be filled completely
    #[inline]
    fn can_fill_completely(&self, cmd: &OrderCommand) -> bool {
        let mut remaining = cmd.size;

        match cmd.side {
            Side::Bid => {
                // Check against asks
                for (price, level) in self.asks.iter() {
                    if Self::is_market_order(cmd) || price <= cmd.price {
                        if level.total_volume >= remaining {
                            return true;
                        }
                        remaining -= level.total_volume;
                    } else {
                        break;
                    }
                }
            }
            Side::Ask => {
                // Check against bids
                for (price, level) in self.bids.iter() {
                    if Self::is_market_order(cmd) || price >= cmd.price {
                        if level.total_volume >= remaining {
                            return true;
                        }
                        remaining -= level.total_volume;
                    } else {
                        break;
                    }
                }
            }
        }

        false
    }

    #[inline]
    fn is_market_order(cmd: &OrderCommand) -> bool {
        (cmd.price == 0 && cmd.side == Side::Ask)
            || (cmd.price == u64::MAX && cmd.side == Side::Bid)
    }
}

impl std::error::Error for OrderBookError {}

pub trait OrderBook<'a> {
    fn new_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError>;
    fn cancel_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError>;
    fn reduce_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError>;
    fn move_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError>;
    fn get_orders_num(&self, action: Side) -> i32;
    fn get_total_orders_volume(&self, action: Side) -> i64;
    fn get_order_by_id(&self, order_id: i64) -> Option<&dyn OrderTrait>;
    fn find_user_orders(&self, user_id: i64) -> Vec<Order>;
    fn ask_orders_stream(
        &'a self,
        sorted: bool,
    ) -> Box<dyn Iterator<Item = &'a dyn OrderTrait> + 'a>;
    fn bid_orders_stream(
        &'a self,
        sorted: bool,
    ) -> Box<dyn Iterator<Item = &'a dyn OrderTrait> + 'a>;
    fn get_l2_market_data_snapshot(&self, size: usize) -> L2MarketData;
    fn publish_l2_market_data_snapshot(&self, data: &mut L2MarketData);
    fn fill_asks(&self, size: usize, data: &mut L2MarketData);
    fn fill_bids(&self, size: usize, data: &mut L2MarketData);
    fn get_total_ask_buckets(&self, limit: usize) -> usize;
    fn get_total_bid_buckets(&self, limit: usize) -> usize;
    fn get_implementation_type(&self) -> OrderBookImplType;
    fn get_symbol_spec(&self) -> &CoreSymbolSpecification;
    fn validate_internal_state(&self);
}

pub fn from_bytes<'a>(
    bytes: &mut &'a [u8],
) -> Result<Box<dyn OrderBook<'a> + 'a>, borsh::io::Error> {
    let impl_type = OrderBookImplType::deserialize(bytes)?;
    match impl_type {
        OrderBookImplType::Naive => {
            let book = naive_impl::OrderBookNaiveImpl::from_bytes(bytes)?;
            Ok(Box::new(book))
        }
        OrderBookImplType::Direct => {
            let book = direct_impl::OrderBookDirectImpl::from_bytes(bytes)?;
            Ok(Box::new(book))
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, BorshSerialize, BorshDeserialize)]
pub enum OrderBookImplType {
    Naive,
    Direct,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct MatcherResult {
    pub volume: i64,
    pub orders_to_remove: Vec<i64>,
}
