//! # Vex Order Book Implementation
//!
//! This module provides a fast and efficient implementation of a limit order book (LOB).
//! It is designed for low-latency, considering existing Architecure of VEX-CORE.
//!
//! ## Core Design Principles
//!
//! 1.  **Data Structure Choice:**
//!     -   **Price Levels (`BookSide`):** BookSide is a trait that defines the interface for accessing
//!         price levels. This provides sorted iteration, which is essential for matching orders.
//!         -   **Asks:** BookSide implementations for asks should provide access to price levels
//!             sorted ascendingly (lowest price first).
//!         -   **Bids:** BookSide implementations for bids should provide access to price levels
//!             sorted descendingly (highest price first).
//!     -   **Order Queue (`VecDeque`):** Within each `PriceLevel`, a `VecDeque` stores the orders.
//!         This acts as a FIFO (First-In, First-Out) queue, ensuring time priority for orders
//!         at the same price.
//!     -   **Direct Order Access (`HashMap`):** A `HashMap<u64, u64>` provides O(1) average-case
//!         lookup time for order-price pairs by their ID. This is crucial for fast cancellation.
//!
//! 2.  **Performance Characteristics:** [Depends on BookSide Implementation]
//!     [For BTreeMap-based BookSide]
//!     -   **Placing an order:**
//!         -   Matching: O(M * N), where M is the number of price levels crossed and N is the average
//!             number of orders at each level. In practice, this is very fast.
//!         -   Resting a new limit order: O(log P), where P is the number of price levels on that side of the book.
//!     -   **Canceling an order:** O(log P + Q), where P is the number of price levels and Q is the
//!         number of orders at the specific price level of the canceled order. The `+ Q` is due
//!         to the linear scan required to find the order in the `VecDeque`. For extreme performance,
//!         this `VecDeque` could be replaced with an intrusive doubly-linked list (see suggestions below).
//!
//! 3.  **State Management:**
//!     -   The order book is self-contained and mutates its state through the `place_order` and
//!         `cancel_order` methods.
//!     -   It generates `MatcherTradeEvent`s and attaches them to the `cmdOrderCommand` for downstream
//!         processors (risk engines and event handlers) to consume.
use crate::tree::BookSide;
use common::{
    L2MarketData, L2SIZE, MatcherTradeEvent, Order, OrderCommand, PriceCache, Side, Status,
    TimeInForce, UserBalance,
};
use std::{
    collections::HashMap,
    ptr::NonNull,
    sync::Arc,
};

pub mod tree;
mod unit_tests;

/// Intrusive doubly-linked list node for O(1) order removal
struct OrderNode {
    order: Order,
    prev: Option<NonNull<OrderNode>>,
    next: Option<NonNull<OrderNode>>,
}

impl OrderNode {
    fn new(order: Order) -> Box<Self> {
        Box::new(Self {
            order,
            prev: None,
            next: None,
        })
    }
}

#[derive(Debug)]
pub struct PriceLevel {
    total_volume: u64,
    head: Option<NonNull<OrderNode>>,
    tail: Option<NonNull<OrderNode>>,
    count: usize,
}

impl PriceLevel {
    fn new() -> Self {
        Self {
            total_volume: 0,
            head: None,
            tail: None,
            count: 0,
        }
    }

    #[inline]
    fn add_order(&mut self, order: Order) -> NonNull<OrderNode> {
        self.total_volume += order.size;
        let mut new_node = OrderNode::new(order);
        let new_node_ptr = NonNull::from(&mut *new_node);

        unsafe {
            if let Some(mut tail_ptr) = self.tail {
                // Link the new node at the tail
                tail_ptr.as_mut().next = Some(new_node_ptr);
                new_node.prev = Some(tail_ptr);
                self.tail = Some(new_node_ptr);
            } else {
                // First node
                self.head = Some(new_node_ptr);
                self.tail = Some(new_node_ptr);
            }
        }

        Box::leak(new_node); // Keep the node alive
        self.count += 1;
        new_node_ptr
    }

    /// Remove a specific order node by pointer (O(1))
    #[inline]
    fn remove_order_node(&mut self, node_ptr: NonNull<OrderNode>) -> Order {
        unsafe {
            let node = Box::from_raw(node_ptr.as_ptr());

            // Update prev node's next pointer
            if let Some(mut prev_ptr) = node.prev {
                prev_ptr.as_mut().next = node.next;
            } else {
                // Removing head
                self.head = node.next;
            }

            // Update next node's prev pointer
            if let Some(mut next_ptr) = node.next {
                next_ptr.as_mut().prev = node.prev;
            } else {
                // Removing tail
                self.tail = node.prev;
            }

            self.count -= 1;
            self.total_volume = self.total_volume.saturating_sub(node.order.size);
            node.order
        }
    }

    /// Get the total volume at this price level
    pub fn get_total_volume(&self) -> u64 {
        self.total_volume
    }

    /// Get the number of orders at this price level
    pub fn get_order_count(&self) -> u64 {
        self.count as u64
    }

    /// Check if the price level is empty
    fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Iterator over orders in FIFO order
    pub fn iter(&self) -> PriceLevelIterator {
        PriceLevelIterator {
            current: self.head,
        }
    }
}

/// Iterator for traversing orders in a price level
pub struct PriceLevelIterator {
    current: Option<NonNull<OrderNode>>,
}

impl Iterator for PriceLevelIterator {
    type Item = Order;

    fn next(&mut self) -> Option<Self::Item> {
        self.current.map(|node_ptr| unsafe {
            let node = node_ptr.as_ref();
            self.current = node.next;
            node.order.clone()
        })
    }
}

impl Drop for PriceLevel {
    fn drop(&mut self) {
        // Clean up all nodes in the list
        let mut current = self.head;
        while let Some(node_ptr) = current {
            unsafe {
                let node = Box::from_raw(node_ptr.as_ptr());
                current = node.next;
            }
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
    /// Order ID -> (price, node pointer) for O(1) cancellations
    orders: HashMap<u64, (u64, NonNull<OrderNode>)>,
    /// Market ID for this order book
    market_id: u32,
}

impl<Ask: BookSide, Bid: BookSide> OrderBook<Ask, Bid> {
    /// Creates a new empty order book.
    pub fn new(bids: Bid, asks: Ask, market_id: u32) -> Self {
        Self {
            market_id,
            bids,
            asks,
            orders: HashMap::new(),
        }
    }

    /// Matches Order
    /// The core matching logic for an incoming taker order.
    fn match_order(&mut self, cmd: &mut OrderCommand) -> u64 {
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

            let mut nodes_to_remove = Vec::new();

            // Iterate through orders at this price level (FIFO via intrusive list)
            let mut current_node = level.head;
            while let Some(node_ptr) = current_node {
                if remaining_size == 0 {
                    break;
                }

                unsafe {
                    let node = node_ptr.as_ref();
                    let maker_order = &node.order;

                    if maker_order.user_id == cmd.user_id {
                        current_node = node.next;
                        continue;
                    }

                    let trade_size = remaining_size.min(maker_order.size);

                    // Update sizes
                    remaining_size -= trade_size;
                    let new_maker_size = maker_order.size - trade_size;

                    let maker_order_completed = new_maker_size == 0;

                    // Create and attach the trade event
                    let event = MatcherTradeEvent {
                        active_order_completed: remaining_size == 0,
                        matched_order_id: maker_order.order_id,
                        maker_user_id: maker_order.user_id,
                        matched_order_completed: maker_order_completed,
                        price,
                        size: trade_size,
                        next_event: None,
                        maker_balance: [UserBalance::default(); 2], // filled by risk engine
                    };
                    cmd.attatch_event(Box::new(event));

                    if maker_order_completed {
                        nodes_to_remove.push((maker_order.order_id, node_ptr));
                        current_node = node.next;
                    } else {
                        // Partial fill - update the order size and volume in place
                        let node_mut = node_ptr.as_ptr();
                        (*node_mut).order.size = new_maker_size;
                        level.total_volume -= trade_size;
                        current_node = node.next;
                    }
                }
            }

            // Remove filled orders from the list and hashmap
            for (order_id, node_ptr) in nodes_to_remove {
                level.remove_order_node(node_ptr);
                self.orders.remove(&order_id);
            }

            if level.is_empty() {
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
    ///     size: 0, /// as set by user, changes to remaining size
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
    pub fn place_order(&mut self, cmd: &mut OrderCommand, price_cache: Arc<PriceCache>) {
        // If the command is already rejected (e.g., by upstream validation), skip processing
        if cmd.status == Status::Rejected {
            return;
        }

        match cmd.time_in_force {
            TimeInForce::Gtc => {
                // Handle GTC (Good 'Til Canceled) orders
                let remaining = self.match_order(cmd);
                if remaining == cmd.size {
                    self.add_to_book(cmd, remaining);
                    cmd.set_status(Status::Placed);
                } else if remaining > 0 {
                    // Add remaining to book
                    self.add_to_book(cmd, remaining);
                    cmd.set_status(Status::PartiallyFilled);
                } else {
                    cmd.set_status(Status::Filled);
                }
                cmd.set_size(remaining);
            }
            TimeInForce::Fok => {
                if !self.can_fill_completely(cmd) {
                    cmd.set_status(Status::Cancelled);
                } else {
                    let remaining = self.match_order(cmd);
                    cmd.set_status(Status::Filled);
                    cmd.set_size(remaining);
                }
            }
            TimeInForce::Ioc => {
                // Handle IOC (Immediate or Cancel) orders
                let remaining = self.match_order(cmd);

                if remaining == 0 {
                    cmd.set_status(Status::Filled);
                } else if remaining < cmd.size {
                    cmd.set_status(Status::PartiallyFilled);
                } else {
                    cmd.set_status(Status::Cancelled);
                }
                cmd.set_size(remaining);
            }
        }
        if cmd.status != Status::Rejected {
            self.record_snapshot(cmd);
            self.update_price_cache(price_cache);
        }
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
    pub fn cancel_order(&mut self, cmd: &mut OrderCommand, price_cache: Arc<PriceCache>) {
        // Similar to place_order, if the command is already rejected, in case the user is not found
        if cmd.status == Status::Rejected {
            return;
        }
        // O(1) lookup via HashMap: order_id -> (price, node_ptr)
        if let Some((price, node_ptr)) = self.orders.remove(&cmd.order_id) {
            if let Some(level) = self.bids.get_level_mut(price) {
                let removed_order = level.remove_order_node(node_ptr);
                cmd.set_price(removed_order.price);
                cmd.set_size(removed_order.size);
                cmd.set_status(Status::Cancelled);
                self.bids.remove_level_if_empty(price);
                self.record_snapshot(cmd);
            } else if let Some(level) = self.asks.get_level_mut(price) {
                let removed_order = level.remove_order_node(node_ptr);
                cmd.set_price(removed_order.price);
                cmd.set_size(removed_order.size);
                cmd.set_status(Status::Cancelled);
                self.asks.remove_level_if_empty(price);
                self.record_snapshot(cmd);
            } else {
                // this must ideally be unreachable, to avoid any undefined behaviour, we reject the order
                cmd.set_status(Status::Rejected);
            }
        } else {
            cmd.set_status(Status::Rejected);
        }
        if cmd.status != Status::Rejected {
            self.update_price_cache(price_cache);
        }
    }

    fn update_price_cache(&self, price_cache: Arc<PriceCache>) {
        price_cache.update_prices(
            self.market_id,
            self.bids.best_price(),
            self.asks.best_price(),
        );
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
        let node_ptr = level.add_order(order);
        self.orders.insert(cmd.order_id, (cmd.price, node_ptr));
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

    /// Get iterator over bid levels (highest price first)
    pub fn get_bids(&self) -> Box<dyn Iterator<Item = (u64, &PriceLevel)> + '_> {
        self.bids.iter()
    }

    /// Get iterator over ask levels (lowest price first)
    pub fn get_asks(&self) -> Box<dyn Iterator<Item = (u64, &PriceLevel)> + '_> {
        self.asks.iter()
    }

    /// Create a snapshot of the orderbook data with specified depth
    pub fn record_snapshot(&self, cmd: &mut OrderCommand) {
        let mut l2_data = L2MarketData::new();

        // Fill bid levels (highest price first)
        let mut bid_idx = 0;
        for (price, level) in self.get_bids().take(L2SIZE) {
            l2_data.bid_prices[bid_idx] = price;
            l2_data.bid_volumes[bid_idx] = level.get_total_volume();
            bid_idx += 1;
        }
        l2_data.bid_depth = bid_idx;

        // Fill ask levels (lowest price first)
        let mut ask_idx = 0;
        for (price, level) in self.get_asks().take(L2SIZE) {
            l2_data.ask_prices[ask_idx] = price;
            l2_data.ask_volumes[ask_idx] = level.get_total_volume();
            ask_idx += 1;
        }
        l2_data.ask_depth = ask_idx;

        // Set timestamp
        l2_data.timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        cmd.l2_data = Some(l2_data);
    }
}
