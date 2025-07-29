use common::OrderCommandType;
use common::cmd::{OrderCommand, ProcessedOrderCommand, Status};
use hashbrown::HashMap;
use tracing::{info, warn};
use vex_orderbook::OrderBook;
use vex_orderbook::tree::{BTreeAskSide, BTreeBidSide};

/// Custom error type for routing failures.
#[derive(Debug)]
pub enum RoutingError {
    OrderBookNotFound,
    ProcessingFailed(OrderBookError),
}

use tracing::{info, warn};
/// Owns all order books and routes commands to the correct one.
pub struct MatchingEngineRouter {
    pub order_books: HashMap<u32, Box<OrderBook<BTreeAskSide, BTreeBidSide>>>,
    pub shard_id: u32,
    pub shard_mask: u64,
}

impl MatchingEngineRouter {
    /// Creates a new router with sharding support
    ///
    /// # Arguments
    /// * `shard_id` - The ID of this shard (0, 1, 2, 3, etc.)
    /// * `num_shards` - Total number of shards (must be power of 2)
    ///
    /// # Reasoning
    /// This matches the exchangeCore constructor: `MatchingEngineRouter(shardId, matchingEnginesNum, ...)`
    /// The power-of-2 validation ensures efficient bitwise operations for symbol_id distribution.
    pub fn new(shard_id: i32, num_shards: i64) -> Self {
        // Validate num_shards is power of 2
        if num_shards.count_ones() != 1 {
            panic!(
                "Invalid number of shards {} - must be power of 2",
                num_shards
            );
        }

        Self {
            order_books: HashMap::new(),
        }
    }

    /// Adds a new symbol_id to the matching engine, creating a new order book for it.
    pub fn add_symbol(&mut self, symbol_id: i32, book_type: OrderBookImplType) {
        let spec = common::model::symbol_specification::TestConstants::symbol_spec_eth_xbt();
        let book: Box<dyn OrderBook + Send> = match book_type {
            OrderBookImplType::Naive => Box::new(OrderBookNaiveImpl::new(spec)),
            OrderBookImplType::Direct => Box::new(OrderBookDirectImpl::new(spec)),
        };
        self.order_books.insert(symbol_id, book);
    }

    /// Check if this router handles the given symbol_id
    ///
    /// # Reasoning
    /// This implements the exact same logic as exchangeCore:
    /// ```java
    /// private boolean symbolForThisHandler(final long symbol_id) {
    ///     return (shardMask == 0) || ((symbol_id & shardMask) == shardId);
    /// }
    /// ```
    ///
    /// The bitwise AND operation efficiently distributes symbols across shards:
    /// - With 4 shards (shard_mask = 3 = 0b11), symbols are distributed as:
    ///   - Symbol 0, 4, 8, 12... → Shard 0 (0 & 3 = 0)
    ///   - Symbol 1, 5, 9, 13... → Shard 1 (1 & 3 = 1)
    ///   - Symbol 2, 6, 10, 14... → Shard 2 (2 & 3 = 2)
    ///   - Symbol 3, 7, 11, 15... → Shard 3 (3 & 3 = 3)
    pub fn symbol_for_this_handler(&self, symbol_id: i64) -> bool {
        (self.shard_mask == 0) || ((symbol_id & self.shard_mask) == self.shard_id as i64)
    }

    /// Main entry point for processing orders
    pub fn process_order(&mut self, cmd: &mut OrderCommand) -> ProcessedOrderCommand {
        let res =
            ProcessedOrderCommand::new(Status::Rejected, cmd.order_id, cmd.user_id , cmd.market_id, cmd.side);
        if self.market_for_this_handler(cmd.market_id as u64) {
            if let Some(order_book) = self.order_books.get_mut(&cmd.market_id) {
                info!(
                    "[Router {}] Processing command for market_id {}",
                    self.shard_id, cmd.market_id
                );

        match command {
            common::cmd::OrderCommandType::PlaceOrder
            | common::cmd::OrderCommandType::CancelOrder
            | common::cmd::OrderCommandType::MoveOrder
            | common::cmd::OrderCommandType::ReduceOrder => {
                // Process specific symbol_id group
                if self.symbol_for_this_handler(cmd.symbol_id as i64) {
                    self.process_matching_command(cmd);
                }
            }
        }
    }

    /// Process matching command
    ///
    /// # Reasoning
    /// This method implements the core matching logic, similar to exchangeCore's `processMatchingCommand`.
    /// It routes commands to the appropriate orderbook.
    fn process_matching_command(&mut self, cmd: &mut OrderCommand) {
        if let Some(order_book) = self.order_books.get_mut(&cmd.symbol_id) {
            info!(
                "[Router {}] Processing command for symbol_id {}",
                self.shard_id, cmd.symbol_id
            );

            let result = match cmd.command {
                common::cmd::OrderCommandType::PlaceOrder => order_book.new_order(cmd),
                common::cmd::OrderCommandType::CancelOrder => order_book.cancel_order(cmd),
                common::cmd::OrderCommandType::MoveOrder => order_book.move_order(cmd),
                common::cmd::OrderCommandType::ReduceOrder => order_book.reduce_order(cmd),
            };

            if let Err(e) = result {
                warn!(
                    "[Router {}] No order book found for market_id {}",
                    self.shard_id, cmd.market_id
                );
            }
        } else {
            warn!(
                "[Router {}] No order book found for symbol_id {}",
                self.shard_id, cmd.symbol_id
            );
        }
        res
    }
}

impl Default for MatchingEngineRouter {
    fn default() -> Self {
        Self::new()
    }
}
