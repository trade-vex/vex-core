use common::cmd::{OrderCommand, OrderCommandType};
use hashbrown::HashMap;
use orderbook::OrderBook;
use orderbook::OrderBookImplType;
use orderbook::direct_impl::OrderBookDirectImpl;
use orderbook::naive_impl::OrderBookNaiveImpl;
use tracing::{info, warn};

/// Owns all order books and routes commands to the correct one.
/// This is the Rust equivalent of `MatchingEngineRouter.java`.
pub struct MatchingEngineRouter {
    pub order_books: HashMap<u32, Box<dyn OrderBook<'static> + Send>>,
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
    pub fn new(shard_id: u32, num_shards: u64) -> Self {
        // Validate num_shards is power of 2
        if num_shards.count_ones() != 1 {
            panic!(
                "Invalid number of shards {} - must be power of 2",
                num_shards
            );
        }

        Self {
            order_books: HashMap::new(),
            shard_id,
            shard_mask: num_shards - 1, // Creates mask : shardMask = numShards - 1
        }
    }

    /// Adds a new symbol_id to the matching engine, creating a new order book for it.
    pub fn add_symbol(&mut self, symbol_id: u32, book_type: OrderBookImplType) {
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
    pub fn symbol_for_this_handler(&self, symbol_id: u64) -> bool {
        (self.shard_mask == 0) || ((symbol_id & self.shard_mask) == self.shard_id as u64)
    }

    /// Main entry point for processing orders
    ///
    /// # Reasoning
    /// This method mirrors the exchangeCore `processOrder(long seq, OrderCommand cmd)` method.
    /// It implements the same command routing logic where each router only processes
    /// commands for symbols it owns
    pub fn process_order(&mut self, cmd: &mut OrderCommand) {
        if self.symbol_for_this_handler(cmd.symbol_id as u64) {
            self.process_matching_command(cmd);
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
                OrderCommandType::PlaceLimitOrder => order_book.new_order(cmd),
                OrderCommandType::PlaceMarketOrder => order_book.new_order(cmd),
                OrderCommandType::CancelOrder => order_book.cancel_order(cmd),
            };

            if let Err(e) = result {
                warn!(
                    "[Router {}] Order book processing failed: {:?}",
                    self.shard_id, e
                );
            }
        } else {
            warn!(
                "[Router {}] No order book found for symbol_id {}",
                self.shard_id, cmd.symbol_id
            );
        }
    }

    /// Get order books for external access
    pub fn get_order_books(&self) -> &HashMap<u32, Box<dyn OrderBook<'static> + Send>> {
        &self.order_books
    }
}

impl Default for MatchingEngineRouter {
    fn default() -> Self {
        Self::new(0, 1)
    }
}
