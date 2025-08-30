use common::cmd::{OrderCommand};
use common::OrderCommandType;
use hashbrown::HashMap;
use vex_orderbook::tree::{BTreeAskSide, BTreeBidSide};
use vex_orderbook::OrderBook;
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
    pub order_books: HashMap<u32, OrderBook<BTreeAskSide , BTreeBidSide>>,
    pub shard_id: u32,
    pub shard_mask: u64,
}

impl MatchingEngineRouter {
    /// Creates a new router with sharding support
    ///
    /// # Arguments
    /// * `shard_id` - The ID of this shard (0, 1, 2, 3, etc.)
    /// * `num_shards` - Total number of shards (must be power of 2)
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
        }
    }

    /// Adds a new market_id to the matching engine, creating a new order book for it.
    /// Uses the provided symbol specification instead of hardcoded values
    pub fn add_symbol(
        &mut self,
        market_id: u32,
        spec: common::model::market_specification::CoreMarketSpecification,
    ) {
        self.order_books.insert(market_id, OrderBook::new(BTreeBidSide::new(), BTreeAskSide::new()));
    }

    /// Check if this router handles the given market_id
    ///
    /// The bitwise AND operation efficiently distributes symbols across shards:
    /// - With 4 shards (shard_mask = 3 = 0b11), symbols are distributed as:
    ///   - Market 0, 4, 8, 12... → Shard 0 (0 & 3 = 0)
    ///   - Market 1, 5, 9, 13... → Shard 1 (1 & 3 = 1)
    ///   - Market 2, 6, 10, 14... → Shard 2 (2 & 3 = 2)
    ///   - Market 3, 7, 11, 15... → Shard 3 (3 & 3 = 3)
    pub fn market_for_this_handler(&self, market_id: u64) -> bool {
        (self.shard_mask == 0) || ((market_id & self.shard_mask) == self.shard_id as u64)
    }

    /// Main entry point for processing orders
    pub fn process_order(&mut self, cmd: &mut OrderCommand) {
        if self.market_for_this_handler(cmd.market_id as u64) {
            if let Some(order_book) = self.order_books.get_mut(&cmd.market_id) {
                info!(
                    "[Router {}] Processing command for market_id {}",
                    self.shard_id, cmd.market_id
                );
    
                let result = match cmd.command {
                    OrderCommandType::PlaceOrder => order_book.place_order(cmd),
                    OrderCommandType::CancelOrder => order_book.cancel_order(cmd),
                };
            } else {
                warn!(
                    "[Router {}] No order book found for market_id {}",
                    self.shard_id, cmd.market_id
                );
            }
        }
    }
}

impl Default for MatchingEngineRouter {
    fn default() -> Self {
        Self::new()
    }
}
