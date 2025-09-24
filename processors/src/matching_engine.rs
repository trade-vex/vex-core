use std::sync::Arc;

use common::L2MarketData;
use common::OrderCommand;
use common::OrderCommandType;
use common::PriceCache;
use common::Status;
use hashbrown::HashMap;
use tracing::{info, warn};
use vex_orderbook::tree::{BTreeAskSide, BTreeBidSide};
use vex_orderbook::OrderBook;

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

    /// Adds a new market_id to the matching engine, creating a new order book for it.
    /// Uses the provided symbol specification instead of hardcoded values
    pub fn add_market(&mut self, market_id: u32) {
        self.order_books.insert(
            market_id,
            Box::new(OrderBook::new(
                BTreeBidSide::new(),
                BTreeAskSide::new(),
                market_id,
            )),
        );
    }

    /// Check if this router handles the given market_id
    ///
    /// The bitwise AND operation efficiently distributes symbols across shards:
    /// - With 4 shards (shard_mask = 3 = 0b11), symbols are distributed as:
    ///   - Market 0, 4, 8, 12... → Shard 0 (0 & 3 = 0)
    ///   - Market 1, 5, 9, 13... → Shard 1 (1 & 3 = 1)
    ///   - Market 2, 6, 10, 14... → Shard 2 (2 & 3 = 2)
    ///   - Market 3, 7, 11, 15... → Shard 3 (3 & 3 = 3)
    #[inline]
    pub fn market_for_this_handler(&self, market_id: u64) -> bool {
        (self.shard_mask == 0) || ((market_id & self.shard_mask) == self.shard_id as u64)
    }

    /// Main entry point for processing orders
    pub fn process_order(&mut self, cmd: &mut OrderCommand, price_cache: Arc<PriceCache>) {
        if self.market_for_this_handler(cmd.market_id as u64) {
            if let Some(order_book) = self.order_books.get_mut(&cmd.market_id) {
                info!(
                    "[Router {}] Processing command for market_id {}",
                    self.shard_id, cmd.market_id
                );

                match cmd.command {
                    OrderCommandType::PlaceOrder => order_book.place_order(cmd, price_cache),
                    OrderCommandType::CancelOrder => order_book.cancel_order(cmd, price_cache),
                }
            } else {
                warn!(
                    "[Router {}] No order book found for market_id {}",
                    self.shard_id, cmd.market_id
                );
                cmd.set_status(Status::Rejected);
            }
        }
    }

    /// Get a reference to the orderbook for a specific market_id
    pub fn get_orderbook(&self, market_id: u32) -> Option<&OrderBook<BTreeAskSide, BTreeBidSide>> {
        self.order_books
            .get(&market_id)
            .map(|boxed_book| boxed_book.as_ref())
    }
}

impl Default for MatchingEngineRouter {
    fn default() -> Self {
        Self::new(0, 1)
    }
}
