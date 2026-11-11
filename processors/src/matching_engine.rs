use common::OrderCommand;
use common::OrderCommandType;
use common::PriceCache;
use common::Status;
use common::order_debug;
use common::order_warn;
use hashbrown::HashMap;
use vex_orderbook::OrderBook;
use vex_orderbook::tree::{BTreeAskSide, BTreeBidSide};

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
            panic!("Invalid number of shards {num_shards} - must be power of 2");
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

    /// Main entry point for processing orders. Callers route first: a router owns the markets
    /// with `market_id & shard_mask == shard_id`.
    pub fn process_order(&mut self, cmd: &mut OrderCommand, price_cache: &PriceCache) {
        if let Some(order_book) = self.order_books.get_mut(&cmd.market_id) {
            order_debug!(
                "matching_dispatch",
                cmd,
                stage = "matching",
                shard_id = self.shard_id
            );

            match cmd.command {
                OrderCommandType::PlaceOrder => order_book.place_order(cmd, price_cache),
                OrderCommandType::CancelOrder => order_book.cancel_order(cmd, price_cache),
                _ => {} // this should be unreachable as non-op commands are filtered out in routing macro.
            }
        } else {
            order_warn!(
                "matching_missing_orderbook",
                cmd,
                stage = "matching",
                shard_id = self.shard_id
            );
            cmd.set_status(Status::Rejected);
        }
    }
}
