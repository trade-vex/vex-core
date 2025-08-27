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
    /// Uses the provided symbol specification instead of hardcoded values
    pub fn add_symbol(&mut self, symbol_id: u32, spec: common::model::symbol_specification::CoreSymbolSpecification, book_type: OrderBookImplType) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use common::model::enums::{OrderType, Side, SymbolType};
    use common::model::symbol_specification::CoreSymbolSpecification;

    fn create_test_symbol_spec(symbol_id: u32) -> CoreSymbolSpecification {
        CoreSymbolSpecification {
            symbol_id,
            symbol_type: SymbolType::CurrencyExchangePair,
            base_currency: 1,   // BTC
            quote_currency: 2,  // USD
            base_scale_k: 1,
            quote_scale_k: 1,
            taker_fee: 0,
            maker_fee: 0,
            margin_buy: 0,
            margin_sell: 0,
        }
    }

    fn create_test_order_command(
        user_id: u64,
        symbol_id: u32,
        price: u64,
        size: u64,
        side: Side,
        command_type: OrderCommandType,
        order_id: u64,
    ) -> OrderCommand {
        OrderCommand {
            command: command_type,
            order_id,
            symbol_id,
            user_id,
            price,
            reserve_bid_price: 0,
            size,
            side,
            order_type: OrderType::Gtc,
            timestamp: 0,
            matcher_event: None,
        }
    }

    #[test]
    fn test_new_matching_engine_router() {
        let router = MatchingEngineRouter::new(1, 4);
        
        assert_eq!(router.shard_id, 1);
        assert_eq!(router.shard_mask, 3); // 4-1 = 3
        assert!(router.order_books.is_empty());
    }

    #[test]
    #[should_panic(expected = "Invalid number of shards 3 - must be power of 2")]
    fn test_new_matching_engine_router_invalid_shards() {
        MatchingEngineRouter::new(0, 3); // 3 is not a power of 2
    }

    #[test]
    fn test_symbol_for_this_handler() {
        let router = MatchingEngineRouter::new(1, 4);
        
        // Symbol ID 5 should be handled by shard 1 (5 & 3 = 1)
        assert!(router.symbol_for_this_handler(5));
        
        // Symbol ID 6 should not be handled by shard 1 (6 & 3 = 2)
        assert!(!router.symbol_for_this_handler(6));
        
        // Symbol ID 1 should be handled by shard 1 (1 & 3 = 1)
        assert!(router.symbol_for_this_handler(1));
        
        // Symbol ID 9 should be handled by shard 1 (9 & 3 = 1)
        assert!(router.symbol_for_this_handler(9));
    }

    #[test]
    fn test_symbol_for_this_handler_single_shard() {
        let router = MatchingEngineRouter::new(0, 1);
        
        // With shard_mask = 0, all symbols should be handled
        assert!(router.symbol_for_this_handler(0));
        assert!(router.symbol_for_this_handler(1));
        assert!(router.symbol_for_this_handler(100));
        assert!(router.symbol_for_this_handler(999));
    }

    #[test]
    fn test_add_symbol_naive() {
        let mut router = MatchingEngineRouter::new(0, 1);
        let spec = create_test_symbol_spec(123);
        
        router.add_symbol(123, spec, OrderBookImplType::Naive);
        
        assert_eq!(router.order_books.len(), 1);
        assert!(router.order_books.contains_key(&123));
    }

    #[test]
    fn test_add_symbol_direct() {
        let mut router = MatchingEngineRouter::new(0, 1);
        let spec = create_test_symbol_spec(456);
        
        router.add_symbol(456, spec, OrderBookImplType::Direct);
        
        assert_eq!(router.order_books.len(), 1);
        assert!(router.order_books.contains_key(&456));
    }

    #[test]
    fn test_add_multiple_symbols() {
        let mut router = MatchingEngineRouter::new(0, 1);
        
        router.add_symbol(1, create_test_symbol_spec(1), OrderBookImplType::Naive);
        router.add_symbol(2, create_test_symbol_spec(2), OrderBookImplType::Direct);
        router.add_symbol(3, create_test_symbol_spec(3), OrderBookImplType::Naive);
        
        assert_eq!(router.order_books.len(), 3);
        assert!(router.order_books.contains_key(&1));
        assert!(router.order_books.contains_key(&2));
        assert!(router.order_books.contains_key(&3));
    }

    #[test]
    fn test_process_order_wrong_shard() {
        let mut router = MatchingEngineRouter::new(1, 4);
        router.add_symbol(1, create_test_symbol_spec(1), OrderBookImplType::Naive);
        
        // Create order for symbol 2, which belongs to shard 2 (2 & 3 = 2)
        let mut cmd = create_test_order_command(
            100, 2, 1000, 10, Side::Bid, OrderCommandType::PlaceLimitOrder, 1
        );
        
        // Should be ignored since symbol 2 doesn't belong to shard 1
        router.process_order(&mut cmd);
        
        // Verify that the order book for symbol 1 is still empty
        if let Some(order_book) = router.order_books.get(&1) {
            assert_eq!(order_book.get_orders_num(Side::Ask), 0);
            assert_eq!(order_book.get_orders_num(Side::Bid), 0);
        }
    }

    #[test]
    fn test_process_order_correct_shard() {
        let mut router = MatchingEngineRouter::new(1, 4);
        router.add_symbol(1, create_test_symbol_spec(1), OrderBookImplType::Naive);
        
        // Create order for symbol 1, which belongs to shard 1 (1 & 3 = 1)
        let mut cmd = create_test_order_command(
            100, 1, 1000, 10, Side::Bid, OrderCommandType::PlaceLimitOrder, 1
        );
        
        router.process_order(&mut cmd);
        
        // Verify that the order was processed
        if let Some(order_book) = router.order_books.get(&1) {
            assert_eq!(order_book.get_orders_num(Side::Bid), 1);
            assert_eq!(order_book.get_orders_num(Side::Ask), 0);
        }
    }

    #[test]
    fn test_process_order_no_orderbook() {
        let mut router = MatchingEngineRouter::new(1, 4);
        // Don't add any order books
        
        // Create order for symbol 1
        let mut cmd = create_test_order_command(
            100, 1, 1000, 10, Side::Bid, OrderCommandType::PlaceLimitOrder, 1
        );
        
        // Should not panic, just log a warning
        router.process_order(&mut cmd);
        
        // Verify no order books exist
        assert!(router.order_books.is_empty());
    }

    #[test]
    fn test_process_limit_order() {
        let mut router = MatchingEngineRouter::new(0, 1);
        router.add_symbol(1, create_test_symbol_spec(1), OrderBookImplType::Naive);
        
        let mut bid_cmd = create_test_order_command(
            100, 1, 1000, 10, Side::Bid, OrderCommandType::PlaceLimitOrder, 1
        );
        let mut ask_cmd = create_test_order_command(
            101, 1, 1100, 5, Side::Ask, OrderCommandType::PlaceLimitOrder, 2
        );
        
        router.process_order(&mut bid_cmd);
        router.process_order(&mut ask_cmd);
        
        if let Some(order_book) = router.order_books.get(&1) {
            assert_eq!(order_book.get_orders_num(Side::Bid), 1);
            assert_eq!(order_book.get_orders_num(Side::Ask), 1);
        }
    }

    #[test]
    fn test_process_cancel_order() {
        let mut router = MatchingEngineRouter::new(0, 1);
        router.add_symbol(1, create_test_symbol_spec(1), OrderBookImplType::Naive);
        
        // Place a limit order first
        let mut place_cmd = create_test_order_command(
            100, 1, 1000, 10, Side::Bid, OrderCommandType::PlaceLimitOrder, 1
        );
        router.process_order(&mut place_cmd);
        
        // Verify the order was placed
        if let Some(order_book) = router.order_books.get(&1) {
            assert_eq!(order_book.get_orders_num(Side::Bid), 1);
        }
        
        // Cancel the order
        let mut cancel_cmd = create_test_order_command(
            100, 1, 0, 0, Side::Bid, OrderCommandType::CancelOrder, 1
        );
        router.process_order(&mut cancel_cmd);
        
        // Verify the order was cancelled
        if let Some(order_book) = router.order_books.get(&1) {
            assert_eq!(order_book.get_orders_num(Side::Bid), 0);
        }
    }

    #[test]
    fn test_get_order_books() {
        let mut router = MatchingEngineRouter::new(0, 1);
        router.add_symbol(1, create_test_symbol_spec(1), OrderBookImplType::Naive);
        router.add_symbol(2, create_test_symbol_spec(2), OrderBookImplType::Direct);
        
        let order_books = router.get_order_books();
        assert_eq!(order_books.len(), 2);
        assert!(order_books.contains_key(&1));
        assert!(order_books.contains_key(&2));
    }

    #[test]
    fn test_sharding_distribution() {
        // Test that symbols are properly distributed across 4 shards
        let shards = [
            MatchingEngineRouter::new(0, 4),
            MatchingEngineRouter::new(1, 4),
            MatchingEngineRouter::new(2, 4),
            MatchingEngineRouter::new(3, 4),
        ];
        
        // Test symbols 0-15 to verify distribution
        for symbol_id in 0u64..16 {
            let expected_shard = (symbol_id & 3) as usize;
            
            for (shard_idx, shard) in shards.iter().enumerate() {
                if shard_idx == expected_shard {
                    assert!(shard.symbol_for_this_handler(symbol_id),
                        "Symbol {} should be handled by shard {}", symbol_id, shard_idx);
                } else {
                    assert!(!shard.symbol_for_this_handler(symbol_id),
                        "Symbol {} should NOT be handled by shard {}", symbol_id, shard_idx);
                }
            }
        }
    }

    #[test]
    fn test_matching_across_multiple_orders() {
        let mut router = MatchingEngineRouter::new(0, 1);
        router.add_symbol(1, create_test_symbol_spec(1), OrderBookImplType::Naive);
        
        // Place multiple orders at different price levels
        let orders = vec![
            create_test_order_command(100, 1, 1000, 10, Side::Bid, OrderCommandType::PlaceLimitOrder, 1),
            create_test_order_command(101, 1, 1100, 15, Side::Ask, OrderCommandType::PlaceLimitOrder, 2),
            create_test_order_command(102, 1, 1050, 5, Side::Bid, OrderCommandType::PlaceLimitOrder, 3),
            create_test_order_command(103, 1, 1200, 8, Side::Ask, OrderCommandType::PlaceLimitOrder, 4),
        ];
        
        for mut order in orders {
            router.process_order(&mut order);
        }
        
        if let Some(order_book) = router.order_books.get(&1) {
            assert_eq!(order_book.get_orders_num(Side::Bid), 2); // Two bid orders
            assert_eq!(order_book.get_orders_num(Side::Ask), 2); // Two ask orders
        }
    }

    #[test]
    fn test_default_matching_engine_router() {
        let router = MatchingEngineRouter::default();
        
        assert_eq!(router.shard_id, 0);
        assert_eq!(router.shard_mask, 0); // Single shard (1-1=0)
        assert!(router.order_books.is_empty());
    }

    #[test]
    fn test_process_order_invalid_symbol_type() {
        let mut router = MatchingEngineRouter::new(0, 1);
        router.add_symbol(1, create_test_symbol_spec(1), OrderBookImplType::Naive);
        
        // Create an order with an invalid/unsupported command type scenario
        let mut cmd = create_test_order_command(
            100, 1, 1000, 10, Side::Bid, OrderCommandType::PlaceLimitOrder, 1
        );
        
        // Change symbol_id to one that doesn't exist
        cmd.symbol_id = 999;
        
        // Should not panic, just handle gracefully
        router.process_order(&mut cmd);
        
        // Original symbol should be unaffected
        if let Some(order_book) = router.order_books.get(&1) {
            assert_eq!(order_book.get_orders_num(Side::Bid), 0);
            assert_eq!(order_book.get_orders_num(Side::Ask), 0);
        }
    }
}
