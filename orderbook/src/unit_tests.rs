#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::tree::{BTreeAskSide, BTreeBidSide};
    use crate::*;
    use common::{OrderCommand, Status, MatcherTradeEvent};
    use common::{OrderCommandType, Side};

    /// Helper functions to inspect the internal state of the `OrderBook`.
    /// These are compiled only when `#[cfg(test)]` is active.
    impl<Ask: BookSide, Bid: BookSide> OrderBook<Ask, Bid> {
        pub fn get_level_volume(&self, side: Side, price: u64) -> u64 {
            let find_result = match side {
                Side::Bid => self.bids.iter().find(|(p, _)| *p == price),
                Side::Ask => self.asks.iter().find(|(p, _)| *p == price),
            };
            find_result.map_or(0, |(_, level)| level.total_volume)
        }

        pub fn get_level_order_count(&self, side: Side, price: u64) -> usize {
            let find_result = match side {
                Side::Bid => self.bids.iter().find(|(p, _)| *p == price),
                Side::Ask => self.asks.iter().find(|(p, _)| *p == price),
            };
            find_result.map_or(0, |(_, level)| level.orders.len())
        }
    }

    /// Helper functions to inspect the internal state of the `OrderBook`.
    /// These are compiled only when `#[cfg(test)]` is active.
    #[cfg(test)]
    impl<Ask: BookSide, Bid: BookSide> OrderBook<Ask, Bid> {
        /// Asserts the state of a specific price level and its data consistency.
        #[cfg(test)]
        pub fn assert_level_state(
            &self,
            side: Side,
            price: u64,
            expected_volume: u64,
            expected_order_count: usize,
        ) {
            let (book_side, order_map): (&dyn BookSide, &HashMap<u64, u64>) = match side {
                Side::Bid => (&self.bids, &self.orders),
                Side::Ask => (&self.asks, &self.orders),
            };

            let level_opt = book_side.iter().find(|(p, _)| *p == price);

            if expected_volume == 0 {
                if let Some((_, level)) = level_opt {
                    assert_eq!(
                        level.total_volume, 0,
                        "Level at price {price} should be empty but has volume"
                    );
                    assert!(
                        level.orders.is_empty(),
                        "Level at price {price} should have no orders"
                    );
                }
                // If the level doesn't exist at all, that's also valid for an expected volume of 0.
            } else {
                let (_, level) = level_opt.unwrap_or_else(|| {
                    panic!("Expected price level at {price} for side {side:?} not found")
                });
                assert_eq!(
                    level.total_volume, expected_volume,
                    "Volume mismatch at price {price} for side {side:?}"
                );
                assert_eq!(
                    level.orders.len(),
                    expected_order_count,
                    "Order count mismatch at price {price} for side {side:?}"
                );

                // Verify internal consistency: total_volume should match sum of order sizes.
                let actual_summed_volume: u64 = level.orders.iter().map(|o| o.size).sum();
                assert_eq!(
                    level.total_volume, actual_summed_volume,
                    "Internal volume sum inconsistency at price {price}"
                );

                // Verify fast lookup map is consistent with the level's orders.
                for order in &level.orders {
                    assert_eq!(
                        order_map.get(&order.order_id),
                        Some(&price),
                        "Order map out of sync for order_id {}",
                        order.order_id
                    );
                }
            }
        }
    }

    #[cfg(test)]
    impl<Ask: BookSide, Bid: BookSide> OrderBook<Ask, Bid> {
        /// Helper function to check if the order book state is consistent
        pub fn verify_state(&mut self) -> Result<(), String> {
            // Check bid_orders consistency
            for (order_id, price) in &self.orders {
                if let Some(level) = self.bids.get_level_mut(*price) {
                    if !level.orders.iter().any(|o| o.order_id == *order_id) {
                        return Err(format!(
                            "Bid order {order_id} at price {price} not found in level"
                        ));
                    }
                } else if let Some(level) = self.asks.get_level_mut(*price) {
                    if !level.orders.iter().any(|o| o.order_id == *order_id) {
                        return Err(format!(
                            "Ask order {order_id} at price {price} not found in level"
                        ));
                    }
                } else {
                    return Err(format!(
                        "Order {order_id} references non-existent price level {price}"
                    ));
                }
            }

            // Check that total_volume matches sum of order sizes
            for (price, level) in self.bids.iter() {
                let calculated_volume: u64 = level.orders.iter().map(|o| o.size).sum();
                if calculated_volume != level.total_volume {
                    return Err(format!(
                        "Bid level at {} has inconsistent volume: {} vs {}",
                        price, level.total_volume, calculated_volume
                    ));
                }
            }

            for (price, level) in self.asks.iter() {
                let calculated_volume: u64 = level.orders.iter().map(|o| o.size).sum();
                if calculated_volume != level.total_volume {
                    return Err(format!(
                        "Ask level at {} has inconsistent volume: {} vs {}",
                        price, level.total_volume, calculated_volume
                    ));
                }
            }

            Ok(())
        }

        /// Get the best bid price and volume
        pub fn best_bid(&self) -> Option<(u64, u64)> {
            self.bids
                .iter()
                .next()
                .map(|(price, level)| (price, level.total_volume))
        }

        /// Get the best ask price and volume
        pub fn best_ask(&self) -> Option<(u64, u64)> {
            self.asks
                .iter()
                .next()
                .map(|(price, level)| (price, level.total_volume))
        }

        /// Get total volume at a specific price level
        pub fn get_volume_at_price(&self, price: u64, side: Side) -> u64 {
            match side {
                Side::Bid => self
                    .bids
                    .iter()
                    .find(|(p, _)| *p == price)
                    .map(|(_, level)| level.total_volume)
                    .unwrap_or(0),
                Side::Ask => self
                    .asks
                    .iter()
                    .find(|(p, _)| *p == price)
                    .map(|(_, level)| level.total_volume)
                    .unwrap_or(0),
            }
        }

        /// Count total number of orders in the book
        pub fn total_order_count(&self) -> usize {
            let bid_count: usize = self.bids.iter().map(|(_, level)| level.orders.len()).sum();
            let ask_count: usize = self.asks.iter().map(|(_, level)| level.orders.len()).sum();
            bid_count + ask_count
        }

        /// Get order by ID (for testing)
        pub fn get_order(&mut self, order_id: u64) -> Option<&Order> {
            // Check bids first
            if let Some(price) = self.orders.get(&order_id) {
                if let Some(best_price) = self.bids.best_price()
                    && *price <= best_price
                    && let Some(level) = self.bids.get_level_mut(*price)
                {
                    return level.orders.iter().find(|o| o.order_id == order_id);
                } else if let Some(price) = self.orders.get(&order_id) {
                    if let Some(level) = self.asks.get_level_mut(*price) {
                        return level.orders.iter().find(|o| o.order_id == order_id);
                    }
                }
            }
            None
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn create_order_command(
        command: OrderCommandType,
        order_id: u64,
        timestamp: u64,
        user_id: u64,
        market_id: u32,
        price: u64,
        size: u64,
        side: Side,
        time_in_force: TimeInForce,
    ) -> OrderCommand {
        OrderCommand {
            command,
            order_id,
            timestamp,
            user_id,
            market_id,
            price,
            size,
            side,
            time_in_force,
            status: Status::Rejected,
            events: None,
        }
    }

    // Helper to verify trade events
    fn verify_trade_events(
        processed: &OrderCommand,
        expected_trades: &[(u64, u64, u64, bool, bool)],
    ) {
        let mut event_opt = processed.events();
        let mut trade_count = 0;

        while let Some(event) = event_opt {
            assert!(
                trade_count < expected_trades.len(),
                "More trades than expected"
            );
            let (
                expected_price,
                expected_size,
                expected_maker_id,
                expected_active_completed,
                expected_maker_completed,
            ) = expected_trades[trade_count];

            assert_eq!(
                event.price, expected_price,
                "Trade {trade_count} price mismatch"
            );
            assert_eq!(
                event.size, expected_size,
                "Trade {trade_count} size mismatch"
            );
            assert_eq!(
                event.maker_user_id, expected_maker_id,
                "Trade {trade_count} maker user mismatch"
            );
            assert_eq!(
                event.active_order_completed, expected_active_completed,
                "Trade {trade_count} active completion mismatch"
            );
            assert_eq!(
                event.matched_order_completed, expected_maker_completed,
                "Trade {trade_count} maker completion mismatch"
            );

            trade_count += 1;

            event_opt = event.next_event.as_deref();
        }

        assert_eq!(
            trade_count,
            expected_trades.len(),
            "Expected {} trades, got {}",
            expected_trades.len(),
            trade_count
        );
    }

    type TestOrderBook = OrderBook<BTreeAskSide, BTreeBidSide>;

    fn create_test_orderbook() -> TestOrderBook {
        OrderBook::new(BTreeBidSide::new(), BTreeAskSide::new())
    }

    #[test]
    fn test_empty_orderbook_creation() {
        let mut book = create_test_orderbook();
        assert_eq!(book.total_order_count(), 0);
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_place_single_bid_gtc() {
        let mut book = create_test_orderbook();
        let cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            1,
            100,
            1001,
            1,
            50,
            100,
            Side::Bid,
            TimeInForce::Gtc,
        );

        let processed = book.place_order(&cmd);
        assert_eq!(processed.status(), Status::Placed);
        assert_eq!(book.total_order_count(), 1);
        assert_eq!(book.best_bid(), Some((50, 100)));
        assert_eq!(book.best_ask(), None);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_place_single_ask_gtc() {
        let mut book = create_test_orderbook();
        let cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            1,
            100,
            1001,
            1,
            55,
            100,
            Side::Ask,
            TimeInForce::Gtc,
        );

        let processed = book.place_order(&cmd);
        assert_eq!(processed.status(), Status::Placed);
        assert_eq!(book.total_order_count(), 1);
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), Some((55, 100)));
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_multiple_orders_same_price_level() {
        let mut book = create_test_orderbook();

        // Add first bid
        let cmd1 = create_order_command(
            OrderCommandType::PlaceOrder,
            1,
            100,
            1001,
            1,
            50,
            100,
            Side::Bid,
            TimeInForce::Gtc,
        );
        book.place_order(&cmd1);

        // Add second bid at same price
        let cmd2 = create_order_command(
            OrderCommandType::PlaceOrder,
            2,
            101,
            1002,
            1,
            50,
            200,
            Side::Bid,
            TimeInForce::Gtc,
        );
        book.place_order(&cmd2);

        assert_eq!(book.total_order_count(), 2);
        assert_eq!(book.best_bid(), Some((50, 300))); // Total volume
        assert_eq!(book.get_volume_at_price(50, Side::Bid), 300);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_multiple_price_levels() {
        let mut book = create_test_orderbook();

        // Add bids at different prices
        let bids = [(1, 50, 100), (2, 51, 150), (3, 49, 200)];

        for (id, price, size) in bids {
            let cmd = create_order_command(
                OrderCommandType::PlaceOrder,
                id,
                100 + id,
                1000 + id,
                1,
                price,
                size,
                Side::Bid,
                TimeInForce::Gtc,
            );
            book.place_order(&cmd);
        }

        // Best bid should be highest price
        assert_eq!(book.best_bid(), Some((51, 150)));
        assert_eq!(book.total_order_count(), 3);
        assert!(book.verify_state().is_ok());

        // Add asks at different prices
        let asks = [
            (4, 52, 80),
            (5, 53, 120),
            (6, 51, 90), // Note: 51 ask is invalid but tests the logic
        ];

        for (id, price, size) in asks {
            let cmd = create_order_command(
                OrderCommandType::PlaceOrder,
                id,
                100 + id,
                1000 + id,
                1,
                price,
                size,
                Side::Ask,
                TimeInForce::Gtc,
            );
            book.place_order(&cmd);
        }

        // Best ask should be lowest price among asks
        assert_eq!(book.best_ask(), Some((52, 80))); // After Match
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_simple_match_full_fill() {
        let mut book = create_test_orderbook();

        // Place a bid
        let bid_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            1,
            100,
            1001,
            1,
            50,
            100,
            Side::Bid,
            TimeInForce::Gtc,
        );
        book.place_order(&bid_cmd);

        // Place matching ask
        let ask_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            2,
            101,
            1002,
            1,
            50,
            100,
            Side::Ask,
            TimeInForce::Gtc,
        );
        let processed = book.place_order(&ask_cmd);

        assert_eq!(processed.status(), Status::Filled);
        assert_eq!(book.total_order_count(), 0); // Both orders should be filled
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);

        // Verify trade event
        verify_trade_events(&processed, &[(50, 100, 1001, true, true)]);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_partial_fill_scenario() {
        let mut book = create_test_orderbook();

        // Place a small bid
        let bid_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            1,
            100,
            1001,
            1,
            50,
            60,
            Side::Bid,
            TimeInForce::Gtc,
        );
        book.place_order(&bid_cmd);

        // Place larger matching ask
        let ask_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            2,
            101,
            1002,
            1,
            50,
            100,
            Side::Ask,
            TimeInForce::Gtc,
        );
        let processed = book.place_order(&ask_cmd);

        assert_eq!(processed.status(), Status::PartiallyFilled);
        assert_eq!(book.total_order_count(), 1); // Remaining ask should be in book
        assert_eq!(book.best_ask(), Some((50, 40))); // 100 - 60 = 40 remaining
        assert_eq!(book.best_bid(), None); // Bid should be completely filled

        // Verify trade event
        verify_trade_events(&processed, &[(50, 60, 1001, false, true)]);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_market_order_buy() {
        let mut book = create_test_orderbook();

        // Place asks at different levels
        let asks = [(1, 50, 100), (2, 51, 200), (3, 52, 150)];

        for (id, price, size) in asks {
            let cmd = create_order_command(
                OrderCommandType::PlaceOrder,
                id,
                100 + id,
                1000 + id,
                1,
                price,
                size,
                Side::Ask,
                TimeInForce::Gtc,
            );
            book.place_order(&cmd);
        }

        // Place market buy order
        let market_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            10,
            200,
            2000,
            1,
            u64::MAX,
            250,
            Side::Bid,
            TimeInForce::Gtc, // Market order
        );
        let processed = book.place_order(&market_cmd);

        assert_eq!(processed.status(), Status::Filled);
        // Should fill 100 at 50 and 150 at 51
        verify_trade_events(
            &processed,
            &[
                (50, 100, 1001, false, true),
                (51, 150, 1002, true, false), // Partially fills the 200 size ask
            ],
        );

        // Check remaining state
        assert_eq!(book.best_ask(), Some((51, 50))); // 200 - 150 = 50 remaining
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_market_order_sell() {
        let mut book = create_test_orderbook();

        // Place bids at different levels
        let bids = [(1, 52, 100), (2, 51, 200), (3, 50, 150)];

        for (id, price, size) in bids {
            let cmd = create_order_command(
                OrderCommandType::PlaceOrder,
                id,
                100 + id,
                1000 + id,
                1,
                price,
                size,
                Side::Bid,
                TimeInForce::Gtc,
            );
            book.place_order(&cmd);
        }

        // Place market sell order
        let market_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            10,
            200,
            2000,
            1,
            0,
            250,
            Side::Ask,
            TimeInForce::Gtc, // Market order
        );
        let processed = book.place_order(&market_cmd);

        assert_eq!(processed.status(), Status::Filled);
        // Should fill 100 at 52 and 150 at 51
        verify_trade_events(
            &processed,
            &[
                (52, 100, 1001, false, true),
                (51, 150, 1002, true, false), // Partially fills the 200 size bid
            ],
        );

        // Check remaining state
        assert_eq!(book.best_bid(), Some((51, 50))); // 200 - 150 = 50 remaining
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_fok_order_success() {
        let mut book = create_test_orderbook();

        // Place enough asks to fill FOK order
        let asks = [(1, 50, 100), (2, 51, 200)];

        for (id, price, size) in asks {
            let cmd = create_order_command(
                OrderCommandType::PlaceOrder,
                id,
                100 + id,
                1000 + id,
                1,
                price,
                size,
                Side::Ask,
                TimeInForce::Gtc,
            );
            book.place_order(&cmd);
        }

        // Place FOK buy order that can be completely filled
        let fok_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            10,
            200,
            2000,
            1,
            51,
            250,
            Side::Bid,
            TimeInForce::Fok,
        );
        let processed = book.place_order(&fok_cmd);

        assert_eq!(processed.status(), Status::Filled);
        verify_trade_events(
            &processed,
            &[(50, 100, 1001, false, true), (51, 150, 1002, true, false)],
        );
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_fok_order_cancelled() {
        let mut book = create_test_orderbook();

        // Place insufficient asks
        let ask_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            1,
            100,
            1001,
            1,
            50,
            100,
            Side::Ask,
            TimeInForce::Gtc,
        );
        book.place_order(&ask_cmd);

        // Place FOK buy order that cannot be completely filled
        let fok_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            10,
            200,
            2000,
            1,
            50,
            200,
            Side::Bid,
            TimeInForce::Fok,
        );
        let processed = book.place_order(&fok_cmd);

        assert_eq!(processed.status(), Status::Cancelled);
        assert_eq!(book.total_order_count(), 1); // Original ask should remain
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_ioc_order_full_fill() {
        let mut book = create_test_orderbook();

        // Place ask
        let ask_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            1,
            100,
            1001,
            1,
            50,
            100,
            Side::Ask,
            TimeInForce::Gtc,
        );
        book.place_order(&ask_cmd);

        // Place IOC buy order that matches exactly
        let ioc_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            10,
            200,
            2000,
            1,
            50,
            100,
            Side::Bid,
            TimeInForce::Ioc,
        );
        let processed = book.place_order(&ioc_cmd);

        assert_eq!(processed.status(), Status::Filled);
        assert_eq!(book.total_order_count(), 0);
        verify_trade_events(&processed, &[(50, 100, 1001, true, true)]);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_ioc_order_partial_fill() {
        let mut book = create_test_orderbook();

        // Place small ask
        let ask_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            1,
            100,
            1001,
            1,
            50,
            60,
            Side::Ask,
            TimeInForce::Gtc,
        );
        book.place_order(&ask_cmd);

        // Place IOC buy order larger than available
        let ioc_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            10,
            200,
            2000,
            1,
            50,
            100,
            Side::Bid,
            TimeInForce::Ioc,
        );
        let processed = book.place_order(&ioc_cmd);

        assert_eq!(processed.status(), Status::PartiallyFilled);
        assert_eq!(book.total_order_count(), 0); // Ask filled, IOC remainder cancelled
        verify_trade_events(&processed, &[(50, 60, 1001, false, true)]);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_ioc_order_no_fill() {
        let mut book = create_test_orderbook();

        // Place ask at higher price
        let ask_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            1,
            100,
            1001,
            1,
            55,
            100,
            Side::Ask,
            TimeInForce::Gtc,
        );
        book.place_order(&ask_cmd);

        // Place IOC buy order at lower price
        let ioc_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            10,
            200,
            2000,
            1,
            50,
            100,
            Side::Bid,
            TimeInForce::Ioc,
        );
        let processed = book.place_order(&ioc_cmd);

        assert_eq!(processed.status(), Status::Cancelled);
        assert_eq!(book.total_order_count(), 1); // Original ask remains
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_order_cancellation_success() {
        let mut book = create_test_orderbook();

        // Place order
        let place_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            1,
            100,
            1001,
            1,
            50,
            100,
            Side::Bid,
            TimeInForce::Gtc,
        );
        book.place_order(&place_cmd);

        // Cancel the order
        let cancel_cmd = create_order_command(
            OrderCommandType::CancelOrder,
            1,
            101,
            1001,
            1,
            50,
            0,
            Side::Bid,
            TimeInForce::Gtc, // size and tif irrelevant for cancel
        );
        let processed = book.cancel_order(&cancel_cmd);

        assert_eq!(processed.status(), Status::Cancelled);
        assert_eq!(book.total_order_count(), 0);
        assert_eq!(book.best_bid(), None);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_order_cancellation_nonexistent() {
        let mut book = create_test_orderbook();

        // Try to cancel non-existent order
        let cancel_cmd = create_order_command(
            OrderCommandType::CancelOrder,
            999,
            101,
            1001,
            1,
            50,
            0,
            Side::Bid,
            TimeInForce::Gtc,
        );
        let processed = book.cancel_order(&cancel_cmd);

        assert_eq!(processed.status(), Status::Rejected); // Should remain rejected since order doesn't exist
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_fifo_order_within_price_level() {
        let mut book = create_test_orderbook();

        // Place multiple bids at same price
        for i in 1..=3 {
            let cmd = create_order_command(
                OrderCommandType::PlaceOrder,
                i,
                100 + i,
                1000 + i,
                1,
                50,
                100,
                Side::Bid,
                TimeInForce::Gtc,
            );
            book.place_order(&cmd);
        }

        // Place ask that partially matches
        let ask_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            10,
            200,
            2000,
            1,
            50,
            150,
            Side::Ask,
            TimeInForce::Gtc, // Matches 1.5 bids
        );
        let processed = book.place_order(&ask_cmd);

        assert_eq!(processed.status(), Status::Filled);

        // Should match first two orders (FIFO)
        verify_trade_events(
            &processed,
            &[
                (50, 100, 1001, false, true), // First bid completely filled
                (50, 50, 1002, true, false),  // Second bid partially filled
            ],
        );

        // Third bid and remainder of second bid should remain
        assert_eq!(book.total_order_count(), 2);
        assert_eq!(book.get_volume_at_price(50, Side::Bid), 150); // 50 + 100 remaining
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_price_time_priority() {
        let mut book = create_test_orderbook();

        // Place bids at different prices and times
        let bids = [
            (1, 49, 100, 100), // Lower price, earlier time
            (2, 51, 100, 101), // Higher price, later time
            (3, 50, 100, 102), // Middle price, latest time
        ];

        for (id, price, size, timestamp) in bids {
            let cmd = create_order_command(
                OrderCommandType::PlaceOrder,
                id,
                timestamp,
                1000 + id,
                1,
                price,
                size,
                Side::Bid,
                TimeInForce::Gtc,
            );
            book.place_order(&cmd);
        }

        // Place ask that matches multiple levels
        let ask_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            10,
            200,
            2000,
            1,
            49,
            250,
            Side::Ask,
            TimeInForce::Gtc,
        );
        let processed = book.place_order(&ask_cmd);

        // Should match in price priority: 51, then 50, then 49
        verify_trade_events(
            &processed,
            &[
                (51, 100, 1002, false, true), // Best price first
                (50, 100, 1003, false, true), // Second best price
                (49, 50, 1001, true, false),  // Lowest price, partial fill
            ],
        );

        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_multiple_matches_across_levels() {
        let mut book = create_test_orderbook();

        // Create a deep book with multiple price levels
        let asks = [(1, 50, 100), (2, 51, 200), (3, 52, 150), (4, 53, 300)];

        for (id, price, size) in asks {
            let cmd = create_order_command(
                OrderCommandType::PlaceOrder,
                id,
                100 + id,
                1000 + id,
                1,
                price,
                size,
                Side::Ask,
                TimeInForce::Gtc,
            );
            book.place_order(&cmd);
        }

        // Large market buy that crosses multiple levels
        let buy_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            10,
            200,
            2000,
            1,
            u64::MAX,
            500,
            Side::Bid,
            TimeInForce::Gtc,
        );
        let processed = book.place_order(&buy_cmd);

        assert_eq!(processed.status(), Status::Filled);

        // Should match: 100@50, 200@51, 150@52, 50@53
        verify_trade_events(
            &processed,
            &[
                (50, 100, 1001, false, true),
                (51, 200, 1002, false, true),
                (52, 150, 1003, false, true),
                (53, 50, 1004, true, false),
            ],
        );

        // Remaining: 250 @ 53
        assert_eq!(book.best_ask(), Some((53, 250)));
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_self_trade_prevention() {
        let mut book = create_test_orderbook();

        // Place bid from user 1001
        let bid_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            1,
            100,
            1001,
            1,
            50,
            100,
            Side::Bid,
            TimeInForce::Gtc,
        );
        book.place_order(&bid_cmd);

        // Try to place ask from same user (this should still match in our implementation)
        let ask_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            2,
            101,
            1001,
            1,
            50,
            100,
            Side::Ask,
            TimeInForce::Gtc,
        );
        let processed = book.place_order(&ask_cmd);

        // In this implementation, self-trades are allowed
        assert_eq!(processed.status(), Status::Filled);
        verify_trade_events(&processed, &[(50, 100, 1001, true, true)]);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_empty_book_market_orders() {
        let mut book = create_test_orderbook();

        // Market buy on empty book, the order should cancell
        let market_buy = create_order_command(
            OrderCommandType::PlaceOrder,
            1,
            100,
            1001,
            1,
            u64::MAX,
            100,
            Side::Bid,
            TimeInForce::Ioc, // market orders are either IOC or FOK
        );
        let processed = book.place_order(&market_buy);

        assert_eq!(processed.status(), Status::Cancelled);

        // Market sell on empty book
        let market_sell = create_order_command(
            OrderCommandType::PlaceOrder,
            2,
            101,
            1002,
            1,
            0,
            100,
            Side::Ask,
            TimeInForce::Ioc,
        );
        let processed2 = book.place_order(&market_sell);

        // Should be placed as limit order at 0 price
        assert_eq!(processed2.status(), Status::Cancelled);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_order_book_spread() {
        let mut book = create_test_orderbook();

        // Place bid and ask with spread
        let bid_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            1,
            100,
            1001,
            1,
            48,
            100,
            Side::Bid,
            TimeInForce::Gtc,
        );
        book.place_order(&bid_cmd);

        let ask_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            2,
            101,
            1002,
            1,
            52,
            100,
            Side::Ask,
            TimeInForce::Gtc,
        );
        book.place_order(&ask_cmd);

        assert_eq!(book.best_bid(), Some((48, 100)));
        assert_eq!(book.best_ask(), Some((52, 100)));

        // Spread = 52 - 48 = 4
        let spread = book.best_ask().unwrap().0 - book.best_bid().unwrap().0;
        assert_eq!(spread, 4);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_large_order_multiple_levels() {
        let mut book = create_test_orderbook();

        // Create deep ask side
        let asks = [
            (1, 100, 50),  // 50 @ 100
            (2, 100, 75),  // 75 @ 100 (same price, different order)
            (3, 101, 100), // 100 @ 101
            (4, 102, 200), // 200 @ 102
            (5, 103, 150), // 150 @ 103
        ];

        for (id, price, size) in asks {
            let cmd = create_order_command(
                OrderCommandType::PlaceOrder,
                id,
                100 + id,
                1000 + id,
                1,
                price,
                size,
                Side::Ask,
                TimeInForce::Gtc,
            );
            book.place_order(&cmd);
        }

        // Large market buy
        let large_buy = create_order_command(
            OrderCommandType::PlaceOrder,
            10,
            200,
            2000,
            1,
            u64::MAX,
            400,
            Side::Bid,
            TimeInForce::Gtc,
        );
        let processed = book.place_order(&large_buy);

        assert_eq!(processed.status(), Status::Filled);

        // Should match: 50@100, 75@100, 100@101, 175@102 (partial)
        verify_trade_events(
            &processed,
            &[
                (100, 50, 1001, false, true),  // First order at 100
                (100, 75, 1002, false, true),  // Second order at 100
                (101, 100, 1003, false, true), // All of 101 level
                (102, 175, 1004, true, false), // Partial fill of 102 level
            ],
        );

        // Remaining: 25@102, 150@103
        assert_eq!(book.best_ask(), Some((102, 25)));
        assert_eq!(book.get_volume_at_price(103, Side::Ask), 150);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_cancel_from_multi_order_level() {
        let mut book = create_test_orderbook();

        // Place multiple orders at same price
        for i in 1..=3 {
            let cmd = create_order_command(
                OrderCommandType::PlaceOrder,
                i,
                100 + i,
                1000 + i,
                1,
                50,
                100,
                Side::Bid,
                TimeInForce::Gtc,
            );
            book.place_order(&cmd);
        }

        assert_eq!(book.get_volume_at_price(50, Side::Bid), 300);
        assert_eq!(book.total_order_count(), 3);

        // Cancel middle order
        let cancel_cmd = create_order_command(
            OrderCommandType::CancelOrder,
            2,
            200,
            1002,
            1,
            50,
            0,
            Side::Bid,
            TimeInForce::Gtc,
        );
        let processed = book.cancel_order(&cancel_cmd);

        assert_eq!(processed.status(), Status::Cancelled);
        assert_eq!(book.get_volume_at_price(50, Side::Bid), 200); // 300 - 100
        assert_eq!(book.total_order_count(), 2);

        // Verify remaining orders are correct
        let remaining_orders: Vec<u64> = book
            .bids
            .iter()
            .flat_map(|(_, level)| level.orders.iter().map(|o| o.order_id))
            .collect();
        assert_eq!(remaining_orders, vec![1, 3]);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_cancel_last_order_removes_level() {
        let mut book = create_test_orderbook();

        // Place single order
        let place_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            1,
            100,
            1001,
            1,
            50,
            100,
            Side::Bid,
            TimeInForce::Gtc,
        );
        book.place_order(&place_cmd);

        assert_eq!(book.best_bid(), Some((50, 100)));

        // Cancel the order
        let cancel_cmd = create_order_command(
            OrderCommandType::CancelOrder,
            1,
            101,
            1001,
            1,
            50,
            0,
            Side::Bid,
            TimeInForce::Gtc,
        );
        book.cancel_order(&cancel_cmd);

        // Price level should be removed
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.get_volume_at_price(50, Side::Bid), 0);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_complex_matching_scenario() {
        let mut book = create_test_orderbook();

        // Build complex book state
        // Bids: 100@49, 200@48, 150@47
        let bids = [(1, 49, 100), (2, 48, 200), (3, 47, 150)];
        for (id, price, size) in bids {
            let cmd = create_order_command(
                OrderCommandType::PlaceOrder,
                id,
                100 + id,
                1000 + id,
                1,
                price,
                size,
                Side::Bid,
                TimeInForce::Gtc,
            );
            book.place_order(&cmd);
        }

        // Asks: 150@51, 250@52, 100@53
        let asks = [(4, 51, 150), (5, 52, 250), (6, 53, 100)];
        for (id, price, size) in asks {
            let cmd = create_order_command(
                OrderCommandType::PlaceOrder,
                id,
                100 + id,
                1000 + id,
                1,
                price,
                size,
                Side::Ask,
                TimeInForce::Gtc,
            );
            book.place_order(&cmd);
        }

        // Now place crossing order - sell at 48 (crosses multiple bid levels)
        let cross_sell = create_order_command(
            OrderCommandType::PlaceOrder,
            10,
            200,
            2000,
            1,
            48,
            220,
            Side::Ask,
            TimeInForce::Gtc,
        );
        let processed = book.place_order(&cross_sell);

        assert_eq!(processed.status(), Status::Filled);

        // Should match: 100@49 (full), 120@48 (partial)
        verify_trade_events(
            &processed,
            &[
                (49, 100, 1001, false, true), // Full match at 49
                (48, 120, 1002, true, false), // Partial match at 48
            ],
        );

        // Check final state
        assert_eq!(book.best_bid(), Some((48, 80))); // 200 - 120 = 80 remaining at 48
        assert_eq!(book.best_ask(), Some((51, 150))); // same as previous was fully filled
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_identical_timestamps() {
        let mut book = create_test_orderbook();

        // Place orders with identical timestamps
        for i in 1..=3 {
            let cmd = create_order_command(
                OrderCommandType::PlaceOrder,
                i,
                100,
                1000 + i,
                1, // Same timestamp
                50,
                100,
                Side::Bid,
                TimeInForce::Gtc,
            );
            book.place_order(&cmd);
        }

        // Match partially to test FIFO with same timestamp
        let ask_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            10,
            200,
            2000,
            1,
            50,
            150,
            Side::Ask,
            TimeInForce::Gtc,
        );
        let processed = book.place_order(&ask_cmd);

        // Should still follow arrival order (FIFO)
        verify_trade_events(
            &processed,
            &[
                (50, 100, 1001, false, true), // First order
                (50, 50, 1002, true, false),  // Second order partial
            ],
        );
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_zero_size_order_handling() {
        let mut book = create_test_orderbook();

        // Try to place zero-size order
        let zero_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            1,
            100,
            1001,
            1,
            50,
            0,
            Side::Bid,
            TimeInForce::Gtc,
        );
        let processed = book.place_order(&zero_cmd);

        // Should be placed (implementation doesn't validate size)
        assert_eq!(processed.status(), Status::Placed);
        assert_eq!(book.get_volume_at_price(50, Side::Bid), 0);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_extreme_price_values() {
        let mut book = create_test_orderbook();

        // Test with extreme prices
        let extreme_bid = create_order_command(
            OrderCommandType::PlaceOrder,
            1,
            100,
            1001,
            1,
            u64::MAX - 1,
            100,
            Side::Bid,
            TimeInForce::Gtc,
        );
        book.place_order(&extreme_bid);

        let extreme_ask = create_order_command(
            OrderCommandType::PlaceOrder,
            2,
            101,
            1002,
            1,
            1,
            100,
            Side::Ask,
            TimeInForce::Gtc,
        );
        assert_eq!(book.best_bid(), Some((u64::MAX - 1, 100)));

        book.place_order(&extreme_ask);

        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_different_market_ids() {
        let mut book = create_test_orderbook();

        // All orders should target the same market_id for this book instance
        let cmd1 = create_order_command(
            OrderCommandType::PlaceOrder,
            1,
            100,
            1001,
            1,
            50,
            100,
            Side::Bid,
            TimeInForce::Gtc,
        );
        book.place_order(&cmd1);

        // This would normally be rejected by a market router, but our book doesn't validate
        let cmd2 = create_order_command(
            OrderCommandType::PlaceOrder,
            2,
            101,
            1002,
            999, // Different market_id
            50,
            100,
            Side::Ask,
            TimeInForce::Gtc,
        );
        let processed = book.place_order(&cmd2);

        // Book doesn't validate market_id, so it processes normally
        assert_eq!(processed.status(), Status::Filled);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_order_state_after_partial_cancellation() {
        let mut book = create_test_orderbook();

        // Place orders at same level
        for i in 1..=5 {
            let cmd = create_order_command(
                OrderCommandType::PlaceOrder,
                i,
                100 + i,
                1000 + i,
                1,
                50,
                100,
                Side::Ask,
                TimeInForce::Gtc,
            );
            book.place_order(&cmd);
        }

        // Cancel every other order
        for i in [2, 4] {
            let cancel_cmd = create_order_command(
                OrderCommandType::CancelOrder,
                i,
                200 + i,
                1000 + i,
                1,
                50,
                0,
                Side::Ask,
                TimeInForce::Gtc,
            );
            book.cancel_order(&cancel_cmd);
        }

        assert_eq!(book.get_volume_at_price(50, Side::Ask), 300); // 5*100 - 2*100
        assert_eq!(book.total_order_count(), 3);

        // Verify correct orders remain
        let remaining_orders: Vec<u64> = book
            .asks
            .iter()
            .flat_map(|(_, level)| level.orders.iter().map(|o| o.order_id))
            .collect();
        assert_eq!(remaining_orders, vec![1, 3, 5]);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_mixed_time_in_force_orders() {
        let mut book = create_test_orderbook();

        // Place GTC orders first
        for i in 1..=3 {
            let cmd = create_order_command(
                OrderCommandType::PlaceOrder,
                i,
                100 + i,
                1000 + i,
                1,
                50,
                100,
                Side::Ask,
                TimeInForce::Gtc,
            );
            book.place_order(&cmd);
        }

        // IOC order that fully matches
        let ioc_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            10,
            200,
            2000,
            1,
            50,
            250,
            Side::Bid,
            TimeInForce::Ioc,
        );
        let processed_ioc = book.place_order(&ioc_cmd);

        assert_eq!(processed_ioc.status(), Status::Filled);
        assert_eq!(book.get_volume_at_price(50, Side::Ask), 50); // 300 - 250 = 50

        // FOK order that can be filled
        let fok_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            11,
            201,
            2001,
            1,
            50,
            50,
            Side::Bid,
            TimeInForce::Fok,
        );
        let processed_fok = book.place_order(&fok_cmd);

        assert_eq!(processed_fok.status(), Status::Filled);
        assert_eq!(book.get_volume_at_price(50, Side::Ask), 0);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_order_lookup_after_operations() {
        let mut book = create_test_orderbook();

        // Place orders
        for i in 1..=3 {
            let cmd = create_order_command(
                OrderCommandType::PlaceOrder,
                i,
                100 + i,
                1000 + i,
                1,
                50,
                100,
                Side::Bid,
                TimeInForce::Gtc,
            );
            book.place_order(&cmd);
        }

        // Verify orders can be found
        for i in 1..=3 {
            let order = book.get_order(i);
            assert!(order.is_some());
            assert_eq!(order.unwrap().order_id, i);
            assert_eq!(order.unwrap().user_id, 1000 + i);
        }

        // Cancel one order
        let cancel_cmd = create_order_command(
            OrderCommandType::CancelOrder,
            2,
            200,
            1002,
            1,
            50,
            0,
            Side::Bid,
            TimeInForce::Gtc,
        );
        book.cancel_order(&cancel_cmd);

        // Verify lookup after cancellation
        assert!(book.get_order(1).is_some());
        assert!(book.get_order(2).is_none()); // Should be removed
        assert!(book.get_order(3).is_some());
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_book_consistency_after_fills() {
        let mut book = create_test_orderbook();

        // Build book with multiple levels
        let orders = [
            (1, Side::Ask, 51, 100),
            (2, Side::Ask, 52, 200),
            (3, Side::Bid, 49, 150),
            (4, Side::Bid, 48, 300),
        ];

        for (id, side, price, size) in orders {
            let cmd = create_order_command(
                OrderCommandType::PlaceOrder,
                id,
                100 + id,
                1000 + id,
                1,
                price,
                size,
                side,
                TimeInForce::Gtc,
            );
            book.place_order(&cmd);
        }

        // Execute crossing trade
        let cross_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            10,
            200,
            2000,
            1,
            u64::MAX,
            180,
            Side::Bid,
            TimeInForce::Gtc, // Market buy
        );
        let processed = book.place_order(&cross_cmd);

        assert_eq!(processed.status(), Status::Filled);

        // Verify consistency
        assert!(book.verify_state().is_ok());

        // Check expected final state
        assert_eq!(book.best_ask(), Some((52, 120))); // 200 - 80 = 120 remaining
        assert_eq!(book.best_bid(), Some((49, 150))); // Unchanged

        // Verify hash maps are consistent
        assert!(!book.orders.contains_key(&1)); // Order 1 fully filled
        assert!(book.orders.contains_key(&2)); // Order 2 partially filled
    }

    // Performance and stress tests
    #[test]
    fn test_large_number_of_orders() {
        let mut book = create_test_orderbook();
        let num_orders = 1000;

        // Place many orders at different prices
        for i in 1..=num_orders {
            let cmd = create_order_command(
                OrderCommandType::PlaceOrder,
                i,
                100 + i,
                1000 + (i % 100),
                1,
                50 + (i % 20),
                100,
                Side::Bid,
                TimeInForce::Gtc,
            );
            let processed = book.place_order(&cmd);
            assert_eq!(processed.status(), Status::Placed);
        }

        assert_eq!(book.total_order_count(), (num_orders) as usize);
        assert!(book.verify_state().is_ok());

        // Cancel half of them
        for i in (1..=num_orders).step_by(2) {
            let side = Side::Bid;
            let cancel_cmd = create_order_command(
                OrderCommandType::CancelOrder,
                i,
                2000 + i,
                1000 + (i % 100),
                1,
                50 + (i % 20),
                0,
                side,
                TimeInForce::Gtc,
            );
            book.cancel_order(&cancel_cmd);
        }

        assert_eq!(book.total_order_count(), (num_orders / 2) as usize);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_all_order_command_types() {
        let mut book = create_test_orderbook();

        // Test PlaceOrder
        let place_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            1,
            100,
            1001,
            1,
            50,
            100,
            Side::Bid,
            TimeInForce::Gtc,
        );
        let processed = book.place_order(&place_cmd);
        assert_eq!(processed.status(), Status::Placed);

        // Test CancelOrder
        let cancel_cmd = OrderCommand {
            command: OrderCommandType::CancelOrder,
            order_id: 1,
            timestamp: 101,
            user_id: 1001,
            market_id: 1,
            price: 50, // Price needed for cancellation lookup
            size: 0,   // Irrelevant for cancel
            side: Side::Bid,
            time_in_force: TimeInForce::Gtc, // Irrelevant for cancel
            status: Status::Rejected,
            events: None,
        };
        let processed = book.cancel_order(&cancel_cmd);
        assert_eq!(processed.status(), Status::Cancelled);
        assert!(book.verify_state().is_ok());
    }

    #[test]
    fn test_all_time_in_force_types() {
        let mut book = create_test_orderbook();

        // Place asks for testing against
        let ask_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            1,
            100,
            1001,
            1,
            50,
            100,
            Side::Ask,
            TimeInForce::Gtc,
        );
        book.place_order(&ask_cmd);

        // Test GTC
        let gtc_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            2,
            101,
            1002,
            1,
            49,
            50,
            Side::Bid,
            TimeInForce::Gtc,
        );
        let processed = book.place_order(&gtc_cmd);
        assert_eq!(processed.status(), Status::Placed); // No match, gets placed

        // Test IOC - immediate match
        let ioc_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            3,
            102,
            1003,
            1,
            50,
            50,
            Side::Bid,
            TimeInForce::Ioc,
        );
        let processed = book.place_order(&ioc_cmd);
        assert_eq!(processed.status(), Status::Filled);

        // Test FOK - add more liquidity first
        let ask_cmd2 = create_order_command(
            OrderCommandType::PlaceOrder,
            4,
            103,
            1004,
            1,
            51,
            200,
            Side::Ask,
            TimeInForce::Gtc,
        );
        book.place_order(&ask_cmd2);

        let fok_cmd = create_order_command(
            OrderCommandType::PlaceOrder,
            5,
            104,
            1005,
            1,
            51,
            200,
            Side::Bid,
            TimeInForce::Fok,
        );
        let processed = book.place_order(&fok_cmd);
        assert_eq!(processed.status(), Status::Filled);

        assert!(book.verify_state().is_ok());
    }

    pub struct TestOrderBuilder {
        order_id: u64,
        user_id: u64,
        market_id: u32,
        price: u64,
        size: u64,
        side: Side,
        time_in_force: TimeInForce,
        timestamp: u64,
    }

    impl TestOrderBuilder {
        pub fn new() -> Self {
            Self {
                order_id: 1,
                user_id: 100,
                market_id: 1,
                price: 1000,
                size: 10,
                side: Side::Bid,
                time_in_force: TimeInForce::Gtc,
                timestamp: 1000,
            }
        }

        pub fn order_id(mut self, order_id: u64) -> Self {
            self.order_id = order_id;
            self
        }

        // pub fn user_id(mut self, user_id: u64) -> Self {
        //     self.user_id = user_id;
        //     self
        // }

        // pub fn market_id(mut self, market_id: u32) -> Self {
        //     self.market_id = market_id;
        //     self
        // }

        pub fn price(mut self, price: u64) -> Self {
            self.price = price;
            self
        }

        pub fn size(mut self, size: u64) -> Self {
            self.size = size;
            self
        }

        pub fn side(mut self, side: Side) -> Self {
            self.side = side;
            self
        }

        pub fn time_in_force(mut self, tif: TimeInForce) -> Self {
            self.time_in_force = tif;
            self
        }

        pub fn timestamp(mut self, timestamp: u64) -> Self {
            self.timestamp = timestamp;
            self
        }

        pub fn build_place_order(self) -> OrderCommand {
            OrderCommand {
                command: OrderCommandType::PlaceOrder,
                order_id: self.order_id,
                timestamp: self.timestamp,
                user_id: self.user_id,
                market_id: self.market_id,
                price: self.price,
                size: self.size,
                side: self.side,
                time_in_force: self.time_in_force,
                status: Status::Rejected,
                events: None,
            }
        }

        pub fn build_cancel_order(self) -> OrderCommand {
            OrderCommand {
                command: OrderCommandType::CancelOrder,
                order_id: self.order_id,
                timestamp: self.timestamp,
                user_id: self.user_id,
                market_id: self.market_id,
                price: self.price,                 // Ignored for cancel
                size: self.size,                   // Ignored for cancel
                side: self.side,                   // Ignored for cancel
                time_in_force: self.time_in_force, // Ignored for cancel
                status: Status::Rejected,
                events: None,
            }
        }

        pub fn market_buy(mut self) -> Self {
            self.side = Side::Bid;
            self.price = u64::MAX; // Market buy convention
            self
        }

        pub fn market_sell(mut self) -> Self {
            self.side = Side::Ask;
            self.price = 0; // Market sell convention
            self
        }
    }

    /// Test helper to check order book state
    pub fn assert_order_book_state<Ask: BookSide, Bid: BookSide>(
        book: &OrderBook<Ask, Bid>,
        expected_bids: &[(u64, u64)], // (price, total_volume())
        expected_asks: &[(u64, u64)], // (price, total_volume())
    ) {
        // Check bid levels
        let bid_levels: Vec<(u64, u64)> = book
            .bids
            .iter()
            .map(|(price, level)| (price, level.total_volume))
            .collect();
        assert_eq!(bid_levels, expected_bids, "Bid levels don't match");

        // Check ask levels
        let ask_levels: Vec<(u64, u64)> = book
            .asks
            .iter()
            .map(|(price, level)| (price, level.total_volume))
            .collect();
        assert_eq!(ask_levels, expected_asks, "Ask levels don't match");
    }

    /// Test helper to create a test order book
    pub fn create_test_order_book() -> OrderBook<BTreeAskSide, BTreeBidSide> {
        OrderBook::new(BTreeBidSide::new(), BTreeAskSide::new())
    }

    /// Counter for generating unique order IDs and timestamps
    pub struct TestCounter {
        order_id: u64,
        timestamp: u64,
    }

    impl TestCounter {
        pub fn new() -> Self {
            Self {
                order_id: 1,
                timestamp: 1000,
            }
        }

        pub fn next_order_id(&mut self) -> u64 {
            let id = self.order_id;
            self.order_id += 1;
            id
        }

        pub fn next_timestamp(&mut self) -> u64 {
            let ts = self.timestamp;
            self.timestamp += 1;
            ts
        }
    }

    #[test]
    fn test_place_simple_bid_order() {
        let mut book = create_test_order_book();
        let mut counter = TestCounter::new();

        let order = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Bid)
            .price(1000)
            .size(100)
            .build_place_order();

        let result = book.place_order(&order);

        assert_eq!(result.status(), Status::Placed);
        assert_eq!(result.order_id(), order.order_id);
        assert_eq!(result.market_id(), order.market_id);
        assert_eq!(result.side(), order.side);

        // Check order book state
        assert_order_book_state(&book, &[(1000, 100)], &[]);
    }

    #[test]
    fn test_place_simple_ask_order() {
        let mut book = create_test_order_book();
        let mut counter = TestCounter::new();

        let order = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Ask)
            .price(1100)
            .size(50)
            .build_place_order();

        let result = book.place_order(&order);

        assert_eq!(result.status(), Status::Placed);
        assert_order_book_state(&book, &[], &[(1100, 50)]);
    }

    #[test]
    fn test_simple_trade_execution() {
        let mut book = create_test_order_book();
        let mut counter = TestCounter::new();

        // Place a bid order first
        let bid_order = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Bid)
            .price(1000)
            .size(100)
            .build_place_order();

        let bid_result = book.place_order(&bid_order);
        assert_eq!(bid_result.status(), Status::Placed);

        // Place an ask order that should match
        let ask_order = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Ask)
            .price(1000)
            .size(50)
            .build_place_order();

        let ask_result = book.place_order(&ask_order);
        assert_eq!(ask_result.status(), Status::Filled);

        // Check that bid order is partially filled and remaining in the book
        assert_order_book_state(&book, &[(1000, 50)], &[]);
    }

    #[test]
    fn test_complete_fill() {
        let mut book = create_test_order_book();
        let mut counter = TestCounter::new();

        // Place a bid order
        let bid_order = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Bid)
            .price(1000)
            .size(100)
            .build_place_order();

        book.place_order(&bid_order);

        // Place an ask order that exactly matches
        let ask_order = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Ask)
            .price(1000)
            .size(100)
            .build_place_order();

        let ask_result = book.place_order(&ask_order);
        assert_eq!(ask_result.status(), Status::Filled);

        // Order book should be empty after complete fill
        assert_order_book_state(&book, &[], &[]);
    }

    #[test]
    fn test_partial_fill_with_remainder() {
        let mut book = create_test_order_book();
        let mut counter = TestCounter::new();

        // Place a small bid order
        let bid_order = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Bid)
            .price(1000)
            .size(50)
            .build_place_order();

        book.place_order(&bid_order);

        // Place a larger ask order
        let ask_order = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Ask)
            .price(1000)
            .size(100)
            .build_place_order();

        let ask_result = book.place_order(&ask_order);
        assert_eq!(ask_result.status(), Status::PartiallyFilled);

        // Check that the remainder is placed in the book
        assert_order_book_state(&book, &[], &[(1000, 50)]);
    }

    #[test]
    fn test_price_priority() {
        let mut book = create_test_order_book();
        let mut counter = TestCounter::new();

        // Place bids at different prices
        let bid1 = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Bid)
            .price(1000)
            .size(100)
            .build_place_order();

        let bid2 = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Bid)
            .price(1100)
            .size(100)
            .build_place_order();

        book.place_order(&bid1);
        book.place_order(&bid2);

        // Place an ask that should match with higher priced bid first
        let ask_order = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Ask)
            .price(1000)
            .size(50)
            .build_place_order();

        let ask_result = book.place_order(&ask_order);
        assert_eq!(ask_result.status(), Status::Filled);

        // The bid at 1100 should be partially filled, bid at 1000 should remain untouched
        assert_order_book_state(&book, &[(1100, 50), (1000, 100)], &[]);
    }

    #[test]
    fn test_time_priority_fifo() {
        let mut book = create_test_order_book();
        let mut counter = TestCounter::new();

        // Place two bids at the same price but different times
        let bid1 = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Bid)
            .price(1000)
            .size(50)
            .build_place_order();

        let bid2 = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Bid)
            .price(1000)
            .size(50)
            .build_place_order();

        book.place_order(&bid1);
        book.place_order(&bid2);

        // Place an ask that matches partially
        let ask_order = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Ask)
            .price(1000)
            .size(25)
            .build_place_order();

        book.place_order(&ask_order);

        // Should have 75 remaining volume at 1000 (25 from bid1, 50 from bid2)
        assert_order_book_state(&book, &[(1000, 75)], &[]);
    }

    #[test]
    fn test_ioc_order_complete_fill() {
        let mut book = create_test_order_book();
        let mut counter = TestCounter::new();

        // Place a bid order
        let bid_order = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Bid)
            .price(1000)
            .size(100)
            .build_place_order();

        book.place_order(&bid_order);

        // Place IOC ask order that can be completely filled
        let ioc_ask = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Ask)
            .price(1000)
            .size(50)
            .time_in_force(TimeInForce::Ioc)
            .build_place_order();

        let result = book.place_order(&ioc_ask);
        assert_eq!(result.status(), Status::Filled);
        assert_order_book_state(&book, &[(1000, 50)], &[]);
    }

    #[test]
    fn test_ioc_order_partial_fill_check() {
        let mut book = create_test_order_book();
        let mut counter = TestCounter::new();

        // Place a small bid order
        let bid_order = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Bid)
            .price(1000)
            .size(30)
            .build_place_order();

        book.place_order(&bid_order);

        // Place IOC ask order that can only be partially filled
        let ioc_ask = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Ask)
            .price(1000)
            .size(50)
            .time_in_force(TimeInForce::Ioc)
            .build_place_order();

        let result = book.place_order(&ioc_ask);
        assert_eq!(result.status(), Status::PartiallyFilled);
        assert_order_book_state(&book, &[], &[]); // No remainder should be placed
    }

    #[test]
    fn test_ioc_order_no_fill_check() {
        let mut book = create_test_order_book();
        let mut counter = TestCounter::new();

        // No opposing orders in the book

        // Place IOC ask order that cannot be filled
        let ioc_ask = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Ask)
            .price(1000)
            .size(50)
            .time_in_force(TimeInForce::Ioc)
            .build_place_order();

        let result = book.place_order(&ioc_ask);
        assert_eq!(result.status(), Status::Cancelled);
        assert_order_book_state(&book, &[], &[]);
    }

    #[test]
    fn test_fok_order_complete_fill() {
        let mut book = create_test_order_book();
        let mut counter = TestCounter::new();

        // Place enough liquidity
        let bid1 = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Bid)
            .price(1000)
            .size(60)
            .build_place_order();

        let bid2 = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Bid)
            .price(1000)
            .size(40)
            .build_place_order();

        book.place_order(&bid1);
        book.place_order(&bid2);

        // Place FOK ask order that can be completely filled
        let fok_ask = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Ask)
            .price(1000)
            .size(100)
            .time_in_force(TimeInForce::Fok)
            .build_place_order();

        let result = book.place_order(&fok_ask);
        assert_eq!(result.status(), Status::Filled);
        assert_order_book_state(&book, &[], &[]);
    }

    #[test]
    fn test_fok_order_insufficient_liquidity() {
        let mut book = create_test_order_book();
        let mut counter = TestCounter::new();

        // Place insufficient liquidity
        let bid_order = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Bid)
            .price(1000)
            .size(50)
            .build_place_order();

        book.place_order(&bid_order);

        // Place FOK ask order that cannot be completely filled
        let fok_ask = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Ask)
            .price(1000)
            .size(100)
            .time_in_force(TimeInForce::Fok)
            .build_place_order();

        let result = book.place_order(&fok_ask);
        assert_eq!(result.status(), Status::Cancelled);
        // Original bid should remain untouched
        assert_order_book_state(&book, &[(1000, 50)], &[]);
    }

    #[test]
    fn test_market_order_buy_check() {
        let mut book = create_test_order_book();
        let mut counter = TestCounter::new();

        // Place some ask orders
        let ask1 = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Ask)
            .price(1100)
            .size(50)
            .build_place_order();

        let ask2 = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Ask)
            .price(1200)
            .size(50)
            .build_place_order();

        book.place_order(&ask1);
        book.place_order(&ask2);

        // Place market buy order
        let market_buy = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .market_buy()
            .size(75)
            .build_place_order();

        let result = book.place_order(&market_buy);
        assert_eq!(result.status(), Status::Filled);

        // Should consume all of ask1 and part of ask2
        assert_order_book_state(&book, &[], &[(1200, 25)]);
    }

    #[test]
    fn test_market_order_sell_check() {
        let mut book = create_test_order_book();
        let mut counter = TestCounter::new();

        // Place some bid orders
        let bid1 = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Bid)
            .price(1000)
            .size(50)
            .build_place_order();

        let bid2 = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Bid)
            .price(900)
            .size(50)
            .build_place_order();

        book.place_order(&bid1);
        book.place_order(&bid2);

        // Place market sell order
        let market_sell = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .market_sell()
            .size(75)
            .build_place_order();

        let result = book.place_order(&market_sell);
        assert_eq!(result.status(), Status::Filled);

        // Should consume all of bid1 and part of bid2
        assert_order_book_state(&book, &[(900, 25)], &[]);
    }

    #[test]
    fn test_order_cancellation() {
        let mut book = create_test_order_book();
        let mut counter = TestCounter::new();

        // Place a bid order
        let bid_order = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .side(Side::Bid)
            .price(1000)
            .size(100)
            .build_place_order();

        let order_id = bid_order.order_id;
        book.place_order(&bid_order);

        // Cancel the order
        let cancel_order = TestOrderBuilder::new()
            .order_id(order_id)
            .timestamp(counter.next_timestamp())
            .side(Side::Bid)
            .price(1000)
            .build_cancel_order();

        let result = book.cancel_order(&cancel_order);
        assert_eq!(result.status(), Status::Cancelled);
        assert_order_book_state(&book, &[], &[]);
    }

    #[test]
    fn test_cancel_nonexistent_order() {
        let mut book = create_test_order_book();
        let mut counter = TestCounter::new();

        // Try to cancel an order that doesn't exist
        let cancel_order = TestOrderBuilder::new()
            .order_id(999)
            .timestamp(counter.next_timestamp())
            .side(Side::Bid)
            .price(1000)
            .build_cancel_order();

        let result = book.cancel_order(&cancel_order);
        assert_eq!(result.status(), Status::Rejected);
    }

    #[test]
    fn test_multiple_price_levels_check() {
        let mut book = create_test_order_book();
        let mut counter = TestCounter::new();

        // Build a more complex order book
        let orders = [
            (Side::Bid, 1000, 100),
            (Side::Bid, 999, 50),
            (Side::Bid, 998, 75),
            (Side::Ask, 1001, 80),
            (Side::Ask, 1002, 60),
            (Side::Ask, 1003, 90),
        ];

        for (side, price, size) in orders.iter() {
            let order = TestOrderBuilder::new()
                .order_id(counter.next_order_id())
                .timestamp(counter.next_timestamp())
                .side(*side)
                .price(*price)
                .size(*size)
                .build_place_order();

            book.place_order(&order);
        }

        assert_order_book_state(
            &book,
            &[(1000, 100), (999, 50), (998, 75)],
            &[(1001, 80), (1002, 60), (1003, 90)],
        );

        // Place a market sell that should hit multiple bid levels
        let market_sell = TestOrderBuilder::new()
            .order_id(counter.next_order_id())
            .timestamp(counter.next_timestamp())
            .market_sell()
            .size(125)
            .build_place_order();

        let result = book.place_order(&market_sell);
        assert_eq!(result.status(), Status::Filled);

        // Should consume all of 1000 bid and part of 999 bid
        assert_order_book_state(
            &book,
            &[(999, 25), (998, 75)],
            &[(1001, 80), (1002, 60), (1003, 90)],
        );
    }

    // A type alias for the concrete OrderBook implementation used in tests
    type TestVexOrderBook = OrderBook<BTreeAskSide, BTreeBidSide>;

    /// Creates a new, empty order book with BTree-backed sides.
    fn create_empty_book() -> TestVexOrderBook {
        OrderBook::new(BTreeBidSide::new(), BTreeAskSide::new())
    }

    /// A helper struct to manage state for tests, ensuring unique and incremental
    /// order IDs and timestamps, which is crucial for simulating a real-world scenario.
    struct TestHarness {
        order_id_counter: AtomicU64,
        timestamp_counter: AtomicU64,
        market_id: u32,
    }

    impl TestHarness {
        fn new(market_id: u32) -> Self {
            Self {
                order_id_counter: AtomicU64::new(1),
                timestamp_counter: AtomicU64::new(1),
                market_id,
            }
        }

        fn next_order_id(&self) -> u64 {
            self.order_id_counter.fetch_add(1, Ordering::SeqCst)
        }

        fn next_timestamp(&self) -> u64 {
            self.timestamp_counter.fetch_add(1, Ordering::SeqCst)
        }

        /// Builder for `PlaceOrder` commands.
        fn create_place_order_cmd(
            &self,
            user_id: u64,
            side: Side,
            price: u64,
            size: u64,
            tif: TimeInForce,
        ) -> OrderCommand {
            OrderCommand {
                command: OrderCommandType::PlaceOrder,
                order_id: self.next_order_id(),
                timestamp: self.next_timestamp(),
                user_id,
                market_id: self.market_id,
                price,
                size,
                side,
                time_in_force: tif,
                status: Status::Rejected,
                events: None,
            }
        }

        /// Builder for market `PlaceOrder` commands.
        fn create_market_order_cmd(&self, user_id: u64, side: Side, size: u64) -> OrderCommand {
            let price = if side == Side::Bid { u64::MAX } else { 0 };
            // Market orders are effectively IOC
            self.create_place_order_cmd(user_id, side, price, size, TimeInForce::Ioc)
        }

        /// Builder for `CancelOrder` commands.
        /// Note: The `OrderBook` implementation requires price and side for efficient lookup.
        fn create_cancel_order_cmd(&self, order_to_cancel: &OrderCommand) -> OrderCommand {
            OrderCommand {
                command: OrderCommandType::CancelOrder,
                order_id: order_to_cancel.order_id,
                timestamp: self.next_timestamp(),
                user_id: order_to_cancel.user_id,
                market_id: self.market_id,
                price: order_to_cancel.price,
                size: 0, // Not relevant for cancel
                side: order_to_cancel.side,
                time_in_force: TimeInForce::Gtc, // Not relevant
                status: Status::Rejected,
                events: None,
            }
        }
    }

    /// Helper to collect all trade events from a `ProcessedOrderCommand`'s linked list
    /// into a Vec for easier assertions.
    fn collect_trade_events(processed: OrderCommand) -> Vec<MatcherTradeEvent> {
        let mut events = Vec::new();
        let mut current_event = processed.events();
        while let Some(event_box) = current_event {
            current_event = event_box.next_event.as_deref();
            events.push(event_box.clone());
        }
        events
    }

    #[test]
    fn test_add_gtc_orders_to_empty_book_no_match() {
        let mut book = create_empty_book();
        let harness = TestHarness::new(1);

        let buy_cmd = harness.create_place_order_cmd(101, Side::Bid, 99_000, 10, TimeInForce::Gtc);
        let processed_buy = book.place_order(&buy_cmd);

        assert_eq!(processed_buy.status(), Status::Placed);
        assert!(processed_buy.events().is_none());
        book.assert_level_state(Side::Bid, 99_000, 10, 1);
        assert_eq!(book.orders.get(&buy_cmd.order_id), Some(&99_000));

        let sell_cmd = harness.create_place_order_cmd(202, Side::Ask, 101_000, 5, TimeInForce::Gtc);
        let processed_sell = book.place_order(&sell_cmd);

        assert_eq!(processed_sell.status(), Status::Placed);
        assert!(processed_sell.events().is_none());
        book.assert_level_state(Side::Ask, 101_000, 5, 1);
        assert_eq!(book.orders.get(&sell_cmd.order_id), Some(&101_000));
    }

    #[test]
    fn test_place_and_cancel_order_leaves_book_clean() {
        let mut book = create_empty_book();
        let harness = TestHarness::new(1);

        let bid_cmd = harness.create_place_order_cmd(101, Side::Bid, 99_000, 10, TimeInForce::Gtc);
        book.place_order(&bid_cmd);
        book.assert_level_state(Side::Bid, 99_000, 10, 1);

        let cancel_cmd = harness.create_cancel_order_cmd(&bid_cmd);
        let processed_cancel = book.cancel_order(&cancel_cmd);

        assert_eq!(processed_cancel.status(), Status::Cancelled);
        book.assert_level_state(Side::Bid, 99_000, 0, 0);
        assert!(!book.orders.contains_key(&bid_cmd.order_id));
    }

    #[test]
    fn test_cancel_nonexistent_order_is_rejected() {
        let mut book = create_empty_book();
        let harness = TestHarness::new(1);
        let bid_cmd = harness.create_place_order_cmd(101, Side::Bid, 99_000, 10, TimeInForce::Gtc);

        // Don't place the order, just create a cancel command for it
        let cancel_cmd = harness.create_cancel_order_cmd(&bid_cmd);
        let processed_cancel = book.cancel_order(&cancel_cmd);

        assert_eq!(processed_cancel.status(), Status::Rejected);
        assert!(book.bids.iter().next().is_none()); // Book remains empty
    }

    #[test]
    fn test_gtc_orders_full_match_clears_level() {
        let mut book = create_empty_book();
        let harness = TestHarness::new(1);

        let sell_cmd =
            harness.create_place_order_cmd(202, Side::Ask, 100_000, 10, TimeInForce::Gtc);
        book.place_order(&sell_cmd);

        let buy_cmd = harness.create_place_order_cmd(101, Side::Bid, 100_000, 10, TimeInForce::Gtc);
        let processed_buy = book.place_order(&buy_cmd);

        assert_eq!(processed_buy.status(), Status::Filled);

        let events = collect_trade_events(processed_buy);
        assert_eq!(events.len(), 1);
        let trade = &events[0];
        assert_eq!(trade.price, 100_000);
        assert_eq!(trade.size, 10);
        assert_eq!(trade.matched_order_id, sell_cmd.order_id);
        assert_eq!(trade.maker_user_id, 202);
        assert!(trade.active_order_completed);
        assert!(trade.matched_order_completed);

        book.assert_level_state(Side::Ask, 100_000, 0, 0);
        assert!(book.orders.is_empty());
    }

    #[test]
    fn test_gtc_taker_is_partially_filled_and_rests() {
        let mut book = create_empty_book();
        let harness = TestHarness::new(1);

        let sell_cmd =
            harness.create_place_order_cmd(202, Side::Ask, 100_000, 10, TimeInForce::Gtc);
        book.place_order(&sell_cmd);

        let buy_cmd = harness.create_place_order_cmd(101, Side::Bid, 100_000, 15, TimeInForce::Gtc);
        let processed_buy = book.place_order(&buy_cmd);

        assert_eq!(processed_buy.status(), Status::PartiallyFilled);

        let events = collect_trade_events(processed_buy);
        assert_eq!(events.len(), 1);
        assert!(!events[0].active_order_completed);
        assert!(events[0].matched_order_completed);

        book.assert_level_state(Side::Ask, 100_000, 0, 0);
        book.assert_level_state(Side::Bid, 100_000, 5, 1);
        assert_eq!(book.orders.get(&buy_cmd.order_id), Some(&100_000));
    }

    #[test]
    fn test_gtc_maker_is_partially_filled_and_remains() {
        let mut book = create_empty_book();
        let harness = TestHarness::new(1);

        let sell_cmd =
            harness.create_place_order_cmd(202, Side::Ask, 100_000, 20, TimeInForce::Gtc);
        book.place_order(&sell_cmd);

        let buy_cmd = harness.create_place_order_cmd(101, Side::Bid, 100_000, 12, TimeInForce::Gtc);
        let processed_buy = book.place_order(&buy_cmd);

        assert_eq!(processed_buy.status(), Status::Filled);

        let events = collect_trade_events(processed_buy);
        assert_eq!(events.len(), 1);
        assert!(events[0].active_order_completed);
        assert!(!events[0].matched_order_completed);

        book.assert_level_state(Side::Bid, 100_000, 0, 0);
        book.assert_level_state(Side::Ask, 100_000, 8, 1);
        assert_eq!(book.orders.get(&sell_cmd.order_id), Some(&100_000));
    }

    #[test]
    fn test_gtc_taker_sweeps_multiple_levels_and_rests() {
        let mut book = create_empty_book();
        let harness = TestHarness::new(1);

        let sell_cmd_1 =
            harness.create_place_order_cmd(202, Side::Ask, 100_000, 10, TimeInForce::Gtc);
        let sell_cmd_2 =
            harness.create_place_order_cmd(203, Side::Ask, 101_000, 10, TimeInForce::Gtc);
        book.place_order(&sell_cmd_1);
        book.place_order(&sell_cmd_2);

        let buy_cmd = harness.create_place_order_cmd(101, Side::Bid, 101_000, 25, TimeInForce::Gtc);
        let processed_buy = book.place_order(&buy_cmd);

        assert_eq!(processed_buy.status(), Status::PartiallyFilled);
        let events = collect_trade_events(processed_buy);
        assert_eq!(events.len(), 2);

        assert_eq!(events[0].price, 100_000);
        assert_eq!(events[1].price, 101_000);

        book.assert_level_state(Side::Ask, 100_000, 0, 0);
        book.assert_level_state(Side::Ask, 101_000, 0, 0);
        book.assert_level_state(Side::Bid, 101_000, 5, 1);
    }

    #[test]
    fn test_ioc_is_partially_filled_and_remainder_cancelled() {
        let mut book = create_empty_book();
        let harness = TestHarness::new(1);

        let sell_cmd =
            harness.create_place_order_cmd(202, Side::Ask, 100_000, 10, TimeInForce::Gtc);
        book.place_order(&sell_cmd);

        let ioc_buy_cmd =
            harness.create_place_order_cmd(101, Side::Bid, 100_000, 15, TimeInForce::Ioc);
        let processed_ioc = book.place_order(&ioc_buy_cmd);

        assert_eq!(processed_ioc.status(), Status::PartiallyFilled);

        let events = collect_trade_events(processed_ioc);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].size, 10);

        book.assert_level_state(Side::Ask, 100_000, 0, 0);
        book.assert_level_state(Side::Bid, 100_000, 0, 0);
        assert!(book.orders.is_empty());
    }

    #[test]
    fn test_ioc_with_no_match_is_fully_cancelled() {
        let mut book = create_empty_book();
        let harness = TestHarness::new(1);

        let sell_cmd =
            harness.create_place_order_cmd(202, Side::Ask, 100_000, 10, TimeInForce::Gtc);
        book.place_order(&sell_cmd);

        let ioc_buy_cmd =
            harness.create_place_order_cmd(101, Side::Bid, 99_000, 15, TimeInForce::Ioc);
        let processed_ioc = book.place_order(&ioc_buy_cmd);

        assert_eq!(processed_ioc.status(), Status::Cancelled);
        assert!(processed_ioc.events().is_none());

        book.assert_level_state(Side::Ask, 100_000, 10, 1);
    }

    #[test]
    fn test_fok_is_cancelled_if_liquidity_is_insufficient() {
        let mut book = create_empty_book();
        let harness = TestHarness::new(1);

        book.place_order(&harness.create_place_order_cmd(
            202,
            Side::Ask,
            100_000,
            10,
            TimeInForce::Gtc,
        ));
        book.place_order(&harness.create_place_order_cmd(
            203,
            Side::Ask,
            101_000,
            10,
            TimeInForce::Gtc,
        ));

        let fok_buy_cmd =
            harness.create_place_order_cmd(101, Side::Bid, 100_000, 15, TimeInForce::Fok);
        let processed_fok = book.place_order(&fok_buy_cmd);

        assert_eq!(processed_fok.status(), Status::Cancelled);
        assert!(processed_fok.events().is_none());

        // Verify book state is unchanged
        book.assert_level_state(Side::Ask, 100_000, 10, 1);
        book.assert_level_state(Side::Ask, 101_000, 10, 1);
    }

    #[test]
    fn test_fok_is_filled_if_liquidity_is_sufficient() {
        let mut book = create_empty_book();
        let harness = TestHarness::new(1);

        book.place_order(&harness.create_place_order_cmd(
            202,
            Side::Ask,
            99_000,
            5,
            TimeInForce::Gtc,
        ));
        book.place_order(&harness.create_place_order_cmd(
            203,
            Side::Ask,
            100_000,
            5,
            TimeInForce::Gtc,
        ));

        let fok_buy_cmd =
            harness.create_place_order_cmd(101, Side::Bid, 100_000, 10, TimeInForce::Fok);
        let processed_fok = book.place_order(&fok_buy_cmd);

        assert_eq!(processed_fok.status(), Status::Filled);
        assert_eq!(collect_trade_events(processed_fok).len(), 2);

        book.assert_level_state(Side::Ask, 99_000, 0, 0);
        book.assert_level_state(Side::Ask, 100_000, 0, 0);
        assert!(book.orders.is_empty());
    }

    #[test]
    fn test_fifo_priority_is_respected_at_same_price_level() {
        let mut book = create_empty_book();
        let harness = TestHarness::new(1);

        let sell_cmd_1 =
            harness.create_place_order_cmd(202, Side::Ask, 100_000, 5, TimeInForce::Gtc);
        let sell_cmd_2 =
            harness.create_place_order_cmd(203, Side::Ask, 100_000, 5, TimeInForce::Gtc);
        book.place_order(&sell_cmd_1);
        book.place_order(&sell_cmd_2);

        book.assert_level_state(Side::Ask, 100_000, 10, 2);

        let buy_cmd = harness.create_place_order_cmd(101, Side::Bid, 100_000, 5, TimeInForce::Gtc);
        let processed_buy = book.place_order(&buy_cmd);

        assert_eq!(processed_buy.status(), Status::Filled);

        let events = collect_trade_events(processed_buy);
        assert_eq!(events[0].matched_order_id, sell_cmd_1.order_id);

        book.assert_level_state(Side::Ask, 100_000, 5, 1);
        assert!(!book.orders.contains_key(&sell_cmd_1.order_id));
        assert!(book.orders.contains_key(&sell_cmd_2.order_id));
    }

    #[test]
    fn test_market_buy_sweeps_available_asks() {
        let mut book = create_empty_book();
        let harness = TestHarness::new(1);

        book.place_order(&harness.create_place_order_cmd(
            202,
            Side::Ask,
            99_000,
            5,
            TimeInForce::Gtc,
        ));
        book.place_order(&harness.create_place_order_cmd(
            203,
            Side::Ask,
            100_000,
            5,
            TimeInForce::Gtc,
        ));

        let market_buy = harness.create_market_order_cmd(101, Side::Bid, 8);
        let processed = book.place_order(&market_buy);

        assert_eq!(processed.status(), Status::Filled);
        let events = collect_trade_events(processed);
        assert_eq!(events.len(), 2);

        assert_eq!(events[0].price, 99_000);
        assert_eq!(events[0].size, 5);
        assert_eq!(events[1].price, 100_000);
        assert_eq!(events[1].size, 3);

        book.assert_level_state(Side::Ask, 99_000, 0, 0);
        book.assert_level_state(Side::Ask, 100_000, 2, 1);
    }

    #[test]
    fn test_market_order_on_empty_book_is_cancelled() {
        let mut book = create_empty_book();
        let harness = TestHarness::new(1);

        let market_buy = harness.create_market_order_cmd(101, Side::Bid, 10);
        let processed = book.place_order(&market_buy);

        assert_eq!(processed.status(), Status::Cancelled);
        assert!(processed.events().is_none());
    }

    #[test]
    fn test_self_trade_executes_successfully() {
        // Note: A production matching engine would typically prevent self-trades.
        // This test confirms the current behavior, which allows them.
        let mut book = create_empty_book();
        let harness = TestHarness::new(1);
        const SAME_USER_ID: u64 = 555;

        let sell_cmd =
            harness.create_place_order_cmd(SAME_USER_ID, Side::Ask, 100_000, 10, TimeInForce::Gtc);
        book.place_order(&sell_cmd);

        let buy_cmd =
            harness.create_place_order_cmd(SAME_USER_ID, Side::Bid, 100_000, 10, TimeInForce::Gtc);
        let processed = book.place_order(&buy_cmd);

        assert_eq!(processed.status(), Status::Filled);
        let events = collect_trade_events(processed);
        assert_eq!(events[0].maker_user_id, SAME_USER_ID);
        book.assert_level_state(Side::Ask, 100_000, 0, 0);
    }

    #[test]
    fn test_large_maker_order_is_filled_by_multiple_takers() {
        let mut book = create_empty_book();
        let harness = TestHarness::new(1);

        let large_sell =
            harness.create_place_order_cmd(202, Side::Ask, 100_000, 50, TimeInForce::Gtc);
        book.place_order(&large_sell);
        book.assert_level_state(Side::Ask, 100_000, 50, 1);

        // First taker buys 10
        let buy_1 = harness.create_place_order_cmd(101, Side::Bid, 100_000, 10, TimeInForce::Gtc);
        assert_eq!(book.place_order(&buy_1).status(), Status::Filled);
        book.assert_level_state(Side::Ask, 100_000, 40, 1);

        // Second taker buys 25
        let buy_2 = harness.create_place_order_cmd(102, Side::Bid, 100_000, 25, TimeInForce::Gtc);
        assert_eq!(book.place_order(&buy_2).status(), Status::Filled);
        book.assert_level_state(Side::Ask, 100_000, 15, 1);

        // Final taker buys the rest
        let buy_3 = harness.create_place_order_cmd(103, Side::Bid, 100_000, 15, TimeInForce::Gtc);
        let processed = book.place_order(&buy_3);
        assert_eq!(processed.status(), Status::Filled);
        let events = collect_trade_events(processed);
        assert!(events[0].matched_order_completed); // Maker order is now complete

        book.assert_level_state(Side::Ask, 100_000, 0, 0);
    }
}
