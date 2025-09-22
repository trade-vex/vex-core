pub mod engine;
pub mod utils;

use std::sync::Arc;

use common::CoreMarketSpecification;
use hashbrown::HashMap;
use processors::events::KafkaEventsHandler;
use processors::journaling::JournalingProcessor;

use crate::engine::{CoreEngine, OrderProducer};

/// Sets up the entire Exchange Core application with all processors.
///
/// This creates the core engine and adds symbols from the provided configuration
pub fn init_exchange(
    symbol_specs: HashMap<u32, CoreMarketSpecification>,
) -> (CoreEngine, OrderProducer) {
    // Initialize journaling processor for audit trail
    let journaling_processor = JournalingProcessor::new();

    // Create events handler for trade events
    let events_handler = Arc::new(KafkaEventsHandler::new("localhost:9093"));

    // Create the Exchange Core with sharded risk engines and matching engines
    // Symbols are automatically added to matching engines during initialization

    #[cfg(test)]
    {
        let test_handler = |cmd: &mut common::OrderCommand, _seq: i64, _end_of_batch: bool| {
            println!("Test handler received command: {:?}", cmd);
        };
        let (core_engine, producer, _) = CoreEngine::new(
            symbol_specs.clone(),
            journaling_processor,
            events_handler,
            test_handler,
        );
        return (core_engine, producer);
    }
    #[cfg(not(test))]
    {
        let (core_engine, producer, _) =
            CoreEngine::new(symbol_specs.clone(), journaling_processor, events_handler);
        return (core_engine, producer);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use common::PriceCache;
    use common::{CoreMarketSpecification, MarketType, OrderCommand, Side, Status};
    use common::{TimeInForce, UserBalance};
    use disruptor::Producer;
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    fn add_spec(market_id: u32, specs: &mut HashMap<u32, CoreMarketSpecification>) {
        specs.insert(
            market_id,
            CoreMarketSpecification::builder()
                .market_id(market_id)
                .market_type(MarketType::Spot)
                .maker_fee(10) // 0.1%
                .taker_fee(20) // 0.2%
                .slippage(5)
                .build()
                .unwrap(),
        );
    }

    // Helper function to reduce test boilerplate
    fn setup_test_env(
        specs: HashMap<u32, CoreMarketSpecification>,
    ) -> (
        OrderProducer,
        Arc<Vec<processors::risk_engine::RiskEngine>>,
        mpsc::Receiver<OrderCommand>,
    ) {
        let (tx, rx) = mpsc::channel::<OrderCommand>();
        let test_handler = move |cmd: &mut OrderCommand, _, _| {
            if cmd.status != Status::Processing {
                tx.send(cmd.clone()).unwrap();
            }
        };

        let (_core_engine, producer, risk_engines_opt) = CoreEngine::new(
            specs,
            JournalingProcessor::new(),
            Arc::new(KafkaEventsHandler::new("localhost:9093")),
            test_handler,
        );
        let risk_engines = risk_engines_opt.expect("Risk engines should be available in test mode");

        (producer, risk_engines, rx)
    }

    fn get_producer(
        test_handler: Option<impl FnMut(&mut OrderCommand, i64, bool) + Send + Sync + 'static>,
    ) -> OrderProducer {
        let mut specs = HashMap::new();
        add_spec(1, &mut specs);
        add_spec(2, &mut specs);
        add_spec(3, &mut specs);
        if test_handler.is_some() {
            let (_, producer, _) = CoreEngine::new(
                specs,
                JournalingProcessor::new(),
                Arc::new(KafkaEventsHandler::new("localhost:9093")),
                test_handler.unwrap(),
            );
            producer
        } else {
            let (_, producer, _) = CoreEngine::new(
                specs,
                JournalingProcessor::new(),
                Arc::new(KafkaEventsHandler::new("localhost:9093")),
                |_cmd: &mut OrderCommand, _seq: i64, _end_of_batch: bool| {},
            );
            producer
        }
    }

    #[test]
    fn test_simple() {
        let test_handler = Some(Box::new(
            |cmd: &mut OrderCommand, _seq: i64, _end_of_batch: bool| {
                println!("Test handler received command: {:?}", cmd);
            },
        ));
        let mut producer = get_producer(test_handler);

        producer.publish(|cmd: &mut OrderCommand| {
            cmd.user_id = 42;
            cmd.market_id = 1;
            cmd.order_id = 1001;
            cmd.price = 5000;
            cmd.size = 10;
            cmd.side = Side::Bid;
            cmd.status = Status::Processing;
        });
    }

    #[test]
    fn test_balance_lock_on_order_placement() {
        // 1. Setup
        let mut specs = HashMap::new();
        let base_asset_id = 1;
        let quote_asset_id = 2;
        // Market ID: base asset in lower 16 bits, quote in upper 16
        let market_id = ((quote_asset_id as u32) << 16) | (base_asset_id as u32);
        add_spec(market_id, &mut specs);

        let (tx, rx) = mpsc::channel::<OrderCommand>();

        let test_handler = move |cmd: &mut OrderCommand, _seq: i64, _end_of_batch: bool| {
            // The test handler is at the end of the disruptor pipeline.
            // We're interested in the final state of a placed order.
            if cmd.status != Status::Processing {
                tx.send(cmd.clone()).unwrap();
            }
        };

        let (_core_engine, mut producer, risk_engines_opt) = CoreEngine::new(
            specs,
            JournalingProcessor::new(),
            Arc::new(KafkaEventsHandler::new("localhost:9093")),
            test_handler,
        );
        let risk_engines = risk_engines_opt.expect("Risk engines should be available in test mode");

        // 2. Pre-fund user account
        let user_id = 42;
        let initial_base_balance = 1_000_000;
        let initial_quote_balance = 1_000_000;

        // User 42 belongs to shard 2 (42 & 3 = 2) with 4 shards.
        let shard_mask = risk_engines.len() as u64 - 1;
        let shard_id = (user_id & shard_mask) as usize;
        let risk_engine = &risk_engines[shard_id];

        risk_engine.set_balance(
            user_id,
            base_asset_id,
            UserBalance::new(initial_base_balance, 0),
        );
        risk_engine.set_balance(
            user_id,
            quote_asset_id,
            UserBalance::new(initial_quote_balance, 0),
        );

        // 3. Publish a BID order that will be placed on the book but not filled
        let order_price = 5000;
        let order_size = 10;
        let expected_locked_amount = order_price * order_size;

        producer.publish(|cmd: &mut OrderCommand| {
            *cmd = OrderCommand::new(
                TimeInForce::Gtc,
                1001,
                user_id,
                order_price,
                order_size,
                Side::Bid,
            );
            cmd.market_id = market_id;
        });

        // 4. Wait for the command to be processed and receive it from the test handler
        let processed_cmd = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("Test timed out waiting for processed command");

        // 5. Assertions
        assert_eq!(processed_cmd.order_id, 1001);
        assert_eq!(processed_cmd.status, Status::Placed);

        // The `balance` field on OrderCommand is the final balance state. For a BID order, the BASE asset is locked.
        let final_base_balance = processed_cmd.balance[0];
        assert_eq!(
            final_base_balance.available,
            initial_base_balance - expected_locked_amount
        );
        assert_eq!(final_base_balance.locked, expected_locked_amount);

        // Quote asset balance should be unchanged for a placed (unfilled) BID order
        let final_quote_balance = processed_cmd.balance[1];
        assert_eq!(final_quote_balance.available, initial_quote_balance);
        assert_eq!(final_quote_balance.locked, 0);
    }

    #[test]
    fn test_balance_update_on_trade() {
        // 1. Setup
        let mut specs = HashMap::new();
        let base_asset_id = 1;
        let quote_asset_id = 2;
        let market_id = ((quote_asset_id as u32) << 16) | (base_asset_id as u32);
        add_spec(market_id, &mut specs);

        let (tx, rx) = mpsc::channel::<OrderCommand>();
        let test_handler = move |cmd: &mut OrderCommand, _, _| {
            if cmd.status != Status::Processing {
                tx.send(cmd.clone()).unwrap();
            }
        };

        let (_core_engine, mut producer, risk_engines_opt) = CoreEngine::new(
            specs,
            JournalingProcessor::new(),
            Arc::new(KafkaEventsHandler::new("localhost:9093")),
            test_handler,
        );
        let risk_engines = risk_engines_opt.expect("Risk engines should be available in test mode");
        let shard_mask = risk_engines.len() as u64 - 1;

        // 2. Define users and order details
        let taker_id = 42; // Belongs to shard 2 (42 & 3 = 2)
        let maker_id = 101; // Belongs to shard 1 (101 & 3 = 1)

        let order_price = 5000;
        let order_size = 10_000; // Use a larger size to make fees non-zero

        // 3. Pre-fund accounts
        // Taker (buyer) needs base asset to buy the quote asset
        let taker_initial_base = order_price * order_size;
        let taker_shard_id = (taker_id & shard_mask) as usize;
        risk_engines[taker_shard_id].set_balance(
            taker_id,
            base_asset_id,
            UserBalance::new(taker_initial_base, 0),
        );
        risk_engines[taker_shard_id].set_balance(taker_id, quote_asset_id, UserBalance::new(0, 0));

        // Maker (seller) needs quote asset to sell
        let maker_initial_quote = order_size;
        let maker_shard_id = (maker_id & shard_mask) as usize;
        risk_engines[maker_shard_id].set_balance(
            maker_id,
            quote_asset_id,
            UserBalance::new(maker_initial_quote, 0),
        );
        risk_engines[maker_shard_id].set_balance(maker_id, base_asset_id, UserBalance::new(0, 0));

        // 4. Place Maker's ASK (sell) order
        let maker_order_id = 2001;
        producer.publish(|cmd| {
            *cmd = OrderCommand::new(
                TimeInForce::Gtc,
                maker_order_id,
                maker_id,
                order_price,
                order_size,
                Side::Ask,
            );
            cmd.market_id = market_id;
        });

        // 5. Wait for Maker's order to be placed on the book
        let maker_placed_cmd = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("Did not receive maker's placed command");
        assert_eq!(maker_placed_cmd.order_id, maker_order_id);
        assert_eq!(maker_placed_cmd.status, Status::Placed);

        // 6. Place Taker's BID (buy) order to trigger a trade
        let taker_order_id = 1002;
        producer.publish(|cmd| {
            *cmd = OrderCommand::new(
                TimeInForce::Gtc,
                taker_order_id,
                taker_id,
                order_price,
                order_size,
                Side::Bid,
            );
            cmd.market_id = market_id;
        });

        // 7. Wait for Taker's order to be filled. This command will contain the trade event.
        let taker_filled_cmd = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("Did not receive taker's filled command");
        assert_eq!(taker_filled_cmd.order_id, taker_order_id);
        assert_eq!(taker_filled_cmd.status, Status::Filled);

        // 8. Assertions
        let trade_event = taker_filled_cmd
            .events()
            .expect("Taker command should have a trade event");

        // --- Taker (Buyer) Balance Verification ---
        let taker_fee = (order_size * 20) / 10000; // 0.2% taker fee on quote asset
        let net_quote_received = order_size - taker_fee;

        assert_eq!(
            taker_filled_cmd.balance[0].total(),
            0,
            "Taker's base balance should be 0"
        );
        assert_eq!(
            taker_filled_cmd.balance[1].total(),
            net_quote_received,
            "Taker's quote balance is incorrect"
        );
        assert_eq!(
            risk_engines[taker_shard_id].get_balance(taker_id, base_asset_id),
            taker_filled_cmd.balance[0]
        );
        assert_eq!(
            risk_engines[taker_shard_id].get_balance(taker_id, quote_asset_id),
            taker_filled_cmd.balance[1]
        );

        // --- Maker (Seller) Balance Verification ---
        let gross_base_received = order_price * order_size;
        let maker_fee = (gross_base_received * 10) / 10000; // 0.1% maker fee on base asset
        let net_base_received = gross_base_received - maker_fee;

        assert_eq!(
            trade_event.maker_balance[0].total(),
            net_base_received,
            "Maker's base balance is incorrect"
        );
        assert_eq!(
            trade_event.maker_balance[1].total(),
            0,
            "Maker's quote balance should be 0"
        );
        assert_eq!(
            risk_engines[maker_shard_id].get_balance(maker_id, base_asset_id),
            trade_event.maker_balance[0]
        );
        assert_eq!(
            risk_engines[maker_shard_id].get_balance(maker_id, quote_asset_id),
            trade_event.maker_balance[1]
        );
    }

    #[test]
    fn test_balance_update_on_partial_multi_trade() {
        // 1. Setup
        let mut specs = HashMap::new();
        let base_asset_id = 1;
        let quote_asset_id = 2;
        let market_id = ((quote_asset_id as u32) << 16) | (base_asset_id as u32);
        add_spec(market_id, &mut specs);

        let (tx, rx) = mpsc::channel::<OrderCommand>();
        let test_handler = move |cmd: &mut OrderCommand, _, _| {
            if cmd.status != Status::Processing {
                tx.send(cmd.clone()).unwrap();
            }
        };

        let (_core_engine, mut producer, risk_engines_opt) = CoreEngine::new(
            specs,
            JournalingProcessor::new(),
            Arc::new(KafkaEventsHandler::new("localhost:9093")),
            test_handler,
        );
        let risk_engines = risk_engines_opt.expect("Risk engines should be available in test mode");
        let shard_mask = risk_engines.len() as u64 - 1;
        let get_shard_id = |user_id: u64| (user_id & shard_mask) as usize;

        // 2. Define users across different shards
        let taker_id = 50; // Shard 2 (50 & 3 = 2)
        let maker1_id = 101; // Shard 1 (101 & 3 = 1)
        let maker2_id = 102; // Shard 2 (102 & 3 = 2)
        let maker3_id = 103; // Shard 3 (103 & 3 = 3)

        let maker1_shard = get_shard_id(maker1_id);
        let maker2_shard = get_shard_id(maker2_id);
        let maker3_shard = get_shard_id(maker3_id);
        let taker_shard = get_shard_id(taker_id);

        // 3. Pre-fund accounts
        let order_price = 5000;
        let maker1_size = 10_000;
        let maker2_size = 20_000;
        let maker3_size = 30_000;
        let taker_trade_size = maker1_size + maker2_size + 15_000; // Will take all of m1, m2, and 15k of m3

        // Fund makers with quote asset to sell
        risk_engines[maker1_shard].set_balance(
            maker1_id,
            quote_asset_id,
            UserBalance::new(maker1_size, 0),
        );
        risk_engines[maker2_shard].set_balance(
            maker2_id,
            quote_asset_id,
            UserBalance::new(maker2_size, 0),
        );
        risk_engines[maker3_shard].set_balance(
            maker3_id,
            quote_asset_id,
            UserBalance::new(maker3_size, 0),
        );

        // Fund taker with base asset to buy
        let taker_initial_base = order_price * taker_trade_size;
        risk_engines[taker_shard].set_balance(
            taker_id,
            base_asset_id,
            UserBalance::new(taker_initial_base, 0),
        );

        // 4. Place Maker orders (all at the same price to test FIFO)
        let maker_orders = [
            (maker1_id, maker1_size, 2001),
            (maker2_id, maker2_size, 2002),
            (maker3_id, maker3_size, 2003),
        ];

        for &(id, size, order_id) in &maker_orders {
            producer.publish(|cmd| {
                *cmd =
                    OrderCommand::new(TimeInForce::Gtc, order_id, id, order_price, size, Side::Ask);
                cmd.market_id = market_id;
            });
            let placed_cmd = rx
                .recv_timeout(Duration::from_secs(1))
                .expect("Maker order placement timed out");
            assert_eq!(placed_cmd.order_id, order_id);
            assert_eq!(placed_cmd.status, Status::Placed);
        }

        // 5. Place Taker's BID order to sweep the book
        let taker_order_id = 1001;
        producer.publish(|cmd| {
            *cmd = OrderCommand::new(
                TimeInForce::Gtc,
                taker_order_id,
                taker_id,
                order_price,
                taker_trade_size,
                Side::Bid,
            );
            cmd.market_id = market_id;
        });

        // 6. Wait for Taker's order to be filled.
        let taker_filled_cmd = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("Did not receive taker's filled command");
        assert_eq!(taker_filled_cmd.order_id, taker_order_id);
        assert_eq!(taker_filled_cmd.status, Status::Filled);

        // 7. Assertions
        let mut event_opt = taker_filled_cmd.events();
        assert!(
            event_opt.is_some(),
            "Taker command should have trade events"
        );

        // --- Event 1: Trade with Maker 1 ---
        let event1 = event_opt.unwrap();
        assert_eq!(event1.maker_user_id, maker1_id);
        assert_eq!(event1.size, maker1_size);
        assert!(event1.matched_order_completed);

        // --- Event 2: Trade with Maker 2 ---
        event_opt = event1.next_event.as_deref();
        let event2 = event_opt.unwrap();
        assert_eq!(event2.maker_user_id, maker2_id);
        assert_eq!(event2.size, maker2_size);
        assert!(event2.matched_order_completed);

        // --- Event 3: Trade with Maker 3 ---
        event_opt = event2.next_event.as_deref();
        let event3 = event_opt.unwrap();
        let maker3_trade_size = 15_000;
        assert_eq!(event3.maker_user_id, maker3_id);
        assert_eq!(event3.size, maker3_trade_size);
        assert!(
            !event3.matched_order_completed,
            "Maker 3 order should be partially filled"
        );
        assert!(
            event3.active_order_completed,
            "Taker order should be completed"
        );

        // --- Verify Maker Balances from Events and Risk Engine State ---
        let maker_fee_rate = 10; // 0.1%
        for (event, maker_id, maker_shard, initial_size) in [
            (event1, maker1_id, maker1_shard, maker1_size),
            (event2, maker2_id, maker2_shard, maker2_size),
            (event3, maker3_id, maker3_shard, maker3_size),
        ] {
            let trade_size = event.size;
            let base_recv = order_price * trade_size;
            let fee = (base_recv * maker_fee_rate) / 10000;
            let net_base_recv = base_recv - fee;

            let (final_quote_avail, final_quote_locked) = if event.matched_order_completed {
                (0, 0)
            } else {
                (0, initial_size - trade_size)
            };

            assert_eq!(event.maker_balance[0].available, net_base_recv);
            assert_eq!(event.maker_balance[0].locked, 0);
            assert_eq!(event.maker_balance[1].available, final_quote_avail);
            assert_eq!(event.maker_balance[1].locked, final_quote_locked);

            assert_eq!(
                risk_engines[maker_shard].get_balance(maker_id, base_asset_id),
                event.maker_balance[0]
            );
            assert_eq!(
                risk_engines[maker_shard].get_balance(maker_id, quote_asset_id),
                event.maker_balance[1]
            );
        }

        // --- Final Taker Balance Verification ---
        let total_quote_received = taker_trade_size;
        let taker_fee = (total_quote_received * 20) / 10000; // 0.2%
        let net_quote_received = total_quote_received - taker_fee;

        assert_eq!(
            taker_filled_cmd.balance[0].total(),
            0,
            "Taker's base balance should be 0"
        );
        assert_eq!(
            taker_filled_cmd.balance[1].total(),
            net_quote_received,
            "Taker's quote balance is incorrect"
        );
        assert_eq!(
            risk_engines[taker_shard].get_balance(taker_id, base_asset_id),
            taker_filled_cmd.balance[0]
        );
        assert_eq!(
            risk_engines[taker_shard].get_balance(taker_id, quote_asset_id),
            taker_filled_cmd.balance[1]
        );

        // --- No more events ---
        assert!(
            event3.next_event.is_none(),
            "There should be no more events"
        );
    }

    #[test]
    fn test_ioc_market_order_partial_fill_and_cancel() {
        // 1. Setup
        let mut specs = HashMap::new();
        let base_asset_id = 1;
        let quote_asset_id = 2;
        let market_id = ((quote_asset_id as u32) << 16) | (base_asset_id as u32);
        add_spec(market_id, &mut specs);
        let (mut producer, risk_engines, rx) = setup_test_env(specs.clone());

        let price_cache = Arc::new(PriceCache::new(specs.keys()));

        let maker_id = 101; // Shard 1
        let taker_id = 42; // Shard 2
        let maker_shard = 1;
        let taker_shard = 2;

        // 2. Fund accounts
        let maker_ask_price = 50_000;
        let maker_ask_size = 5_000;
        risk_engines[maker_shard]
            .set_balance(maker_id, quote_asset_id, UserBalance::new(maker_ask_size, 0));

        let taker_initial_base = 1_000_000_000;
        risk_engines[taker_shard].set_balance(
            taker_id,
            base_asset_id,
            UserBalance::new(taker_initial_base, 0),
        );

        // 3. Place Maker's resting ASK order
        producer.publish(|cmd| {
            *cmd = OrderCommand::new(
                TimeInForce::Gtc,
                2001,
                maker_id,
                maker_ask_price,
                maker_ask_size,
                Side::Ask,
            );
            cmd.market_id = market_id;
        });
        let maker_placed = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(maker_placed.status, Status::Placed);
        price_cache.update_prices(market_id, 0, maker_ask_price); // Manually update price cache

        // 4. Place Taker's IOC Market Buy order, larger than available liquidity
        let taker_order_size = 10_000;
        producer.publish(|cmd| {
            *cmd = OrderCommand::new(
                TimeInForce::Ioc,
                1001,
                taker_id,
                u64::MAX, // Market order price
                taker_order_size,
                Side::Bid,
            );
            cmd.market_id = market_id;
        });

        // 5. Wait for Taker's partially filled command
        let taker_cmd = rx.recv_timeout(Duration::from_secs(5)).unwrap();

        // 6. Assertions
        assert_eq!(taker_cmd.status, Status::PartiallyFilled);
        assert_eq!(taker_cmd.size, taker_order_size - maker_ask_size); // Remaining size

        let event = taker_cmd.events().unwrap();
        assert_eq!(event.size, maker_ask_size);
        assert_eq!(event.price, maker_ask_price);
        assert!(event.matched_order_completed); // Maker order was fully filled

        // --- Verify Balances ---
        // Taker (Buyer)
        let taker_fee = (maker_ask_size * 20) / 10000; // 0.2% on quote
        let taker_net_quote_received = maker_ask_size - taker_fee;
        let taker_base_spent = maker_ask_price * maker_ask_size;

        let taker_final_base = risk_engines[taker_shard].get_balance(taker_id, base_asset_id);
        let taker_final_quote = risk_engines[taker_shard].get_balance(taker_id, quote_asset_id);

        assert_eq!(
            taker_final_base.total(),
            taker_initial_base - taker_base_spent
        );
        assert_eq!(taker_final_base.locked, 0, "All locked funds for taker should be settled or released");
        assert_eq!(taker_final_quote.total(), taker_net_quote_received);

        // Maker (Seller)
        let maker_gross_base_received = maker_ask_price * maker_ask_size;
        let maker_fee = (maker_gross_base_received * 10) / 10000; // 0.1% on base
        let maker_net_base_received = maker_gross_base_received - maker_fee;

        let maker_final_base = risk_engines[maker_shard].get_balance(maker_id, base_asset_id);
        let maker_final_quote = risk_engines[maker_shard].get_balance(maker_id, quote_asset_id);

        assert_eq!(maker_final_base.total(), maker_net_base_received);
        assert_eq!(maker_final_quote.total(), 0);
    }

    #[test]
    fn test_fok_limit_order_rejection_and_success() {
        // 1. Setup
        let mut specs = HashMap::new();
        let base_asset_id = 1;
        let quote_asset_id = 2;
        let market_id = ((quote_asset_id as u32) << 16) | (base_asset_id as u32);
        add_spec(market_id, &mut specs);
        let (mut producer, risk_engines, rx) = setup_test_env(specs);

        let maker_id = 101;
        let taker_id = 42;
        let maker_shard = 1;
        let taker_shard = 2;

        // 2. Fund accounts
        risk_engines[maker_shard].set_balance(maker_id, quote_asset_id, UserBalance::new(100, 0));
        let taker_initial_base = 10_000_000;
        risk_engines[taker_shard]
            .set_balance(taker_id, base_asset_id, UserBalance::new(taker_initial_base, 0));

        // 3. Place Maker's resting ASK order
        producer.publish(|cmd| {
            *cmd = OrderCommand::new(TimeInForce::Gtc, 2001, maker_id, 50_000, 100, Side::Ask);
            cmd.market_id = market_id;
        });
        rx.recv().unwrap(); // Consume maker's placed command

        // --- Part 1: FOK Rejection ---

        // 4. Place Taker's FOK order that is too large
        producer.publish(|cmd| {
            *cmd = OrderCommand::new(TimeInForce::Fok, 1001, taker_id, 50_000, 101, Side::Bid);
            cmd.market_id = market_id;
        });

        // 5. Assert rejection
        let taker_rejected_cmd = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(taker_rejected_cmd.status, Status::Cancelled);
        assert!(taker_rejected_cmd.events().is_none());

        // Balances should be unchanged
        let taker_balance_after_rejection =
            risk_engines[taker_shard].get_balance(taker_id, base_asset_id);
        assert_eq!(
            taker_balance_after_rejection.total(),
            taker_initial_base
        );
        assert_eq!(taker_balance_after_rejection.locked, 0);

        // --- Part 2: FOK Success with Price Improvement ---

        // 6. Place another maker order to provide enough liquidity with price improvement
        let maker2_id = 105; // Shard 1
        risk_engines[maker_shard].set_balance(maker2_id, quote_asset_id, UserBalance::new(100, 0));
        producer.publish(|cmd| {
            *cmd = OrderCommand::new(TimeInForce::Gtc, 2002, maker2_id, 49_000, 100, Side::Ask);
            cmd.market_id = market_id;
        });
        rx.recv().unwrap(); // Consume maker2's placed command

        // 7. Place Taker's FOK order that can now be filled
        producer.publish(|cmd| {
            *cmd = OrderCommand::new(TimeInForce::Fok, 1002, taker_id, 50_000, 150, Side::Bid);
            cmd.market_id = market_id;
        });

        // 8. Assert success
        let taker_filled_cmd = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(taker_filled_cmd.status, Status::Filled);

        let mut event_opt = taker_filled_cmd.events();
        let event1 = event_opt.unwrap(); // Match with maker2 at 49k
        event_opt = event1.next_event.as_deref();
        let event2 = event_opt.unwrap(); // Match with maker1 at 50k

        assert_eq!(event1.price, 49_000);
        assert_eq!(event1.size, 100);
        assert_eq!(event2.price, 50_000);
        assert_eq!(event2.size, 50);

        // 9. Verify final balances
        let taker_base_spent = (100 * 49_000) + (50 * 50_000);
        let taker_final_base = risk_engines[taker_shard].get_balance(taker_id, base_asset_id);

        // NOTE: This assertion will likely fail with the current settlement logic,
        // as the price improvement (difference between locked amount and actual cost)
        // is not being refunded to the user's available balance. This highlights a bug.
        assert_eq!(
            taker_final_base.total(),
            taker_initial_base - taker_base_spent,
            "Taker's final base balance is incorrect"
        );
        assert_eq!(taker_final_base.locked, 0);
    }
}
