use hashbrown::HashMap;
use server::engine::CoreEngine;
use server::events::SimpleEventsHandler;

use common::cmd::OrderCommand;
use common::model::{
    symbol_specification::CoreSymbolSpecification,
    // enums::{OrderAction, OrderType},
    user_profile::{UserProfile, UserStatus},
};
use disruptor::Producer;
use orderbook::OrderBookImplType;
use processors::{
    journaling::JournalingProcessor, matching_engine::MatchingEngineRouter, risk_engine::RiskEngine,
};
use std::sync::Arc;
use tracing::info;
// Sets up the entire Exchange Core application with all processors.
pub fn init_exchange() -> (
    CoreEngine,
    disruptor::MultiProducer<OrderCommand, disruptor::MultiConsumerBarrier>,
    Arc<SimpleEventsHandler>,
) {
    // Initialize the matching engine router with a default order book.
    let mut matching_engine_router = MatchingEngineRouter::new();
    matching_engine_router.add_symbol(0, OrderBookImplType::Naive);

    // Create a symbol specification for the risk engine.
    let mut symbol_specs = HashMap::new();
    // : Use distinct base and quote currencies for a realistic test
    let mut spec = CoreSymbolSpecification::default();
    spec.base_currency = 1;  // e.g., BTC
    spec.quote_currency = 2; // e.g., USD
    symbol_specs.insert(0, spec);

    // Create a risk engine with a user profile(say 100 here)
    let mut risk_engine = RiskEngine::new(symbol_specs);

    // : Fund the seller (user 100) with base currency (1)
    let mut user_profile_100 = UserProfile::new(100, UserStatus::Active);
    user_profile_100.accounts.insert(1, 1_000_000); 
    risk_engine.user_profiles.insert(100, user_profile_100);

    // : Fund the buyer (user 101) with quote currency (2)
    let mut user_profile_101 = UserProfile::new(101, UserStatus::Active);
    user_profile_101.accounts.insert(2, 1_000_000);
    risk_engine.user_profiles.insert(101, user_profile_101);

    // Initialize the journaling processor to log commands and events.
    let journaling_processor = JournalingProcessor::new();

    // Create a events handler to collect and process events.
    let events_handler = Arc::new(SimpleEventsHandler::new()); // shared with core

    // Create the Exchange Core with all components wired together.
    let (core_engine, producer) = CoreEngine::new(
        risk_engine,
        matching_engine_router,
        journaling_processor,
        events_handler.clone(),
    );

    // Return core_engine, producer, and handler directly
    (core_engine, producer, events_handler)
}

// Simulates a gateway receiving a message from a client over the network (will be from Aeron later).

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    info!("--- Running Full Disruptor Core Test ---");

    let (mut core, mut producer, handler) = init_exchange();

    // Place order
    let mut cmd = OrderCommand::default();
    cmd.order_id = 1;
    cmd.uid = 100;
    cmd.symbol = 0;
    cmd.size = 10;
    cmd.price = 9629;
    producer.publish(|e| *e = cmd.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Cancel order
    let mut cancel_cmd = OrderCommand::cancel(1, 100);
    cancel_cmd.symbol = 0;
    producer.publish(|e| *e = cancel_cmd.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Reduce order
    let mut cmd2 = OrderCommand::default();
    cmd2.order_id = 2;
    cmd2.uid = 100;
    cmd2.symbol = 0;
    cmd2.size = 10;
    cmd2.price = 9629;
    producer.publish(|e| *e = cmd2.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut reduce_cmd = OrderCommand::reduce(2, 100, 5);
    reduce_cmd.symbol = 0;
    producer.publish(|e| *e = reduce_cmd.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Move order
    let mut cmd3 = OrderCommand::default();
    cmd3.order_id = 3;
    cmd3.uid = 100;
    cmd3.symbol = 0;
    cmd3.size = 10;
    cmd3.price = 9629;
    producer.publish(|e| *e = cmd3.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut move_cmd = OrderCommand::move_order(3, 100, 9700);
    move_cmd.symbol = 0;
    producer.publish(|e| *e = move_cmd.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Check events
    info!("\n--- Asserting events received by handler ---");
    { // : Add an inner scope to release the lock
        let received_events = handler.events.lock().unwrap();
        assert!(
            received_events.len() >= 4,
            "Should have received at least four events"
        );

        // Detailed assertions for event types
        let mut has_reduce = false;
        let mut has_cancel = false;
        let mut _has_move = false;
        for event in received_events.iter() {
            match format!("{:?}", event) {
                s if s.contains("Reduce") => has_reduce = true,
                s if s.contains("Cancel") => has_cancel = true,
                s if s.contains("Move") => _has_move = true,
                _ => {}
            }
        }
        assert!(has_reduce, "Should have at least one Reduce event");
        assert!(has_cancel, "Should have at least one Cancel event");
    } // : The lock on handler.events is released here as `received_events` goes out of scope.

    // --- User balance assertion ---
    // : Check for the correct currency (1 for the seller)
    let balance = core.get_user_balance(100, 1).unwrap();
    println!("User 100 balance in currency 1: {}", balance);

    // --- Matching test: Place matching ASK and BID orders ---
    // Use a price that is guaranteed to be the best available to ensure the correct orders match.
    let mut ask_cmd = OrderCommand::new_order(
        common::model::enums::OrderType::Gtc,
        10, // order_id
        100, // uid
        9620, // price (better than the existing order at 9629)
        0, // reserve_bid_price
        5, // size
        common::model::enums::OrderAction::Ask,
    );
    ask_cmd.symbol = 0;
    producer.publish(|e| *e = ask_cmd.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut bid_cmd = OrderCommand::new_order(
        common::model::enums::OrderType::Gtc,
        11, // order_id
        101, // uid (different user)
        9620, // price (matches the new ASK)
        0,
        5,
        common::model::enums::OrderAction::Bid,
    );
    bid_cmd.symbol = 0;
    producer.publish(|e| *e = bid_cmd.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // --- Assert trade event and filled quantities ---
    let received_events = handler.events.lock().unwrap();
    let mut has_trade = false;
    for event in received_events.iter() {
        if format!("{:?}", event).contains("Trade") {
            has_trade = true;
            println!("Trade event: {:?}", event);
        }
    }
    assert!(has_trade, "Should have at least one Trade event");

    // --- Assert user balances updated ---
    // Check for the correct currencies (1 and 2) after the trade
    let ask_user_base_balance = core.get_user_balance(100, 1).unwrap();
    let ask_user_quote_balance = core.get_user_balance(100, 2).unwrap();
    let bid_user_base_balance = core.get_user_balance(101, 1).unwrap();
    let bid_user_quote_balance = core.get_user_balance(101, 2).unwrap();
    println!("User 100 (ASK) balances: base={}, quote={}", ask_user_base_balance, ask_user_quote_balance);
    println!("User 101 (BID) balances: base={}, quote={}", bid_user_base_balance, bid_user_quote_balance);

    core.run().await;
}
