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
    symbol_specs.insert(0, CoreSymbolSpecification::default());

    // Create a risk engine with a user profile(say 100 here)
    let mut risk_engine = RiskEngine::new(symbol_specs);
    let mut user_profile = UserProfile::new(100, UserStatus::Active);
    // Deposit funds for the user to be able to place an ASK order
    // The default symbol spec has base currency 0, so we deposit into that account.
    user_profile.accounts.insert(0, 1_000_000); 
    risk_engine
        .user_profiles
        .insert(100, user_profile);

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

    // 1. Set up the entire exchange core with its pipeline.
    let (mut core, mut producer, handler) = init_exchange();

    // 2. Spawn the core engine to run in the background.
    let core_handle = tokio::spawn(async move {
        core.run().await;
    });

    // 3. Directly publish a command to the disruptor
    let mut cmd = OrderCommand::default();
    // Set fields as needed for your test
    cmd.order_id = 1;
    cmd.uid = 100;
    cmd.symbol = 0;
    cmd.size = 10;
    cmd.price = 9629;

    producer.publish(|e| {
        *e = cmd.clone();
    });

    // Give the engine a moment to process the command sequentially.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 4. ASSERT: Check that the correct final event was produced and handled.
    info!("\n--- Asserting final event received by handler ---");
    let received_events = handler.events.lock().unwrap();
    assert_eq!(
        received_events.len(),
        1,
        "Should have received exactly one event"
    );

    let event = &received_events[0];
    assert_eq!(
        event.event_type,
        common::model::enums::MatcherEventType::Reduce
    );
    assert!(!event.matched_order_completed);
    assert_eq!(event.matched_order_id, 1);
    assert_eq!(event.maker_uid, 100);
    assert_eq!(event.price, 9629);
    assert_eq!(event.size, 10);
    info!(
        "\n--- Test Passed: Full pipeline executed and correct PlaceOrder event was received. ---"
    );

    drop(core_handle);
}