mod api;
mod engine;
mod events;

use api::ExchangeApi;
use common::cmd::OrderCommand;
use common::model::{
    enums::{OrderAction, OrderType},
    user_profile::{UserProfile, UserStatus},
};
use engine::CoreEngine;
use events::SimpleEventsHandler;
use orderbook::OrderBookImplType;
use processors::{
    journaling::JournalingProcessor, matching_engine::MatchingEngineRouter, risk_engine::RiskEngine,
};
use std::sync::Arc;
use tokio::sync::mpsc;

// Sets up the entire Exchange Core application with all processors.
pub fn init_exchange() -> (ExchangeApi, CoreEngine, Arc<SimpleEventsHandler>) {
    let (command_tx, command_rx) = mpsc::channel(1024);

    // Initialize the matching engine router with a default order book.
    let mut matching_engine_router = MatchingEngineRouter::new();
    matching_engine_router.add_symbol(0, OrderBookImplType::Naive);

    // Create a risk engine with a user profile(say 100 here)
    let mut risk_engine = RiskEngine::new();
    risk_engine
        .user_profiles
        .insert(100, UserProfile::new(100, UserStatus::Active));

    // Initialize the journaling processor to log commands and events.
    let journaling_processor = JournalingProcessor::new();

    // Create a events handler to collect and process events.
    let events_handler = Arc::new(SimpleEventsHandler::new()); // shared with core

    // Create the Exchange Core with all components wired together.
    let core_engine = CoreEngine::new(
        command_rx,
        risk_engine,
        matching_engine_router,
        journaling_processor,
        events_handler.clone(),
    );

    // Create the Exchange API that will allow external clients to submit commands.
    let exchange_api = ExchangeApi::new(command_tx);

    (exchange_api, core_engine, events_handler)
}

// Simulates a gateway receiving a message from a client over the network (will be from Aeron later).
async fn run_dummy_gateway_listener(api: ExchangeApi) {
    println!("[Dummy Client Gateway] Simulating message received from client...");

    let new_order_cmd =
        OrderCommand::new_order(OrderType::Gtc, 1, 100, 9629, 1500, 10, OrderAction::Bid);

    println!("[Dummy Gateway] Submitting command to the Exchange API...");
    api.submit_command(new_order_cmd).await.unwrap();
    println!("[Dummy Gateway] Command submitted successfully.");
}

#[tokio::main]
async fn main() {
    // Initialize logging to see output from `tracing`.
    tracing_subscriber::fmt::init();

    println!("--- Running Full Sequential Core Test ---");

    // 1. Set up the entire exchange core with its pipeline.
    let (api, mut core, handler) = init_exchange();

    // 2. Spawn the core engine to run in the background.
    let core_handle = tokio::spawn(async move {
        core.run().await;
    });

    // 3. Run the dummy gateway to simulate a client sending an order.
    run_dummy_gateway_listener(api).await;

    // Give the engine a moment to process the command sequentially.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 4. ASSERT: Check that the correct final event was produced and handled.
    println!("\n--- Asserting final event received by handler ---");
    // When a GTC order is placed but not matched, it results in a "Reduce" event with matched_order_completed as false
    // representing the funds being put on hold.
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
    assert_eq!(event.matched_order_uid, 100);
    assert_eq!(event.price, 9629);
    assert_eq!(event.size, 10);
    println!(
        "\n--- Test Passed: Full pipeline executed and correct PlaceOrder event was received. ---"
    );

    // Cleanly shut down the core task and release resources.
    drop(core_handle);
}
