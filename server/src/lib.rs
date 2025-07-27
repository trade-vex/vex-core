pub mod engine;
pub mod events;

use std::sync::Arc;

use common::{
    cmd::OrderCommand,
    model::{
        symbol_specification::CoreSymbolSpecification,
        user_profile::{UserProfile, UserStatus},
    },
};
use disruptor::{MultiProducer, MultiConsumerBarrier};
use hashbrown::HashMap;
use orderbook::OrderBookImplType;
use processors::{
    journaling::JournalingProcessor, matching_engine::MatchingEngineRouter, risk_engine::RiskEngine,
};

use crate::{engine::CoreEngine, events::SimpleEventsHandler};

/// Sets up the entire Exchange Core application with all processors.
pub fn init_exchange() -> (
    CoreEngine,
    MultiProducer<OrderCommand, MultiConsumerBarrier>,
    Arc<SimpleEventsHandler>,
) {
    // Initialize the matching engine router with a default order book.
    let mut matching_engine_router = MatchingEngineRouter::new();
    matching_engine_router.add_symbol(0, OrderBookImplType::Naive);

    // Create a symbol specification for the risk engine.
    let mut symbol_specs = HashMap::new();
    // : Use distinct base and quote currencies for a realistic test
    let mut spec = CoreSymbolSpecification::default();
    spec.base_currency = 1; // e.g., BTC
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
