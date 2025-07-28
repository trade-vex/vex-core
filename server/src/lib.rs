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

const NUM_RISK_ENGINES: i32 = 2; // Example: must be a power of 2
const NUM_MATCHING_ENGINES: i64 = 1; // Example: must be a power of 2

/// Sets up the entire Exchange Core application with all processors.
pub fn init_exchange() -> (
    CoreEngine,
    MultiProducer<OrderCommand, MultiConsumerBarrier>,
    Arc<SimpleEventsHandler>,
) {
    // Initialize the matching engine router with a default order book.
    let matching_engine_routers: Vec<MatchingEngineRouter> = (0..NUM_MATCHING_ENGINES)
        .map(|shard_id| {
            let mut router = MatchingEngineRouter::new(shard_id as i32, NUM_MATCHING_ENGINES);
            // Symbols are distributed across shards. This logic adds symbol 0 to shard 0.
            if (0 & (NUM_MATCHING_ENGINES - 1)) == shard_id {
                router.add_symbol(0, OrderBookImplType::Naive);
            }
            router
        })
        .collect();

    // Create a symbol specification for the risk engine.
    let mut symbol_specs = HashMap::new();
    // Use distinct base and quote currencies for a realistic test
    let mut spec = CoreSymbolSpecification::default();
    spec.base_currency = 1; // e.g., BTC
    spec.quote_currency = 2; // e.g., USD
    symbol_specs.insert(0, spec);

    // Create multiple risk engines for parallel processing
    let risk_engines: Vec<RiskEngine> = (0..NUM_RISK_ENGINES)
        .map(|shard_id| {
            let mut risk_engine = RiskEngine::new(symbol_specs.clone(), shard_id, NUM_RISK_ENGINES);
            
            if (100 & (NUM_RISK_ENGINES - 1) as i64) == shard_id as i64 {
                let mut user_profile_100 = UserProfile::new(100, UserStatus::Active);
                user_profile_100.accounts.insert(1, 1_000_000);
                risk_engine.user_profiles.insert(100, user_profile_100);
            }
            if (101 & (NUM_RISK_ENGINES - 1) as i64) == shard_id as i64 {
                let mut user_profile_101 = UserProfile::new(101, UserStatus::Active);
                user_profile_101.accounts.insert(2, 1_000_000);
                risk_engine.user_profiles.insert(101, user_profile_101);
            }
            risk_engine
        })
        .collect();

    // Initialize the journaling processor to log commands and events.
    let journaling_processor = JournalingProcessor::new();

    // Create a events handler to collect and process events.
    let events_handler = Arc::new(SimpleEventsHandler::new()); // shared with core

    // Create the Exchange Core with all components wired together.
    let (core_engine, producer) = CoreEngine::new(
        risk_engines,
        matching_engine_routers,
        journaling_processor,
        events_handler.clone(),
    );

    // Return core_engine, producer, and handler directly
    (core_engine, producer, events_handler)
}

