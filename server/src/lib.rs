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
use disruptor::{MultiConsumerBarrier, MultiProducer};
use hashbrown::HashMap;
use orderbook::OrderBookImplType;
use processors::{journaling::JournalingProcessor, risk_engine::RiskEngine};

use crate::{engine::CoreEngine, events::SimpleEventsHandler};

/// Sets up the entire Exchange Core application with all processors.
///
/// This creates the core engine and adds symbols dynamically, like real exchanges.
pub fn init_exchange() -> (
    CoreEngine,
    MultiProducer<OrderCommand, MultiConsumerBarrier>,
    Arc<SimpleEventsHandler>,
) {
    // Create symbol specifications for the risk engine
    let mut symbol_specs = HashMap::new();
    let mut spec = CoreSymbolSpecification::default();
    spec.base_currency = 1; // BTC
    spec.quote_currency = 2; // USD
    symbol_specs.insert(0, spec);

    // Create risk engine with funded user profiles
    let mut risk_engine = RiskEngine::new(symbol_specs);

    // Fund seller (user 100) with base currency (BTC)
    let mut seller_profile = UserProfile::new(100, UserStatus::Active);
    seller_profile.accounts.insert(1, 1_000_000); // 1M BTC
    risk_engine.user_profiles.insert(100, seller_profile);

    // Fund buyer (user 101) with quote currency (USD)
    let mut buyer_profile = UserProfile::new(101, UserStatus::Active);
    buyer_profile.accounts.insert(2, 1_000_000); // 1M USD
    risk_engine.user_profiles.insert(101, buyer_profile);

    // Initialize journaling processor for audit trail
    let journaling_processor = JournalingProcessor::new();

    // Create events handler for trade events
    let events_handler = Arc::new(SimpleEventsHandler::new());

    // Create the Exchange Core with empty routers
    let (core_engine, producer) =
        CoreEngine::new(risk_engine, journaling_processor, events_handler.clone());

    core_engine.add_symbol(0, OrderBookImplType::Naive);

    (core_engine, producer, events_handler)
}
