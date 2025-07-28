pub mod engine;
pub mod events;

use std::sync::Arc;

use common::{
    cmd::OrderCommand,
    model::{
        symbol_specification::CoreSymbolSpecification,
    },
};
use disruptor::{MultiConsumerBarrier, MultiProducer};
use hashbrown::HashMap;
use orderbook::OrderBookImplType;
use processors::{journaling::JournalingProcessor, risk_engine::RiskEngine};

use crate::{engine::CoreEngine, events::SimpleEventsHandler};

/// Sets up the entire Exchange Core application with all processors.
///
/// This creates the core engine and adds symbols dynamically
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
    let risk_engine = RiskEngine::new(symbol_specs);

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
