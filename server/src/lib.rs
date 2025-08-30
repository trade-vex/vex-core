pub mod engine;
pub mod utils;

use std::sync::Arc;

use common::model::market_specification::CoreMarketSpecification;
use hashbrown::HashMap;
use processors::journaling::JournalingProcessor;
use processors::events::SimpleEventsHandler;

use crate::engine::{CoreEngine, OrderProducer};

/// Sets up the entire Exchange Core application with all processors.
///
/// This creates the core engine and adds symbols from the provided configuration
pub fn init_exchange(
    symbol_specs: HashMap<u32, CoreMarketSpecification>,
) -> (CoreEngine, OrderProducer, Arc<SimpleEventsHandler>) {
    // Initialize journaling processor for audit trail
    let journaling_processor = JournalingProcessor::new();

    // Create events handler for trade events
    let events_handler = Arc::new(SimpleEventsHandler::new());

    // Create the Exchange Core with sharded risk engines and matching engines
    // Symbols are automatically added to matching engines during initialization
    let (core_engine, producer) = CoreEngine::new(
        symbol_specs.clone(),
        journaling_processor,
        events_handler.clone(),
    );

    (core_engine, producer, events_handler)
}
