pub mod engine;
pub mod utils;

use std::sync::Arc;

use common::model::symbol_specification::CoreSymbolSpecification;
use hashbrown::HashMap;
use processors::journaling::JournalingProcessor;
use processors::events::SimpleEventsHandler;

use crate::{
    engine::{CoreEngine, Producer},
    events::SimpleEventsHandler,
};

/// Sets up the entire Exchange Core application with all processors.
///
/// This creates the core engine and adds symbols dynamically
pub fn init_exchange() -> (CoreEngine, Producer, Arc<SimpleEventsHandler>) {
    // Create symbol_id specifications for the risk engine
    let mut symbol_specs = HashMap::new();
    let mut spec = CoreSymbolSpecification::default();
    spec.base_currency = 1; // BTC
    spec.quote_currency = 2; // USD
    symbol_specs.insert(0, spec);

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
