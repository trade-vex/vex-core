pub mod engine;
pub mod events;

use std::sync::Arc;

use common::model::symbol_specification::CoreSymbolSpecification;
use hashbrown::HashMap;
use orderbook::OrderBookImplType;
use processors::journaling::JournalingProcessor;

use crate::{
    engine::{CoreEngine, Producer},
    events::SimpleEventsHandler,
};

/// Sets up the entire Exchange Core application with all processors.
///
/// This creates the core engine and adds symbols dynamically
pub fn init_exchange() -> (CoreEngine, Producer, Arc<SimpleEventsHandler>) {
    // Create symbol_id specifications for the risk engine
    // This is placeholder , remove this from here , it comes as an input to the init_exchange
    // function which takes in the symbol specs from vex_config
    // TODO : 1. Update vex_config to load symbol symbol specs from toml , yml for something similar
    //           and desearilze it into CoreSymbolSpecification struct
    //        2. Update main.rs to provide the symbol spec as an input to the init_exchange function
    //        3. Update lib.rs accordingly and use those symbol specs to build core engine
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
    let (core_engine, producer) =
        CoreEngine::new(symbol_specs, journaling_processor, events_handler.clone());

    core_engine.add_symbol(0, OrderBookImplType::Naive);

    (core_engine, producer, events_handler)
}
