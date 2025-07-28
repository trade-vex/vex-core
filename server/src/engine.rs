use crate::events::EventsHandler;
use common::cmd::OrderCommand;
use common::model::symbol_specification::CoreSymbolSpecification;
use disruptor::{
    BusySpin, MultiConsumerBarrier, MultiProducer, ProcessorSettings, build_multi_producer,
};
use hashbrown::HashMap;
use processors::{
    journaling::JournalingProcessor, matching_engine::MatchingEngineRouter, risk_engine::RiskEngine,
};
use std::sync::Arc;
use std::sync::Mutex;
use tracing::{info, warn};

// Import macros from utils crate
use utils::{create_matching_handler, create_risk_handler};

type ProducerType = MultiProducer<OrderCommand, MultiConsumerBarrier>;

/// This follows the exact same architecture as the  ExchangeCore:
/// 1. Multiple parallel Risk Engines (R1) for risk hold/pre-processing
/// 2. Multiple parallel Matching Engines for order processing  
/// 3. Risk Engine release (R2) for settlement (embedded in matching engine events)
///
/// Each processor runs on its own dedicated thread/core.
pub struct CoreEngine {
    /// Sharded risk engines for parallel risk processing
    /// Each shard handles users based on uid % num_shards
    risk_engines: Arc<Vec<Mutex<RiskEngine>>>,

    /// Sharded matching engine routers for parallel order processing
    /// Each shard handles symbols based on symbol_id % num_shards
    matching_engine_routers: Vec<Arc<Mutex<MatchingEngineRouter>>>,
}

impl CoreEngine {
    /// Creates a new CoreEngine with the exact same architecture as Java ExchangeCore
    ///
    /// Architecture:
    /// ```
    /// [Publishers]
    ///      ↓
    /// [Disruptor Ring Buffer]
    ///      ↓
    /// [Journaling] (Core 1)
    ///      ↓
    /// [Risk Engine R1] (Cores 2-5) - 4 parallel shards for risk hold
    ///      ↓
    /// [Matching Engine] (Cores 6-9) - 4 parallel shards for order processing
    ///      ↓
    /// [Risk Engine R2] (embedded in matching engine event processing)
    ///      ↓
    /// [Event Handlers] (market data, notifications, etc.)
    /// ```
    pub fn new(
        symbol_specs: HashMap<i32, CoreSymbolSpecification>,
        journaling_processor: JournalingProcessor,
        events_handler: Arc<dyn EventsHandler>,
    ) -> (Self, ProducerType) {
        let factory = || OrderCommand::default();
        let buffer_size = 1024; // Power of 2 for disruptor efficiency

        let journaling_arc = Arc::new(journaling_processor);
        let events_handler_arc = events_handler.clone();

        // Create 4 sharded risk engines for parallel risk processing
        // Power of 2 sharding enables efficient bitwise operations: uid & shard_mask
        let num_risk_engines = 4;
        let mut risk_engines = Vec::new();

        for shard_id in 0..num_risk_engines {
            let risk_engine = RiskEngine::new(symbol_specs.clone(), shard_id, num_risk_engines);
            risk_engines.push(risk_engine);
        }

        let risk_engines_arc: Arc<Vec<Mutex<RiskEngine>>> =
            Arc::new(risk_engines.into_iter().map(Mutex::new).collect());

        // Create 4 sharded matching engine routers for parallel order processing
        // Each router handles a subset of symbols: symbol_id & shard_mask
        let num_matching_engines = 4;
        let mut matching_engine_routers = Vec::new();

        for shard_id in 0..num_matching_engines {
            let router = MatchingEngineRouter::new(shard_id, num_matching_engines as i64);
            matching_engine_routers.push(Arc::new(std::sync::Mutex::new(router)));
        }

        // Create journaling handler for audit trail and recovery
        let journaling_handler = {
            let journaling_clone = journaling_arc.clone();
            move |cmd: &OrderCommand, _sequence: i64, _end_of_batch: bool| {
                journaling_clone.journal_command(cmd);
            }
        };

        // Create 4 separate risk engine R1 handlers using macro
        // Each handler runs on its own thread/core
        let risk_r1_handler_0 = create_risk_handler!(0, risk_engines_arc);
        let risk_r1_handler_1 = create_risk_handler!(1, risk_engines_arc);
        let risk_r1_handler_2 = create_risk_handler!(2, risk_engines_arc);
        let risk_r1_handler_3 = create_risk_handler!(3, risk_engines_arc);

        // Create 4 separate matching engine handlers using macro
        // Each handler runs on its own thread/core
        let matching_handler_0 = create_matching_handler!(
            0,
            matching_engine_routers,
            events_handler_arc,
            journaling_arc,
            risk_engines_arc
        );
        let matching_handler_1 = create_matching_handler!(
            1,
            matching_engine_routers,
            events_handler_arc,
            journaling_arc,
            risk_engines_arc
        );
        let matching_handler_2 = create_matching_handler!(
            2,
            matching_engine_routers,
            events_handler_arc,
            journaling_arc,
            risk_engines_arc
        );
        let matching_handler_3 = create_matching_handler!(
            3,
            matching_engine_routers,
            events_handler_arc,
            journaling_arc,
            risk_engines_arc
        );

        // Build the disruptor pipeline
        // This creates the same dependency graph and parallelism as exchangeCore
        let producer = build_multi_producer(buffer_size, factory, BusySpin)
            // Stage 1: Journaling (equivalent to afterGrouping.handleEventsWith(jh))
            .pin_at_core(1)
            .handle_events_with(journaling_handler)
            // Stage 2: Risk Engine R1 - 4 parallel handlers (equivalent to riskEngines.forEach)
            // Each handler processes ALL events but filters internally based on user ID
            .pin_at_core(2)
            .handle_events_with(risk_r1_handler_0)
            .pin_at_core(3)
            .handle_events_with(risk_r1_handler_1)
            .pin_at_core(4)
            .handle_events_with(risk_r1_handler_2)
            .pin_at_core(5)
            .handle_events_with(risk_r1_handler_3)
            .and_then() // Creates dependency: matching engines wait for risk engines
            // Stage 3: Matching Engine - 4 parallel handlers (equivalent to Java's disruptor.after(procR1))
            // Each handler processes ALL events but filters internally based on symbol ID
            .pin_at_core(6)
            .handle_events_with(matching_handler_0)
            .pin_at_core(7)
            .handle_events_with(matching_handler_1)
            .pin_at_core(8)
            .handle_events_with(matching_handler_2)
            .pin_at_core(9)
            .handle_events_with(matching_handler_3)
            .build();

        let engine = Self {
            risk_engines: risk_engines_arc.clone(),
            matching_engine_routers,
        };

        info!("  CoreEngine initialized ");
        info!("  - 4 parallel Risk Engines (R1) on cores 2-5");
        info!("  - 4 parallel Matching Engines on cores 6-9");
        info!("  - Journaling on core 1");
        info!("  - Risk Engine R2 embedded in matching engine event processing");

        (engine, producer)
    }

    /// Adds a symbol to the appropriate matching engine shard
    ///
    /// Uses the same sharding logic as runtime processing: symbol_id & shard_mask
    /// This ensures symbols are distributed evenly across matching engine shards
    /// for optimal load balancing and memory usage.
    pub fn add_symbol(&self, symbol_id: i32, book_type: orderbook::OrderBookImplType) {
        // Calculate which matching engine shard owns this symbol
        let num_shards = self.matching_engine_routers.len() as i64;
        let shard_mask = num_shards - 1; // Power of 2 mask for efficient bitwise operations
        let owner_shard_id = (symbol_id as i64) & shard_mask;
        let router_index = owner_shard_id as usize;

        // Add symbol only to the owning shard for memory efficiency
        if let Some(router) = self.matching_engine_routers.get(router_index) {
            let mut matching_engine = router.lock().unwrap();
            matching_engine.add_symbol(symbol_id, book_type);

            info!(
                "Added symbol {} to MatchingEngine shard {} (owner_shard_id={})",
                symbol_id, router_index, owner_shard_id
            );
        } else {
            warn!(
                "Failed to add symbol {}: router index {} out of bounds",
                symbol_id, router_index
            );
        }
    }

    // The API handling logic will be removed and implemented in the gateway , currently placegolder for testing

    /// Gets user balance from the appropriate risk engine shard
    ///
    /// Routes the query to the correct risk engine shard based on user ID.
    /// This ensures consistent access to user data regardless of which
    /// risk engine shard is currently holding the user's state.
    pub fn get_user_balance(&self, uid: u64, currency: i32) -> Option<i64> {
        // Route to the correct risk engine shard using the same logic as processing
        let uid_i64 = uid as i64;
        let num_shards = self.risk_engines.len() as i64;
        let shard_mask = num_shards - 1; // Power of 2 mask
        let risk_engine_index = (uid_i64 & shard_mask) as usize;

        if let Some(risk_engine_mutex) = self.risk_engines.get(risk_engine_index) {
            let risk_engine = risk_engine_mutex.lock().unwrap();
            if let Some(balance) = risk_engine
                .user_profiles
                .get(&uid_i64)
                .and_then(|profile| profile.accounts.get(&currency).copied())
            {
                return Some(balance);
            }
        }
        None
    }

    /// Gets order fill quantity from the appropriate matching engine shard
    ///
    /// Searches across all matching engine shards to find the order.
    /// In a production system, this could be optimized by routing to the
    /// correct shard based on symbol_id, but searching all shards is safer
    /// for now since we don't store the symbol_id with the order_id.
    pub fn get_order_filled(&self, order_id: i64) -> Option<i64> {
        // Search across all matching engine router shards
        for (shard_id, router) in self.matching_engine_routers.iter().enumerate() {
            let matching_engine = router.lock().unwrap();

            // Search all order books in this shard
            for order_book in matching_engine.get_order_books().values() {
                if let Some(order) = order_book.get_order_by_id(order_id) {
                    info!(
                        "Found order {} in MatchingEngine shard {}",
                        order_id, shard_id
                    );
                    return Some(order.filled());
                }
            }
        }
        None
    }
}
