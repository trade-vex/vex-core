use crate::{
    create_event_handler, create_matching_handler, create_risk_handler, create_risk_r2_handler,
};
use common::cmd::{OrderCommand, ProcessedOrderCommand, Status};
use common::model::market_specification::CoreMarketSpecification;
use common::Side;
use disruptor::{
    BusySpin, MultiConsumerBarrier, MultiProducer, ProcessorSettings, build_multi_producer,
};
use hashbrown::HashMap;
use parking_lot::Mutex;
use processors::{
    journaling::JournalingProcessor, matching_engine::MatchingEngineRouter, risk_engine::RiskEngine, events::EventsHandler
};
use std::sync::Arc;
use std::thread;
use tracing::{info, warn};
use vex_config::CoreNetworkingConfig;
use vex_networking::server::VexCoreServer;

pub type OrderProducer = MultiProducer<OrderCommand, MultiConsumerBarrier>;
pub type ProcessedOrderProducer = MultiProducer<ProcessedOrderCommand, MultiConsumerBarrier>;

/// This follows the exact same architecture as the  ExchangeCore:
/// 1. Multiple parallel Risk Engines (R1) for risk hold/pre-processing
/// 2. Multiple parallel Matching Engines for order processing  
/// 3. Risk Engine release (R2) for settlement (embedded in matching engine events)
///
/// Each processor runs on its own dedicated thread/core.
pub struct CoreEngine {}

impl CoreEngine {
    /// Creates a new CoreEngine
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
        symbol_specs: HashMap<u32, CoreMarketSpecification>,
        journaling_processor: JournalingProcessor,
        events_handler: Arc<dyn EventsHandler>,
    ) -> (Self, OrderProducer) {
        let order_factory = || OrderCommand::default();
        let matcher_event_factory = || ProcessedOrderCommand::new(Status::Rejected, 0, 0, Side::Ask);
        let buffer_size = 1024; // Power of 2 for disruptor efficiency

        // Using Arc to share stateful processors with the main thread and the consumer threads.
        let journaling_arc = Arc::new(journaling_processor);
        // Create journaling handler for audit trail and recovery
        let journaling_handler = {
            let journaling_clone = journaling_arc.clone();
            move |cmd: &OrderCommand, _sequence: i64, _end_of_batch: bool| {
                journaling_clone.journal_command(cmd);
            }
        };
        let events_handler_arc = events_handler.clone();

        // Create 4 sharded risk engines for parallel risk processing
        // Power of 2 sharding enables efficient bitwise operations: user_id & shard_mask
        let num_risk_engines = 4;
        let mut risk_engines = Vec::new();

        for shard_id in 0..num_risk_engines {
            let mut risk_engine = RiskEngine::new(symbol_specs.clone(), shard_id, num_risk_engines);
            
            // Add initial user profiles to this shard with funded accounts
            for user_id in [100, 101] {
                let user_id = user_id as u64;
                let shard_mask = (num_risk_engines - 1) as u64;
                
                // Only add users that belong to this shard
                if (user_id & shard_mask) == shard_id as u64 {
                    let mut user_profile = common::model::user_profile::UserProfile::new(
                        user_id, 
                        common::model::user_profile::UserStatus::Active
                    );
                    
                    // Fund the user with initial balances
                    user_profile.accounts.insert(1, 1000000); // 1M base currency
                    user_profile.accounts.insert(2, 1000000); // 1M quote currency
                    
                    risk_engine.user_profiles.insert(user_id, user_profile);
                    
                    info!("Added user {} to RiskEngine shard {} with initial balances", user_id, shard_id);
                }
            }
            
            risk_engines.push(risk_engine);
        }

        let risk_engines_arc: Arc<Vec<Mutex<RiskEngine>>> =
            Arc::new(risk_engines.into_iter().map(Mutex::new).collect());

        // Create 4 sharded matching engine routers for parallel order processing
        // Each router handles a subset of symbols: symbol_id & shard_mask
        let num_matching_engines = 4;
        let mut matching_engine_routers = Vec::new();

        for shard_id in 0..num_matching_engines {
            let router = MatchingEngineRouter::new(shard_id, num_matching_engines as u64);
            matching_engine_routers.push(Arc::new(Mutex::new(router)));
        }

        // Add all symbols to the appropriate matching engine shards
        for &symbol_id in symbol_specs.keys() {
            let shard_mask = (num_matching_engines - 1) as u64;
            let owner_shard_id = (symbol_id as u64) & shard_mask;
            let router_index = owner_shard_id as usize;

            if let Some(router) = matching_engine_routers.get(router_index) {
                let mut matching_engine = router.lock();
                matching_engine.add_market(symbol_id);

                info!(
                    "Added symbol_id {} to MatchingEngine shard {} during initialization",
                    symbol_id, router_index
                );
            }
        }



        // Build the second ring buffer first (the producer of this is required as an input in matching_engine_royter handler)
        let matcher_event_producer =
            build_multi_producer(buffer_size, matcher_event_factory, BusySpin)
                // Stage 1: Journaling for raw events
                .pin_at_core(10)
                .handle_events_with({
                    let journaling_clone = journaling_arc.clone();
                    move |processed_cmd: &ProcessedOrderCommand, _sequence: i64, _end_of_batch: bool| {
                        journaling_clone.journal_event(processed_cmd);
                    }
                })
                // Stage 2: Risk Engine R2 - 4 parallel handlers
                .pin_at_core(11)
                .handle_events_with(create_risk_r2_handler!(0, risk_engines_arc))
                .pin_at_core(12)
                .handle_events_with(create_risk_r2_handler!(1, risk_engines_arc))
                .pin_at_core(13)
                .handle_events_with(create_risk_r2_handler!(2, risk_engines_arc))
                .pin_at_core(14)
                .handle_events_with(create_risk_r2_handler!(3, risk_engines_arc))
                .and_then() // Creates dependency: event handlers wait for risk engines
                // Stage 3: Event Handlers
                .pin_at_core(15)
                .handle_events_with(create_event_handler!(events_handler_arc))
                .build();

        // Build the disruptor pipeline
        // This creates the same dependency graph and parallelism as exchangeCore
        let producer = build_multi_producer(buffer_size, order_factory, BusySpin)
            // Stage 1: Journaling
            .pin_at_core(1)
            .handle_events_with(move |cmd: &OrderCommand, _, _| {
                journaling_arc_stage1.journal_command(cmd);
            })
            // Stage 2: Risk Engine on core 2
            .pin_at_core(2)
            .handle_events_with(create_risk_handler!(0, risk_engines_arc))
            .pin_at_core(3)
            .handle_events_with(create_risk_handler!(1, risk_engines_arc))
            .pin_at_core(4)
            .handle_events_with(create_risk_handler!(2, risk_engines_arc))
            .pin_at_core(5)
            .handle_events_with(create_risk_handler!(3, risk_engines_arc))
            .and_then() // Creates dependency: matching engines wait for risk engines
            // Stage 3: Matching Engine - 4 parallel handlers
            // Each handler processes ALL events but filters internally based on symbol_id ID
            .pin_at_core(6)
            .handle_events_with(create_matching_handler!(0, matching_engine_routers, matcher_event_producer))
            .pin_at_core(7)
            .handle_events_with(create_matching_handler!(1, matching_engine_routers, matcher_event_producer))
            .pin_at_core(8)
            .handle_events_with(create_matching_handler!(2, matching_engine_routers, matcher_event_producer))
            .pin_at_core(9)
            .handle_events_with(create_matching_handler!(3, matching_engine_routers, matcher_event_producer))
            .build();

        let engine = Self {};
        (engine, producer)
    }

    /// Run Starts the Networking. 2 Processes Starts
    /// 1. Listens for New Gateway Clients  
    /// 2. Listens for OrderCommands from Gateways
    pub fn run(&mut self, producer: OrderProducer, networking_config: CoreNetworkingConfig) {
        // Start the disruptor ring buffer processing
        // This will block and process events in parallel across all handlers
        let _ = thread::spawn(move || {
            let mut core_server = VexCoreServer::new(networking_config, producer)
                .expect("Failed to create VexCoreServer");
            match core_server.start() {
                Ok(()) => println!("Server run() completed successfully (unexpected)"),
                Err(e) => println!("Server run() error: {e}"),
            }
        });
    }
}
