use crate::events::EventsHandler;
use common::cmd::OrderCommand;
use disruptor::BusySpin;
use disruptor::ProcessorSettings;
use processors::{
    journaling::JournalingProcessor, matching_engine::MatchingEngineRouter, risk_engine::RiskEngine,
};
use std::sync::Arc;
use tracing::{info, warn};
/// The central processing unit of the exchange, running a sequential pipeline.
/// This is the equivalent of `CoreEngine.java` orchestrating the processors.
pub struct CoreEngine {
    /// Sharded risk engines for parallel risk processing
    /// Each shard handles users based on user_id % num_shards
    risk_engines: Arc<Vec<Mutex<RiskEngine>>>,

    /// Sharded matching engine routers for parallel order processing
    /// Each shard handles symbols based on symbol_id % num_shards
    matching_engine_routers: Vec<Arc<Mutex<MatchingEngineRouter>>>,
}

impl CoreEngine {
    pub fn new(
        symbol_specs: HashMap<u32, CoreSymbolSpecification>,
        journaling_processor: JournalingProcessor,
        events_handler: Arc<dyn EventsHandler>,
    ) -> (
        Self,
        disruptor::SingleProducer<OrderCommand, disruptor::MultiConsumerBarrier>,
    ) {
        let factory = || OrderCommand::default();
        let buffer_size = 1024;

        // Using Arc to share stateful processors with the main thread and the consumer threads.
        let journaling_arc = Arc::new(journaling_processor);
        let risk_engine_arc = Arc::new(std::sync::Mutex::new(risk_engine));
        let matching_engine_arc = Arc::new(std::sync::Mutex::new(matching_engine_router));
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
            .handle_events_with(move |cmd: &OrderCommand, _, _| {
                journaling_arc_stage1.journal_command(cmd);
            })
            // Stage 2: Risk Engine on core 2
            .pin_at_core(2)
            .handle_events_with({
                let risk_engine_clone = risk_engine_arc.clone();
                move |cmd: &OrderCommand, _, _| {
                    let mut risk_engine = risk_engine_clone.lock().unwrap();
                    let mut cmd_clone = cmd.clone();
                    if let Err(e) = risk_engine.pre_process_command(&mut cmd_clone) {
                        warn!(
                            "[Disruptor Core] Risk check failed: {:?}. Rejecting command.",
                            e
                        );
                    }
                }
            })
            // Stage 3: Matching Engine + event handling on core 3
            .pin_at_core(3)
            .handle_events_with(risk_r1_handler_1)
            .pin_at_core(4)
            .handle_events_with(risk_r1_handler_2)
            .pin_at_core(5)
            .handle_events_with(risk_r1_handler_3)
            .and_then() // Creates dependency: matching engines wait for risk engines
            // Stage 3: Matching Engine - 4 parallel handlers (equivalent to Java's disruptor.after(procR1))
            // Each handler processes ALL events but filters internally based on symbol_id ID
            .pin_at_core(6)
            .handle_events_with(matching_handler_0)
            .pin_at_core(7)
            .handle_events_with(matching_handler_1)
            .pin_at_core(8)
            .handle_events_with(matching_handler_2)
            .pin_at_core(9)
            .handle_events_with(matching_handler_3)
            .build();

        let mut engine = Self {
            _producer: Some(producer),
        };

        // Take the producer out of the engine for returning
        let producer = engine._producer.take().unwrap();
        // (engine, producer)
        (engine, producer)
    }

    /// Run Starts the Networking. 2 Processes Starts
    /// 1. Listens for New Gateway Clients  
    /// 2. Listens for OrderCommands from Gateways
    pub fn run(&mut self, produder: Producer, networking_config: CoreNetworkingConfig) {
        // Start the disruptor ring buffer processing
        // This will block and process events in parallel across all handlers
        let _ = thread::spawn(move || {
            let mut core_server =
            VexCoreServer::new(networking_config, produder)
                .expect("Failed to create VexCoreServer");
            match core_server.start() {
                Ok(()) => println!("Server run() completed successfully (unexpected)"),
                Err(e) => println!("Server run() error: {e}"),
            }
        });
    
    }

    /// Adds a symbol_id to the appropriate matching engine shard
    ///
    /// Uses the same sharding logic as runtime processing: symbol_id & shard_mask
    /// This ensures symbols are distributed evenly across matching engine shards
    /// for optimal load balancing and memory usage.
    pub fn add_symbol(&self, symbol_id: u32, spec: CoreSymbolSpecification, book_type: orderbook::OrderBookImplType) {
        // Calculate which matching engine shard owns this symbol_id
        let num_shards = self.matching_engine_routers.len() as u64;
        let shard_mask = num_shards - 1; // Power of 2 mask for efficient bitwise operations
        let owner_shard_id = (symbol_id as u64) & shard_mask;
        let router_index = owner_shard_id as usize;

        // Add symbol_id only to the owning shard for memory efficiency
        if let Some(router) = self.matching_engine_routers.get(router_index) {
            let mut matching_engine = router.lock().unwrap();
            matching_engine.add_symbol(symbol_id, spec, book_type);

            info!(
                "Added symbol_id {} to MatchingEngine shard {} (owner_shard_id={})",
                symbol_id, router_index, owner_shard_id
            );
        } else {
            warn!(
                "Failed to add symbol_id {}: router index {} out of bounds",
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
    pub fn get_user_balance(&self, user_id: u64, currency: u32) -> Option<u64> {
        // Route to the correct risk engine shard using the same logic as processing
        let num_shards = self.risk_engines.len() as u64;
        let shard_mask = num_shards - 1; // Power of 2 mask
        let risk_engine_index = (user_id & shard_mask) as usize;

        if let Some(risk_engine_mutex) = self.risk_engines.get(risk_engine_index) {
            let risk_engine = risk_engine_mutex.lock().unwrap();
            if let Some(balance) = risk_engine
                .user_profiles
                .get(&user_id)
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
    pub fn get_order_filled(&self, order_id: u64) -> Option<u64> {
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
