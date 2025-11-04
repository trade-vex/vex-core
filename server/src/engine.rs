use crate::{create_event_handler, create_risk_handler, create_risk_r2_handler};
use common::CoreMarketSpecification;
use common::OrderCommand;
use common::PriceCache;
use common::Status;
use common::TimeInForce;
use common::{base_asset, quote_asset};
use disruptor::{
    BusySpin, MultiProducer, ProcessorSettings, SingleConsumerBarrier, build_multi_producer,
};
use hashbrown::HashMap;
use processors::{
    events::EventsHandler, journaling::JournalingProcessor, matching_engine::MatchingEngineRouter,
    risk_engine::RiskEngine,
};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use tracing::info;
use vex_config::CoreNetworkingConfig;
use vex_networking::server::GatewayPublications;
use vex_networking::server::VexCoreServer;

pub type OrderProducer = MultiProducer<OrderCommand, SingleConsumerBarrier>;

/// This follows the exact same architecture as the  ExchangeCore:
/// 1. Multiple parallel Risk Engines (R1) for risk hold/pre-processing
/// 2. Multiple parallel Matching Engines for order processing  
/// 3. Risk Engine release (R2) for settlement (embedded in matching engine events)
///
/// Each processor runs on its own dedicated thread/core.
pub struct CoreEngine {
    /// Gateway Publications for sending responses back to gateways
    publications: Arc<GatewayPublications>,
}

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
        mut journaling_processor: JournalingProcessor,
        events_handler: Arc<dyn EventsHandler>,
        publications: Arc<GatewayPublications>,
        #[cfg(test)] test_handler: impl 'static + Send + FnMut(&mut OrderCommand, i64, bool),
    ) -> (Self, OrderProducer, Option<Arc<Vec<RiskEngine>>>) {
        // Setup PriceCache
        let price_cache = Arc::new(PriceCache::new(symbol_specs.keys()));

        let order_factory = || OrderCommand::default();
        let buffer_size = 1024; // Power of 2 for disruptor efficiency

        // Create journaling handler for audit trail and recovery
        let journaling_handler = {
            move |cmd: &mut OrderCommand, _sequence: i64, _end_of_batch: bool| {
                journaling_processor.journal_command(cmd);
            }
        };
        let events_handler_arc = events_handler.clone();

        // Create 4 sharded risk engines for parallel risk processing
        // Power of 2 sharding enables efficient bitwise operations: user_id & shard_mask
        let num_risk_engines = 4;
        let mut risk_engines = Vec::new();

        for shard_id in 0..num_risk_engines {
            let risk_engine = RiskEngine::new(symbol_specs.clone(), shard_id, num_risk_engines);
            risk_engines.push(risk_engine);
        }

        let risk_engines_arc: Arc<Vec<RiskEngine>> = Arc::new(risk_engines.into_iter().collect());

        // Create 4 sharded matching engine routers for parallel order processing
        // Each router handles a subset of symbols: symbol_id & shard_mask
        let num_matching_engines = 4;
        let mut matching_engine_routers = Vec::new();

        for shard_id in 0..num_matching_engines {
            let router = MatchingEngineRouter::new(shard_id, num_matching_engines as u64);
            matching_engine_routers.push(router);
        }

        // Add all symbols to the appropriate matching engine shards
        for &symbol_id in symbol_specs.keys() {
            let shard_mask = (num_matching_engines - 1) as u64;
            let owner_shard_id = (symbol_id as u64) & shard_mask;
            let router_index = owner_shard_id as usize;

            if let Some(router) = matching_engine_routers.get_mut(router_index) {
                router.add_market(symbol_id);

                info!(
                    "Added symbol_id {} to MatchingEngine shard {} during initialization",
                    symbol_id, router_index
                );
            }
        }

        // Phase 3: take routers out of the Vec and move into closures
        let mut iter = matching_engine_routers.into_iter();

        let mut router_handlers_iter = (0..num_matching_engines).map(|_| {
            let mut router = iter.next().unwrap();
            let price_cache = price_cache.clone();
            move |cmd: &mut OrderCommand, _sequence: i64, _end_of_batch: bool| {
                router.process_order(cmd, price_cache.clone());
            }
        });

        // Build the disruptor pipeline
        // This creates the same dependency graph and parallelism as exchangeCore
        let producer = build_multi_producer(buffer_size, order_factory, BusySpin)
            // Stage 1: Journaling
            // .pin_at_core(1)
            .handle_events_with(journaling_handler)
            // Stage 2: Risk Engine R1 - 4 parallel handlers (equivalent to riskEngines.forEach)
            // Each handler processes ALL events but filters internally based on user ID
            // .pin_at_core(2)
            .handle_events_with(create_risk_handler!(0, risk_engines_arc, price_cache))
            // .pin_at_core(3)
            .handle_events_with(create_risk_handler!(1, risk_engines_arc, price_cache))
            // .pin_at_core(4)
            .handle_events_with(create_risk_handler!(2, risk_engines_arc, price_cache))
            // .pin_at_core(5)
            .handle_events_with(create_risk_handler!(3, risk_engines_arc, price_cache))
            .and_then() // Creates dependency: matching engines wait for risk engines
            // Stage 3: Matching Engine - 4 parallel handlers
            // Each handler processes ALL events but filters internally based on symbol_id ID
            // .pin_at_core(6)
            .handle_events_with(router_handlers_iter.next().unwrap())
            // .pin_at_core(7)
            .handle_events_with(router_handlers_iter.next().unwrap())
            // .pin_at_core(8)
            .handle_events_with(router_handlers_iter.next().unwrap())
            // .pin_at_core(9)
            .handle_events_with(router_handlers_iter.next().unwrap())
            .and_then()
            // .pin_at_core(10)
            .handle_events_with(create_risk_r2_handler!(0, risk_engines_arc))
            // .pin_at_core(11)
            .handle_events_with(create_risk_r2_handler!(1, risk_engines_arc))
            // .pin_at_core(12)
            .handle_events_with(create_risk_r2_handler!(2, risk_engines_arc))
            // .pin_at_core(13)
            .handle_events_with(create_risk_r2_handler!(3, risk_engines_arc))
            .and_then() // Creates dependency: event handlers wait for risk engines
            // Stage 3: Event Handlers
            // .pin_at_core(14)
            .handle_events_with(create_event_handler!(events_handler_arc));

        // Optional test handler for unit tests
        #[cfg(test)]
        {
            let producer = producer
                .and_then()
                .pin_at_core(15)
                .handle_events_with(test_handler)
                .build();

            return (Self { publications }, producer, Some(risk_engines_arc));
        }
        #[allow(unreachable_code)]
        let producer = producer.build();

        let engine = Self { publications };
        (engine, producer, Some(risk_engines_arc))
    }

    /// Run Starts the Networking. 2 Processes Starts
    /// 1. Listens for New Gateway Clients  
    /// 2. Listens for OrderCommands from Gateways
    pub fn run(
        &mut self,
        producer: OrderProducer,
        networking_config: CoreNetworkingConfig,
    ) -> JoinHandle<()> {
        // Start the disruptor ring buffer processing
        // This will block and process events in parallel across all handlers
        let publications = self.publications.clone();
        thread::spawn(move || {
            let mut core_server = VexCoreServer::new(networking_config, producer, publications)
                .expect("Failed to create VexCoreServer");
            match core_server.start() {
                Ok(()) => println!("Server run() completed successfully (unexpected)"),
                Err(e) => println!("Server run() error: {e}"),
            }
        })
    }
}
