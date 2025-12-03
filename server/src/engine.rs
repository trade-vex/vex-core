use crate::{create_risk_handler, create_risk_r2_handler};
use common::{
    CoreMarketSpecification, OrderCommand, OrderCommandType, PriceCache, Status, TimeInForce,
    base_asset, quote_asset,
};
use disruptor::{
    BusySpin, MultiProducer, ProcessorSettings, SingleConsumerBarrier, build_multi_producer,
};
use hashbrown::HashMap;
use processors::{
    events::EventsHandler,
    journaling::{JournalingProcessor, ReplayControl},
    matching_engine::MatchingEngineRouter,
    risk_engine::RiskEngine,
};
use std::{
    sync::Arc,
    sync::atomic::AtomicBool,
    thread::{self, JoinHandle},
};
use tracing::info;
use vex_config::CoreNetworkingConfig;
use vex_networking::server::Publications;
use vex_networking::server::VexCoreServer;

/// Type alias for the order command producer
pub type OrderProducer = MultiProducer<OrderCommand, SingleConsumerBarrier>;

#[derive(Clone)]
pub struct ReplayContext {
    pub producer: OrderProducer,
    pub control: ReplayControl,
}

/// Type alias for a shared reference to the risk engines
pub type RiskEngines = Arc<Vec<RiskEngine>>;

/// Result type for engine operations
pub type EngineResult<T> = Result<T, EngineError>;

/// Errors that can occur during engine initialization and operation
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// Error during server initialization
    #[error("Failed to initialize server: {0}")]
    ServerInitialization(String),

    /// Error during server execution
    #[error("Server runtime error: {0}")]
    ServerRuntime(String),

    /// Configuration error
    #[error("Invalid configuration: {0}")]
    Configuration(String),
}

/// Configuration constants for the core engine
#[cfg(not(feature = "test-config"))]
mod config {
    /// Number of risk engine shards for parallel processing
    pub const NUM_RISK_ENGINES: usize = 4;
    /// Number of matching engine shards for parallel processing
    pub const NUM_MATCHING_ENGINES: usize = 4;
    /// Size of the disruptor ring buffer (must be power of 2)
    pub const BUFFER_SIZE: usize = 1024;
}

/// Test configuration with smaller values for faster testing
#[cfg(feature = "test-config")]
mod config {
    pub const NUM_RISK_ENGINES: usize = 4;
    pub const NUM_MATCHING_ENGINES: usize = 4;
    pub const BUFFER_SIZE: usize = 256;
}

use config::*;

/// Core pinning configuration for processor threads
#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "test-config", allow(dead_code))]
pub struct CorePinning {
    journaling: usize,
    risk_engines: [usize; NUM_RISK_ENGINES],
    matching_engines: [usize; NUM_MATCHING_ENGINES],
    risk_r2_engines: [usize; NUM_RISK_ENGINES],
    events: usize,
}

impl Default for CorePinning {
    fn default() -> Self {
        Self {
            journaling: 1,
            risk_engines: [2, 3, 4, 5],
            matching_engines: [6, 7, 8, 9],
            risk_r2_engines: [10, 11, 12, 13],
            events: 14,
        }
    }
}

/// High-performance exchange core engine
///
/// This follows the exact same architecture as ExchangeCore:
/// 1. Multiple parallel Risk Engines (R1) for risk hold/pre-processing
/// 2. Multiple parallel Matching Engines for order processing
/// 3. Risk Engine release (R2) for settlement (embedded in matching engine events)
///
/// Each processor runs on its own dedicated thread/core for maximum throughput.
///
/// # Architecture
/// ```text
/// [Publishers]
///      ↓
/// [Disruptor Ring Buffer]
///      ↓
/// [Journaling] (Core 1)
///      ↓
/// [Risk Engine R1] (Cores 2-5) - 4 parallel shards
///      ↓
/// [Matching Engine] (Cores 6-9) - 4 parallel shards
///      ↓
/// [Risk Engine R2] (Cores 10-13) - 4 parallel shards
///      ↓
/// [Event Handlers] (Core 14)
/// ```
pub struct CoreEngine {
    /// Gateway Publications for sending responses back to gateways
    publications: Arc<Publications>,
}

impl CoreEngine {
    /// Internal constructor that builds the disruptor pipeline
    ///
    /// This method creates the complete processing pipeline with all stages.
    ///
    /// # Errors
    ///
    /// Currently does not return errors, but signature allows for future error handling
    pub fn new(
        symbol_specs: HashMap<u32, CoreMarketSpecification>,
        journaling_processor: JournalingProcessor,
        events_handler: impl EventsHandler,
        publications: Arc<Publications>,
        core_pinning: CorePinning,
    ) -> EngineResult<(Self, OrderProducer)> {
        let (engine, producer) = Self::build_engine(
            symbol_specs,
            journaling_processor,
            events_handler,
            publications,
            core_pinning,
        )?;
        Ok((engine, producer))
    }

    fn build_engine(
        symbol_specs: HashMap<u32, CoreMarketSpecification>,
        mut journaling_processor: JournalingProcessor,
        events_handler: impl EventsHandler,
        publications: Arc<Publications>,
        core_pinning: CorePinning,
    ) -> EngineResult<(Self, OrderProducer)> {
        let price_cache = Arc::new(PriceCache::new(symbol_specs.keys()));

        let order_factory = OrderCommand::default;

        let journaling_handler =
            move |cmd: &mut OrderCommand, _sequence: i64, _end_of_batch: bool| {
                journaling_processor.journal_command(cmd);
            };

        let risk_engines_arc = Self::initialize_risk_engines(&symbol_specs, NUM_RISK_ENGINES);

        let matching_engine_routers =
            Self::initialize_matching_engines(&symbol_specs, NUM_MATCHING_ENGINES);

        let router_handlers = matching_engine_routers.into_iter().map(|mut router| {
            let price_cache_clone = Arc::clone(&price_cache);
            move |cmd: &mut OrderCommand, _sequence: i64, _end_of_batch: bool| {
                if cmd.command == OrderCommandType::DepositFunds
                    || cmd.command == OrderCommandType::WithdrawFunds
                    || cmd.status == Status::Rejected
                {
                    return;
                }
                router.process_order(cmd, Arc::clone(&price_cache_clone));
            }
        });

        let events_handler = move |cmd: &mut OrderCommand, _sequence: i64, _end_of_batch: bool| {
            events_handler.handle_processed_command(cmd);
        };

        // Build the disruptor pipeline with proper stage dependencies
        let producer = Self::build_disruptor_pipeline(
            BUFFER_SIZE,
            order_factory,
            journaling_handler,
            &risk_engines_arc,
            &price_cache,
            router_handlers,
            events_handler,
            core_pinning,
        );

        let engine = Self { publications };
        Ok((engine, producer))
    }

    /// Initializes risk engines with specified sharding
    fn initialize_risk_engines(
        symbol_specs: &HashMap<u32, CoreMarketSpecification>,
        num_shards: usize,
    ) -> RiskEngines {
        let risk_engines: Vec<_> = (0..num_shards)
            .map(|shard_id| {
                RiskEngine::new(symbol_specs.clone(), shard_id as u32, num_shards as u32)
            })
            .collect();

        Arc::new(risk_engines)
    }

    /// Initializes matching engines with symbol distribution
    fn initialize_matching_engines(
        symbol_specs: &HashMap<u32, CoreMarketSpecification>,
        num_shards: usize,
    ) -> Vec<MatchingEngineRouter> {
        let mut routers: Vec<_> = (0..num_shards)
            .map(|shard_id| MatchingEngineRouter::new(shard_id as u32, num_shards as u64))
            .collect();

        // Distribute symbols across shards using bitwise masking
        let shard_mask = (num_shards - 1) as u64;
        for &symbol_id in symbol_specs.keys() {
            let owner_shard_id = ((symbol_id as u64) & shard_mask) as usize;

            if let Some(router) = routers.get_mut(owner_shard_id) {
                router.add_market(symbol_id);
                info!(
                    symbol_id,
                    owner_shard_id, "Added symbol to MatchingEngine shard"
                );
            }
        }

        routers
    }

    /// Builds the complete disruptor pipeline with all processing stages
    #[allow(clippy::too_many_arguments)]
    fn build_disruptor_pipeline<X, Y, Z>(
        buffer_size: usize,
        order_factory: fn() -> OrderCommand,
        journaling_handler: X,
        risk_engines: &RiskEngines,
        price_cache: &Arc<PriceCache>,
        mut router_handlers_iter: impl Iterator<Item = Y>,
        events_handler: Z,
        core_pinning: CorePinning,
    ) -> OrderProducer
    where
        X: FnMut(&mut OrderCommand, i64, bool) + Send + 'static,
        Y: FnMut(&mut OrderCommand, i64, bool) + Send + 'static,
        Z: FnMut(&mut OrderCommand, i64, bool) + Send + 'static,
    {
        // Build the entire pipeline in one chain to maintain proper types
        build_multi_producer(buffer_size, order_factory, BusySpin)
            // Stage 1: Journaling for audit trail
            .pin_at_core(core_pinning.journaling)
            .handle_events_with(journaling_handler)
            // Stage 2: Risk Engine R1 - parallel risk hold/pre-processing
            .pin_at_core(core_pinning.risk_engines[0])
            .handle_events_with(create_risk_handler!(0, risk_engines, price_cache))
            .pin_at_core(core_pinning.risk_engines[1])
            .handle_events_with(create_risk_handler!(1, risk_engines, price_cache))
            .pin_at_core(core_pinning.risk_engines[2])
            .handle_events_with(create_risk_handler!(2, risk_engines, price_cache))
            .pin_at_core(core_pinning.risk_engines[3])
            .handle_events_with(create_risk_handler!(3, risk_engines, price_cache))
            // Dependency barrier: matching engines wait for risk engines
            .and_then()
            // Stage 3: Matching Engine - parallel order processing
            .pin_at_core(core_pinning.matching_engines[0])
            .handle_events_with(router_handlers_iter.next().expect("Missing router handler"))
            .pin_at_core(core_pinning.matching_engines[1])
            .handle_events_with(router_handlers_iter.next().expect("Missing router handler"))
            .pin_at_core(core_pinning.matching_engines[2])
            .handle_events_with(router_handlers_iter.next().expect("Missing router handler"))
            .pin_at_core(core_pinning.matching_engines[3])
            .handle_events_with(router_handlers_iter.next().expect("Missing router handler"))
            // Dependency barrier: R2 engines wait for matching
            .and_then()
            // Stage 4: Risk Engine R2 - parallel settlement processing
            .pin_at_core(core_pinning.risk_r2_engines[0])
            .handle_events_with(create_risk_r2_handler!(0, risk_engines))
            .pin_at_core(core_pinning.risk_r2_engines[1])
            .handle_events_with(create_risk_r2_handler!(1, risk_engines))
            .pin_at_core(core_pinning.risk_r2_engines[2])
            .handle_events_with(create_risk_r2_handler!(2, risk_engines))
            .pin_at_core(core_pinning.risk_r2_engines[3])
            .handle_events_with(create_risk_r2_handler!(3, risk_engines))
            // Dependency barrier: event handlers wait for settlement
            .and_then()
            // Stage 5: Event Handlers for market data and notifications
            .pin_at_core(core_pinning.events)
            .handle_events_with(events_handler)
            .build()
    }

    /// Starts the networking layer and begins processing orders
    ///
    /// This method spawns two concurrent processes:
    /// 1. Gateway connection listener - accepts new client connections
    /// 2. Order command processor - processes incoming orders from gateways
    ///
    /// # Arguments
    ///
    /// * `producer` - The disruptor producer for publishing orders
    /// * `networking_config` - Network configuration including ports and limits
    ///
    /// # Returns
    ///
    /// A `JoinHandle` that can be used to wait for server shutdown
    ///
    /// # Panics
    ///
    /// Panics if the server fails to initialize (e.g., port binding fails)
    pub fn run(
        &self,
        producer: OrderProducer,
        replay_control: ReplayControl,
        networking_config: CoreNetworkingConfig,
    ) -> (JoinHandle<Result<(), EngineError>>, Arc<AtomicBool>) {
        let publications = Arc::clone(&self.publications);
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_for_thread = Arc::clone(&shutdown_flag);

        let handle = thread::Builder::new()
            .name("vex-core-server".into())
            .spawn(move || {
                let replay = replay_control.is_enabled();

                let server_result = VexCoreServer::new(
                    networking_config,
                    producer,
                    publications,
                    replay,
                    shutdown_for_thread,
                )
                .map_err(|e| {
                    EngineError::ServerInitialization(format!(
                        "Failed to create VexCoreServer: {e}"
                    ))
                });

                replay_control.disable();

                let mut core_server = server_result?;

                core_server
                    .start()
                    .map_err(|e| EngineError::ServerRuntime(format!("Server error: {e}")))
            })
            .expect("Failed to spawn server thread");

        (handle, shutdown_flag)
    }

    /// Returns a reference to the gateway publications
    ///
    /// This can be used to send responses back to connected gateways
    #[must_use]
    pub fn publications(&self) -> &Arc<Publications> {
        &self.publications
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    /// Test-specific core pinning configuration
    /// Uses higher core numbers to avoid conflicts with system processes
    #[derive(Debug, Clone, Copy)]
    pub struct TestCorePinning {
        journaling: usize,
        risk_engines: [usize; NUM_RISK_ENGINES],
        matching_engines: [usize; NUM_MATCHING_ENGINES],
        risk_r2_engines: [usize; NUM_RISK_ENGINES],
        events: usize,
        test_handler: usize,
    }

    impl Default for TestCorePinning {
        fn default() -> Self {
            Self {
                journaling: 1,
                risk_engines: [2, 3, 4, 5],
                matching_engines: [6, 7, 8, 9],
                risk_r2_engines: [10, 11, 12, 13],
                events: 14,
                test_handler: 15,
            }
        }
    }

    /// Test-specific builder that extends CoreEngineBuilder with test functionality
    pub struct TestEngineBuilder {
        symbol_specs: Option<HashMap<u32, CoreMarketSpecification>>,
        journaling_processor: Option<JournalingProcessor>,
        events_handler: Option<Box<dyn EventsHandler>>,
        publications: Option<Arc<Publications>>,
        core_pinning: TestCorePinning,
        #[allow(clippy::type_complexity)]
        test_handler: Option<Box<dyn FnMut(&mut OrderCommand, i64, bool) + Send + 'static>>,
        risk_engines: Option<RiskEngines>,
    }

    impl TestEngineBuilder {
        /// Creates a new test builder with default configuration
        pub const fn new() -> Self {
            Self {
                symbol_specs: None,
                journaling_processor: None,
                events_handler: None,
                publications: None,
                core_pinning: TestCorePinning {
                    journaling: 1,
                    risk_engines: [2, 3, 4, 5],
                    matching_engines: [6, 7, 8, 9],
                    risk_r2_engines: [10, 11, 12, 13],
                    events: 14,
                    test_handler: 15,
                },
                test_handler: None,
                risk_engines: None,
            }
        }

        /// Sets the symbol specifications for markets
        #[must_use]
        pub fn with_symbol_specs(mut self, specs: HashMap<u32, CoreMarketSpecification>) -> Self {
            self.symbol_specs = Some(specs);
            self
        }

        /// Sets the journaling processor for audit trail
        #[must_use]
        pub fn with_journaling_processor(mut self, processor: JournalingProcessor) -> Self {
            self.journaling_processor = Some(processor);
            self
        }

        /// Sets the events handler for trade events
        #[must_use]
        pub fn with_events_handler(mut self, handler: impl EventsHandler) -> Self {
            self.events_handler = Some(Box::new(handler));
            self
        }

        /// Sets the gateway publications for client communication
        #[must_use]
        pub fn with_publications(mut self, publications: Arc<Publications>) -> Self {
            self.publications = Some(publications);
            self
        }

        /// Sets a test handler for capturing processed commands
        #[must_use]
        pub fn with_test_handler<F>(mut self, handler: F) -> Self
        where
            F: FnMut(&mut OrderCommand, i64, bool) + Send + 'static,
        {
            self.test_handler = Some(Box::new(handler));
            self
        }

        /// Sets pre-created risk engines for test access
        #[must_use]
        pub fn with_risk_engines(mut self, risk_engines: RiskEngines) -> Self {
            self.risk_engines = Some(risk_engines);
            self
        }

        /// Builds the test engine with all configured parameters
        ///
        /// # Errors
        ///
        /// Returns `EngineError::Configuration` if any required field is missing
        pub fn build(mut self) -> EngineResult<(CoreEngine, OrderProducer)> {
            let symbol_specs = self
                .symbol_specs
                .take()
                .ok_or_else(|| EngineError::Configuration("Symbol specs are required".into()))?;
            let journaling_processor = self.journaling_processor.take().ok_or_else(|| {
                EngineError::Configuration("Journaling processor is required".into())
            })?;
            let events_handler = self
                .events_handler
                .take()
                .ok_or_else(|| EngineError::Configuration("Events handler is required".into()))?;
            let publications = self
                .publications
                .take()
                .ok_or_else(|| EngineError::Configuration("Publications are required".into()))?;

            let test_handler = self.test_handler.take();
            let risk_engines = self.risk_engines.take();
            let core_pinning = self.core_pinning;

            self.build_internal(
                symbol_specs,
                journaling_processor,
                events_handler,
                publications,
                test_handler,
                risk_engines,
                core_pinning,
            )
        }

        /// Internal build method that constructs the test engine
        #[allow(clippy::too_many_arguments)]
        fn build_internal(
            self,
            symbol_specs: HashMap<u32, CoreMarketSpecification>,
            mut journaling_processor: JournalingProcessor,
            events_handler: Box<dyn EventsHandler>,
            publications: Arc<Publications>,
            test_handler: Option<Box<dyn FnMut(&mut OrderCommand, i64, bool) + Send + 'static>>,
            risk_engines: Option<RiskEngines>,
            core_pinning: TestCorePinning,
        ) -> EngineResult<(CoreEngine, OrderProducer)> {
            let price_cache = Arc::new(PriceCache::new(symbol_specs.keys()));
            let order_factory = OrderCommand::default;

            let journaling_handler =
                move |cmd: &mut OrderCommand, _sequence: i64, _end_of_batch: bool| {
                    journaling_processor.journal_command(cmd);
                };

            // Use provided risk engines or create new ones
            let risk_engines_arc = risk_engines.unwrap_or_else(|| {
                CoreEngine::initialize_risk_engines(&symbol_specs, NUM_RISK_ENGINES)
            });

            let matching_engine_routers =
                CoreEngine::initialize_matching_engines(&symbol_specs, NUM_MATCHING_ENGINES);

            let router_handlers = matching_engine_routers.into_iter().map(|mut router| {
                let price_cache_clone = Arc::clone(&price_cache);
                move |cmd: &mut OrderCommand, _sequence: i64, _end_of_batch: bool| {
                    if cmd.command == OrderCommandType::DepositFunds
                        || cmd.command == OrderCommandType::WithdrawFunds
                        || cmd.status == Status::Rejected
                    {
                        return;
                    }
                    router.process_order(cmd, Arc::clone(&price_cache_clone));
                }
            });

            let mut router_handlers_iter = router_handlers.into_iter();

            let events_handler =
                move |cmd: &mut OrderCommand, _sequence: i64, _end_of_batch: bool| {
                    events_handler.handle_processed_command(cmd);
                };

            let producer = if let Some(test_handler) = test_handler {
                self.build_test_pipeline(
                    BUFFER_SIZE,
                    order_factory,
                    journaling_handler,
                    &risk_engines_arc,
                    &price_cache,
                    &mut router_handlers_iter,
                    events_handler,
                    test_handler,
                    core_pinning,
                )
            } else {
                CoreEngine::build_disruptor_pipeline(
                    BUFFER_SIZE,
                    order_factory,
                    journaling_handler,
                    &risk_engines_arc,
                    &price_cache,
                    &mut router_handlers_iter,
                    events_handler,
                    CorePinning::default(),
                )
            };

            let engine = CoreEngine { publications };
            Ok((engine, producer))
        }

        /// Builds the test-specific disruptor pipeline with test handler
        #[allow(clippy::too_many_arguments)]
        fn build_test_pipeline(
            &self,
            buffer_size: usize,
            order_factory: fn() -> OrderCommand,
            journaling_handler: impl FnMut(&mut OrderCommand, i64, bool) + Send + 'static,
            risk_engines: &RiskEngines,
            price_cache: &Arc<PriceCache>,
            router_handlers_iter: &mut impl Iterator<
                Item = impl FnMut(&mut OrderCommand, i64, bool) + Send + 'static,
            >,
            events_handler: impl FnMut(&mut OrderCommand, i64, bool) + Send + 'static,
            test_handler: Box<dyn FnMut(&mut OrderCommand, i64, bool) + Send + 'static>,
            core_pinning: TestCorePinning,
        ) -> OrderProducer {
            build_multi_producer(buffer_size, order_factory, BusySpin)
                .pin_at_core(core_pinning.journaling)
                .handle_events_with(journaling_handler)
                .pin_at_core(core_pinning.risk_engines[0])
                .handle_events_with(create_risk_handler!(0, risk_engines, price_cache))
                .pin_at_core(core_pinning.risk_engines[1])
                .handle_events_with(create_risk_handler!(1, risk_engines, price_cache))
                .pin_at_core(core_pinning.risk_engines[2])
                .handle_events_with(create_risk_handler!(2, risk_engines, price_cache))
                .pin_at_core(core_pinning.risk_engines[3])
                .handle_events_with(create_risk_handler!(3, risk_engines, price_cache))
                .and_then()
                .pin_at_core(core_pinning.matching_engines[0])
                .handle_events_with(router_handlers_iter.next().expect("Missing router handler"))
                .pin_at_core(core_pinning.matching_engines[1])
                .handle_events_with(router_handlers_iter.next().expect("Missing router handler"))
                .pin_at_core(core_pinning.matching_engines[2])
                .handle_events_with(router_handlers_iter.next().expect("Missing router handler"))
                .pin_at_core(core_pinning.matching_engines[3])
                .handle_events_with(router_handlers_iter.next().expect("Missing router handler"))
                .and_then()
                .pin_at_core(core_pinning.risk_r2_engines[0])
                .handle_events_with(create_risk_r2_handler!(0, risk_engines))
                .pin_at_core(core_pinning.risk_r2_engines[1])
                .handle_events_with(create_risk_r2_handler!(1, risk_engines))
                .pin_at_core(core_pinning.risk_r2_engines[2])
                .handle_events_with(create_risk_r2_handler!(2, risk_engines))
                .pin_at_core(core_pinning.risk_r2_engines[3])
                .handle_events_with(create_risk_r2_handler!(3, risk_engines))
                .and_then()
                .pin_at_core(core_pinning.events)
                .handle_events_with(events_handler)
                .and_then()
                .pin_at_core(core_pinning.test_handler)
                .handle_events_with(test_handler)
                .build()
        }
    }

    impl Default for TestEngineBuilder {
        fn default() -> Self {
            Self::new()
        }
    }
}
