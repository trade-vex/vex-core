use crate::{create_event_handler, create_risk_handler, create_risk_r2_handler};
use common::{
    CoreMarketSpecification, OrderCommand, PriceCache, Status, TimeInForce, base_asset, quote_asset,
};
use disruptor::{
    BusySpin, MultiProducer, ProcessorSettings, SingleConsumerBarrier, build_multi_producer,
};
use hashbrown::HashMap;
use processors::{
    events::EventsHandler, journaling::JournalingProcessor, matching_engine::MatchingEngineRouter,
    risk_engine::RiskEngine,
};
use std::{
    sync::Arc,
    thread::{self, JoinHandle},
};
use tracing::info;
use vex_config::CoreNetworkingConfig;
use vex_networking::server::{GatewayPublications, VexCoreServer};

/// Type alias for the order command producer
pub type OrderProducer = MultiProducer<OrderCommand, SingleConsumerBarrier>;

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
    #[cfg(test)]
    test_handler: usize,
}

impl Default for CorePinning {
    fn default() -> Self {
        Self {
            journaling: 1,
            risk_engines: [2, 3, 4, 5],
            matching_engines: [6, 7, 8, 9],
            risk_r2_engines: [10, 11, 12, 13],
            events: 14,
            #[cfg(test)]
            test_handler: 15,
        }
    }
}

/// Builder for constructing a `CoreEngine` with fluent API
///
/// # Example
/// ```no_run
/// use vex_server::engine::CoreEngineBuilder;
/// use hashbrown::HashMap;
///
/// let (engine, producer, risk_engines) = CoreEngineBuilder::new()
///     .with_symbol_specs(symbol_specs)
///     .with_journaling_processor(journaling)
///     .with_events_handler(events_handler)
///     .with_publications(publications)
///     .build()
///     .expect("Failed to build core engine");
/// ```
pub struct CoreEngineBuilder {
    symbol_specs: Option<HashMap<u32, CoreMarketSpecification>>,
    journaling_processor: Option<JournalingProcessor>,
    events_handler: Option<Arc<dyn EventsHandler>>,
    publications: Option<Arc<GatewayPublications>>,
    core_pinning: CorePinning,
    #[cfg(test)]
    test_handler: Option<Box<dyn FnMut(&mut OrderCommand, i64, bool) + Send + 'static>>,
}

impl CoreEngineBuilder {
    /// Creates a new builder with default configuration
    #[must_use]
    pub const fn new() -> Self {
        Self {
            symbol_specs: None,
            journaling_processor: None,
            events_handler: None,
            publications: None,
            core_pinning: CorePinning {
                journaling: 1,
                risk_engines: [2, 3, 4, 5],
                matching_engines: [6, 7, 8, 9],
                risk_r2_engines: [10, 11, 12, 13],
                events: 14,
                #[cfg(test)]
                test_handler: 15,
            },
            #[cfg(test)]
            test_handler: None,
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
    pub fn with_events_handler(mut self, handler: Arc<dyn EventsHandler>) -> Self {
        self.events_handler = Some(handler);
        self
    }

    /// Sets the gateway publications for client communication
    #[must_use]
    pub fn with_publications(mut self, publications: Arc<GatewayPublications>) -> Self {
        self.publications = Some(publications);
        self
    }

    /// Sets custom core pinning configuration
    #[must_use]
    pub fn with_core_pinning(mut self, pinning: CorePinning) -> Self {
        self.core_pinning = pinning;
        self
    }

    /// Sets a test handler (only available in test builds)
    #[cfg(test)]
    #[must_use]
    pub fn with_test_handler<F>(mut self, handler: F) -> Self
    where
        F: FnMut(&mut OrderCommand, i64, bool) + Send + 'static,
    {
        self.test_handler = Some(Box::new(handler));
        self
    }

    /// Builds the `CoreEngine` with the configured parameters
    ///
    /// # Errors
    ///
    /// Returns `EngineError::Configuration` if any required field is missing
    pub fn build(self) -> EngineResult<(CoreEngine, OrderProducer, Option<RiskEngines>)> {
        let symbol_specs = self
            .symbol_specs
            .ok_or_else(|| EngineError::Configuration("Symbol specs are required".into()))?;
        let journaling_processor = self.journaling_processor.ok_or_else(|| {
            EngineError::Configuration("Journaling processor is required".into())
        })?;
        let events_handler = self
            .events_handler
            .ok_or_else(|| EngineError::Configuration("Events handler is required".into()))?;
        let publications = self
            .publications
            .ok_or_else(|| EngineError::Configuration("Publications are required".into()))?;

        #[cfg(test)]
        {
            if let Some(test_handler) = self.test_handler {
                return CoreEngine::new_internal(
                    symbol_specs,
                    journaling_processor,
                    events_handler,
                    publications,
                    self.core_pinning,
                    Some(test_handler),
                );
            }
        }

        CoreEngine::new_internal(
            symbol_specs,
            journaling_processor,
            events_handler,
            publications,
            self.core_pinning,
            #[cfg(test)]
            None,
        )
    }
}

impl Default for CoreEngineBuilder {
    fn default() -> Self {
        Self::new()
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
    publications: Arc<GatewayPublications>,
}

impl CoreEngine {
    /// Creates a new CoreEngine using the builder pattern
    ///
    /// # Deprecated
    ///
    /// Use [`CoreEngineBuilder`] instead for better type safety and ergonomics.
    ///
    /// # Errors
    ///
    /// Returns `EngineError` if engine initialization fails
    #[deprecated(since = "0.1.0", note = "Use CoreEngineBuilder instead")]
    #[cfg(test)]
    pub fn new(
        symbol_specs: HashMap<u32, CoreMarketSpecification>,
        journaling_processor: JournalingProcessor,
        events_handler: Arc<dyn EventsHandler>,
        publications: Arc<GatewayPublications>,
        test_handler: impl 'static + Send + FnMut(&mut OrderCommand, i64, bool),
    ) -> (Self, OrderProducer, Option<RiskEngines>) {
        Self::new_internal(
            symbol_specs,
            journaling_processor,
            events_handler,
            publications,
            CorePinning::default(),
            Some(Box::new(test_handler)),
        )
        .expect("Failed to create CoreEngine")
    }

    /// Creates a new CoreEngine using the builder pattern (production version)
    ///
    /// # Deprecated
    ///
    /// Use [`CoreEngineBuilder`] instead for better type safety and ergonomics.
    #[deprecated(since = "0.1.0", note = "Use CoreEngineBuilder instead")]
    #[cfg(not(test))]
    pub fn new(
        symbol_specs: HashMap<u32, CoreMarketSpecification>,
        journaling_processor: JournalingProcessor,
        events_handler: Arc<dyn EventsHandler>,
        publications: Arc<GatewayPublications>,
    ) -> (Self, OrderProducer, Option<RiskEngines>) {
        Self::new_internal(
            symbol_specs,
            journaling_processor,
            events_handler,
            publications,
            CorePinning::default(),
        )
        .expect("Failed to create CoreEngine")
    }

    /// Internal constructor that builds the disruptor pipeline
    ///
    /// This method creates the complete processing pipeline with all stages.
    ///
    /// # Errors
    ///
    /// Currently does not return errors, but signature allows for future error handling
    fn new_internal(
        symbol_specs: HashMap<u32, CoreMarketSpecification>,
        mut journaling_processor: JournalingProcessor,
        events_handler: Arc<dyn EventsHandler>,
        publications: Arc<GatewayPublications>,
        core_pinning: CorePinning,
        #[cfg(test)] test_handler: Option<
            Box<dyn FnMut(&mut OrderCommand, i64, bool) + Send + 'static>,
        >,
    ) -> EngineResult<(Self, OrderProducer, Option<RiskEngines>)> {
        // Initialize shared price cache for all processors
        let price_cache = Arc::new(PriceCache::new(symbol_specs.keys()));

        // Create order command factory for the disruptor
        let order_factory = OrderCommand::default;

        // Create journaling handler with proper closure capturing
        let journaling_handler =
            move |cmd: &mut OrderCommand, _sequence: i64, _end_of_batch: bool| {
                journaling_processor.journal_command(cmd);
            };

        // Clone events handler for the final stage
        let events_handler_arc = events_handler;

        // Initialize risk engines with power-of-2 sharding for efficient bitwise operations
        let risk_engines_arc = Self::initialize_risk_engines(&symbol_specs, NUM_RISK_ENGINES);

        // Initialize matching engine routers with symbol distribution
        let matching_engine_routers =
            Self::initialize_matching_engines(&symbol_specs, NUM_MATCHING_ENGINES);

        // Create matching engine handlers with proper closure scoping
        let router_handlers: Vec<_> = matching_engine_routers
            .into_iter()
            .map(|mut router| {
                let price_cache_clone = Arc::clone(&price_cache);
                move |cmd: &mut OrderCommand, _sequence: i64, _end_of_batch: bool| {
                    router.process_order(cmd, Arc::clone(&price_cache_clone));
                }
            })
            .collect();

        let mut router_handlers_iter = router_handlers.into_iter();

        // Build the disruptor pipeline with proper stage dependencies
        let producer = Self::build_disruptor_pipeline(
            BUFFER_SIZE,
            order_factory,
            journaling_handler,
            &risk_engines_arc,
            &price_cache,
            &mut router_handlers_iter,
            events_handler_arc,
            core_pinning,
            #[cfg(test)]
            test_handler,
        );

        let engine = Self { publications };
        Ok((engine, producer, Some(risk_engines_arc)))
    }

    /// Initializes risk engines with specified sharding
    fn initialize_risk_engines(
        symbol_specs: &HashMap<u32, CoreMarketSpecification>,
        num_shards: usize,
    ) -> RiskEngines {
        let risk_engines: Vec<_> = (0..num_shards)
            .map(|shard_id| RiskEngine::new(symbol_specs.clone(), shard_id as u32, num_shards as u32))
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
    fn build_disruptor_pipeline(
        buffer_size: usize,
        order_factory: fn() -> OrderCommand,
        journaling_handler: impl FnMut(&mut OrderCommand, i64, bool) + Send + 'static,
        risk_engines: &RiskEngines,
        price_cache: &Arc<PriceCache>,
        router_handlers_iter: &mut impl Iterator<
            Item = impl FnMut(&mut OrderCommand, i64, bool) + Send + 'static,
        >,
        events_handler: Arc<dyn EventsHandler>,
        core_pinning: CorePinning,
        #[cfg(test)] test_handler: Option<
            Box<dyn FnMut(&mut OrderCommand, i64, bool) + Send + 'static>,
        >,
    ) -> OrderProducer {
        // Build the entire pipeline in one chain to maintain proper types
        let producer = build_multi_producer(buffer_size, order_factory, BusySpin)
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
            .handle_events_with(create_event_handler!(events_handler));

        // Optional test handler for integration testing
        #[cfg(test)]
        if let Some(test_handler) = test_handler {
            return producer
                .and_then()
                .pin_at_core(core_pinning.test_handler)
                .handle_events_with(test_handler)
                .build();
        }

        producer.build()
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
        networking_config: CoreNetworkingConfig,
    ) -> JoinHandle<Result<(), EngineError>> {
        let publications = Arc::clone(&self.publications);

        thread::Builder::new()
            .name("vex-core-server".into())
            .spawn(move || {
                let mut core_server = VexCoreServer::new(networking_config, producer, publications)
                    .map_err(|e| {
                        EngineError::ServerInitialization(format!(
                            "Failed to create VexCoreServer: {e}"
                        ))
                    })?;

                core_server
                    .start()
                    .map_err(|e| EngineError::ServerRuntime(format!("Server error: {e}")))
            })
            .expect("Failed to spawn server thread")
    }

    /// Returns a reference to the gateway publications
    ///
    /// This can be used to send responses back to connected gateways
    #[must_use]
    pub fn publications(&self) -> &Arc<GatewayPublications> {
        &self.publications
    }
}
