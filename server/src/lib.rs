pub mod engine;
mod utils;

use common::CoreMarketSpecification;
use engine::{CoreEngine, EngineResult, OrderProducer};
use hashbrown::HashMap;
use processors::{
    events::KafkaEventsHandler,
    journaling::{JournalingProcessor, ReplayControl},
};
use std::{
    sync::{Arc, atomic::AtomicBool},
    thread::JoinHandle,
};
use vex_config::VexConfig;
use vex_networking::server::Publications;

// Re-export for convenience
pub use engine::EngineError;

pub struct RunningEngine {
    thread: JoinHandle<Result<(), EngineError>>,
    shutdown_flag: Arc<AtomicBool>,
}

impl RunningEngine {
    /// Returns on panic or error during server execution
    pub fn join(self) -> Result<(), EngineError> {
        self.thread.join().unwrap_or_else(|e| {
            Err(EngineError::ServerRuntime(format!(
                "Server thread panicked: {:?}",
                e
            )))
        })
    }

    /// Returns a clone of the shutdown flag for external signalling
    pub fn shutdown_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown_flag)
    }
}

/// Starts the exchange core server with the given configuration
///
/// This is the main entry point for production use. It initializes all components
/// (risk engines, matching engines, journaling, event handlers) and starts the
/// networking layer.
///
/// # Arguments
///
/// * `config` - Complete server configuration including symbols and networking
///
/// # Returns
///
/// A `RunningEngine` handle that keeps the server running. When dropped,
/// the server will shut down gracefully.
///
/// # Errors
///
/// Returns `EngineError` if initialization or server startup fails.
///
/// # Example
///
/// ```no_run
/// use vex_config::{VexConfig, Environment};
/// use vex_server::start;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let config = VexConfig::new(Environment::Development);
/// let _engine = start(config, false)?;
/// // Server runs until _engine is dropped
/// # Ok(())
/// # }
/// ```
pub fn start(config: VexConfig, replay: bool) -> Result<RunningEngine, EngineError> {
    config
        .validate()
        .map_err(|error| EngineError::Configuration(error.to_string()))?;

    report_disabled_journaling(&config);
    if !config.core_networking.enable_authentication
        || config
            .gateway_networking
            .gateway_authentication_key
            .is_empty()
    {
        tracing::warn!(
            target: "core_server",
            action = "gateway_authentication_disabled",
            environment = %config.environment,
            "GATEWAY HANDSHAKE AUTHENTICATION IS DISABLED; DEVELOPMENT/TEST USE ONLY"
        );
    }

    #[allow(unused_mut)] // mut needed when balance-preload feature is enabled
    let ((engine, mut producer), replay_control) = init_internal(
        config.symbols.symbols.clone(),
        config.kafka_broker.clone(),
        replay,
        config.core_networking.enable_core_pinning,
        config.environment.clone(),
    )?;

    // Balance preload for test/local environments
    #[cfg(feature = "balance-preload")]
    if config.balance_preload.enabled {
        use common::OrderCommand;
        use disruptor::Producer;
        use tracing::info;

        info!(
            "Balance preload enabled, funding {} users",
            config.balance_preload.users.len()
        );

        for (user_id, balances) in &config.balance_preload.users {
            for balance in balances {
                info!(
                    "Depositing {} units of asset {} for user {}",
                    balance.amount, balance.asset_id, user_id
                );

                producer.publish(|cmd| {
                    *cmd = OrderCommand::deposit_funds(*user_id, balance.amount, balance.asset_id);
                });
            }
        }

        info!("Balance preload complete");
    }

    let gateway_authentication_key = config.gateway_networking.gateway_authentication_key.clone();
    let (thread_handle, shutdown_flag) = engine.run(
        producer,
        replay_control,
        config.core_networking,
        gateway_authentication_key,
    );

    Ok(RunningEngine {
        thread: thread_handle,
        shutdown_flag,
    })
}

fn report_disabled_journaling(config: &VexConfig) {
    if config.core_networking.request_control_channel.is_empty() {
        tracing::error!(
            target: "server",
            action = "journaling_disabled",
            archive_recording = false,
            environment = %config.environment,
            "JOURNALING IS DISABLED: commands will not be recoverable by replay"
        );
    }
}

/// Internal initialization function
///
/// Creates the core engine with all necessary components. This is used by both
/// the production `start()` function and the test `setup()` function.
pub fn init_internal(
    symbol_specs: HashMap<u32, CoreMarketSpecification>,
    kafka_broker: String,
    replay: bool,
    enable_core_pinning: bool,
    environment: vex_config::Environment,
) -> EngineResult<((CoreEngine, OrderProducer), ReplayControl)> {
    let replay_control = if replay {
        ReplayControl::enabled()
    } else {
        ReplayControl::disabled()
    };
    let publications = Arc::new(Publications::new());
    let journaling_processor =
        JournalingProcessor::new(Arc::clone(&publications), replay_control.clone());

    // Get schema registry URL from environment variable or use default
    let schema_registry_url = std::env::var("SCHEMA_REGISTRY_URL")
        .unwrap_or_else(|_| "http://localhost:8081".to_string());

    let events_handler = KafkaEventsHandler::new(
        &kafka_broker,
        &schema_registry_url,
        Arc::clone(&publications),
        replay_control.clone(),
    );

    // Use no-pinning for Development environment to avoid CPU affinity issues
    let core_pinning = if matches!(environment, vex_config::Environment::Development) {
        None
    } else {
        Some(engine::CorePinning::default())
    };

    let engine = CoreEngine::new(
        symbol_specs,
        journaling_processor,
        events_handler,
        publications,
        core_pinning.unwrap_or_default(),
        enable_core_pinning,
    )?;

    Ok((engine, replay_control))
}

/// Test utilities for vex-server
///
/// This module provides a simplified API for testing the exchange engine.
/// Use `test::setup()` to create a test environment with direct access to
/// the producer and risk engines.
#[cfg(test)]
pub mod test {
    use super::*;
    use common::{OrderCommand, Status};
    use engine::{RiskEngines, test::TestEngineBuilder};
    use std::{cell::UnsafeCell, sync::mpsc};

    /// Test engine instance with access to internals for testing
    pub struct TestEngine {
        /// Order producer for publishing test commands
        pub producer: OrderProducer,
        /// Risk engines for balance manipulation and verification
        pub risk_engines: RiskEngines,
        /// Receiver for processed commands
        pub receiver: mpsc::Receiver<OrderCommand>,
    }

    /// Sets up a test environment with the given market specifications (tuple form)
    ///
    /// Returns a tuple of (producer, risk_engines, receiver) for backwards compatibility.
    /// For new code, prefer using `setup()` which returns a `TestEngine` struct.
    pub fn setup_tuple(
        specs: HashMap<u32, CoreMarketSpecification>,
    ) -> (OrderProducer, RiskEngines, mpsc::Receiver<OrderCommand>) {
        let test_engine = setup(specs);
        (
            test_engine.producer,
            test_engine.risk_engines,
            test_engine.receiver,
        )
    }

    /// Sets up a test environment with the given market specifications
    ///
    /// This creates a complete engine instance configured for testing, with
    /// a channel for receiving processed commands.
    ///
    /// # Example
    ///
    /// ```
    /// use vex_server::test::setup;
    /// use hashbrown::HashMap;
    ///
    /// let mut specs = HashMap::new();
    /// // ... add market specs ...
    ///
    /// let test_env = setup(specs);
    /// test_env.producer.publish(|cmd| {
    ///     // Configure order command
    /// });
    ///
    /// let result = test_env.receiver.recv_timeout(Duration::from_secs(1)).unwrap();
    /// ```
    pub fn setup(specs: HashMap<u32, CoreMarketSpecification>) -> TestEngine {
        let (tx, rx) = mpsc::channel::<OrderCommand>();
        let test_handler = move |cell: &UnsafeCell<OrderCommand>, _: i64, _: bool| {
            // SAFETY: This test capture handler is alone after the events and_then()
            // barrier, so creating this exclusive reference cannot alias another handler's
            // reference.
            let cmd = unsafe { &mut *cell.get() };
            if cmd.status != Status::Processing {
                let _ = tx.send(cmd.clone());
            }
        };

        let publications = Arc::new(Publications::new());

        // Create risk engines manually for test access
        use processors::risk_engine::RiskEngine;
        let risk_engines = Arc::new(
            (0..4)
                .map(|shard_id| RiskEngine::new(specs.clone(), shard_id as u32, 4))
                .collect::<Vec<_>>(),
        );

        let (_engine, producer) = TestEngineBuilder::new()
            .with_symbol_specs(specs)
            .with_journaling_processor(JournalingProcessor::new(
                Arc::clone(&publications),
                ReplayControl::disabled(),
            ))
            .with_events_handler(KafkaEventsHandler::new(
                "localhost:9092",
                "http://localhost:8081",
                Arc::clone(&publications),
                ReplayControl::disabled(),
            ))
            .with_publications(publications)
            .with_risk_engines(Arc::clone(&risk_engines))
            .with_test_handler(test_handler)
            .build()
            .expect("Failed to build test engine");

        TestEngine {
            producer,
            risk_engines,
            receiver: rx,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test::setup_tuple;
    use super::*;
    use common::{
        MarketType, OrderCommand, PriceCache, Side, Status, TimeInForce, UserBalance, base_asset,
        quote_asset,
    };
    use disruptor::Producer;
    use processors::risk_engine::RiskEngine;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use tracing::field::{Field, Visit};
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::{Layer, Registry};

    struct ActionVisitor {
        journaling_disabled: bool,
    }

    impl Visit for ActionVisitor {
        fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}

        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "action" && value == "journaling_disabled" {
                self.journaling_disabled = true;
            }
        }
    }

    struct DisabledJournalLayer(Arc<AtomicBool>);

    impl<S: Subscriber> Layer<S> for DisabledJournalLayer {
        fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
            let mut visitor = ActionVisitor {
                journaling_disabled: false,
            };
            event.record(&mut visitor);
            if visitor.journaling_disabled && *event.metadata().level() == Level::ERROR {
                self.0.store(true, Ordering::Relaxed);
            }
        }
    }

    #[test]
    fn non_production_config_loudly_reports_disabled_journaling() {
        let mut config = VexConfig::new(vex_config::Environment::Development);
        config.core_networking.request_control_channel.clear();
        let saw_error = Arc::new(AtomicBool::new(false));
        let subscriber = Registry::default().with(DisabledJournalLayer(Arc::clone(&saw_error)));

        assert!(config.validate().is_ok());
        tracing::subscriber::with_default(subscriber, || report_disabled_journaling(&config));
        assert!(saw_error.load(Ordering::Relaxed));
    }

    #[cfg(feature = "balance-preload")]
    #[test]
    fn production_balance_preload_is_rejected_before_startup() {
        let mut config = VexConfig::new(vex_config::Environment::Production);
        config.balance_preload.enabled = true;

        assert!(matches!(
            start(config, false),
            Err(EngineError::Configuration(_))
        ));
    }

    /// Helper function to add a market specification to the specs map
    pub fn add_spec(market_id: u32, specs: &mut HashMap<u32, CoreMarketSpecification>) {
        specs.insert(
            market_id,
            CoreMarketSpecification::builder()
                .market_id(market_id)
                .market_type(MarketType::Spot)
                .maker_fee(10) // 0.1%
                .taker_fee(20) // 0.2%
                .slippage(5)
                .build()
                .unwrap(),
        );
    }

    #[derive(Clone, Copy)]
    enum ShippedScaling {
        EthUsdt,
        BtcUsdt,
    }

    /// Adds a market using the scale factors shipped in `vex-config/src/symbols.rs`.
    fn add_shipped_spec(
        scaling: ShippedScaling,
        specs: &mut HashMap<u32, CoreMarketSpecification>,
    ) -> u32 {
        let (base_asset_id, market_type, base_scale_k, quote_scale_k, base_native_scale) =
            match scaling {
                ShippedScaling::EthUsdt => {
                    (3, MarketType::Spot, 100_000, 10, 1_000_000_000_000_000_000)
                }
                ShippedScaling::BtcUsdt => (2, MarketType::FuturesContract, 1, 1, 100_000_000),
            };
        let quote_asset_id = 1;
        let market_id = ((quote_asset_id as u32) << 16) | (base_asset_id as u32);

        specs.insert(
            market_id,
            CoreMarketSpecification::builder()
                .market_id(market_id)
                .market_type(market_type)
                .base_scale_k(base_scale_k)
                .quote_scale_k(quote_scale_k)
                .base_native_scale(base_native_scale)
                .quote_native_scale(1_000_000)
                // These are the committed configuration's 10 bp maker / 20 bp taker rates.
                .maker_fee(10)
                .taker_fee(20)
                .slippage(5)
                .build()
                .unwrap(),
        );

        market_id
    }

    /// Helper struct for checking user balances in tests
    pub struct BalanceChecker<'a> {
        risk_engines: &'a Arc<Vec<RiskEngine>>,
        shard_mask: u64,
    }

    impl<'a> BalanceChecker<'a> {
        /// Creates a new balance checker
        pub fn new(risk_engines: &'a Arc<Vec<RiskEngine>>) -> Self {
            Self {
                risk_engines,
                shard_mask: risk_engines.len() as u64 - 1,
            }
        }

        /// Gets the shard ID for a given user
        fn get_shard_id(&self, user_id: u64) -> usize {
            (user_id & self.shard_mask) as usize
        }

        /// Checks if a user's balance matches expected values
        pub fn check(
            &self,
            user_id: u64,
            asset_id: u16,
            expected_available: u64,
            expected_locked: u64,
            context: &str,
        ) {
            let shard_id = self.get_shard_id(user_id);
            let balance = self.risk_engines[shard_id].get_balance(user_id, asset_id);
            let expected_balance = UserBalance::new(expected_available, expected_locked);
            assert_eq!(
                balance, expected_balance,
                "Balance check failed for User {user_id} Asset {asset_id} [Context: {context}]"
            );
        }

        /// Sets a user's balance
        pub fn set_balance(&self, user_id: u64, asset_id: u16, available: u64, locked: u64) {
            let shard_id = self.get_shard_id(user_id);
            self.risk_engines[shard_id].set_balance(
                user_id,
                asset_id,
                UserBalance::new(available, locked),
            );
        }
    }

    #[test]
    fn test_quote_cost_at_shipped_eth_and_btc_scales() {
        let mut specs = HashMap::new();
        let eth_market_id = add_shipped_spec(ShippedScaling::EthUsdt, &mut specs);
        let btc_market_id = add_shipped_spec(ShippedScaling::BtcUsdt, &mut specs);
        let eth = &specs[&eth_market_id];
        let btc = &specs[&btc_market_id];

        assert_eq!(
            (
                eth.base_native_scale,
                eth.quote_native_scale,
                eth.base_scale_k,
                eth.quote_scale_k,
            ),
            (1_000_000_000_000_000_000, 1_000_000, 100_000, 10)
        );
        assert_eq!(
            (
                btc.base_native_scale,
                btc.quote_native_scale,
                btc.base_scale_k,
                btc.quote_scale_k,
            ),
            (100_000_000, 1_000_000, 1, 1)
        );

        let eth_price = 30_000; // $3,000 at the shipped quote_scale_k of 10.
        let smallest_nonzero_eth_fill = 333_333_334;
        assert_eq!(
            eth.calculate_quote_cost(eth_price, smallest_nonzero_eth_fill - 1),
            0
        );
        assert_eq!(
            eth.calculate_quote_cost(eth_price, smallest_nonzero_eth_fill),
            1
        );
        assert_eq!(
            eth.calculate_quote_cost(eth_price, 1_000_000_000_000_000_000),
            3_000_000_000
        );
        assert_eq!(
            eth.calculate_quote_cost(eth_price, u64::MAX),
            55_340_232_221
        );

        let btc_price = 60_000;
        assert_eq!(btc.calculate_quote_cost(btc_price, 1), 600);
        assert_eq!(
            btc.calculate_quote_cost(btc_price, 100_000_000),
            60_000_000_000
        );

        // This is the largest BTC fill whose $60,000 quote cost fits in u64.
        let largest_btc_fill_in_range = 30_744_573_456_182_586;
        assert_eq!(
            btc.calculate_quote_cost(btc_price, largest_btc_fill_in_range),
            18_446_744_073_709_551_600
        );
    }

    #[test]
    // Fixed in this stack by PR #146 (calculate_quote_cost saturates instead of wrapping).
    fn test_shipped_btc_quote_cost_is_monotonic_past_largest_in_range() {
        let mut specs = HashMap::new();
        let market_id = add_shipped_spec(ShippedScaling::BtcUsdt, &mut specs);
        let spec = &specs[&market_id];
        let largest_fill_in_range = 30_744_573_456_182_586;
        let largest_cost = spec.calculate_quote_cost(60_000, largest_fill_in_range);
        let next_cost = spec.calculate_quote_cost(60_000, largest_fill_in_range + 1);

        assert!(
            next_cost > largest_cost,
            "quote cost must not silently wrap after the largest in-range fill"
        );
    }

    #[test]
    fn test_shipped_eth_collateral_lock_and_release_for_both_sides() {
        let mut specs = HashMap::new();
        let market_id = add_shipped_spec(ShippedScaling::EthUsdt, &mut specs);
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 0, 1);
        let base_asset_id = base_asset(market_id);
        let quote_asset_id = quote_asset(market_id);
        let price = 30_000;
        let size = 1_000_000_000;
        let quote_cost = 3;

        let bid_user_id = 10;
        engine.set_balance(bid_user_id, quote_asset_id, UserBalance::new(quote_cost, 0));
        let mut bid = OrderCommand::place_order(
            TimeInForce::Gtc,
            bid_user_id,
            price,
            size,
            Side::Bid,
            market_id,
            1,
        );
        engine.pre_process_command(&mut bid, Arc::clone(&price_cache));
        assert_eq!(
            engine.get_balance(bid_user_id, quote_asset_id),
            UserBalance::new(0, quote_cost)
        );
        engine.handle_cancellation(&mut bid);
        assert_eq!(
            engine.get_balance(bid_user_id, quote_asset_id),
            UserBalance::new(quote_cost, 0)
        );

        let ask_user_id = 11;
        engine.set_balance(ask_user_id, base_asset_id, UserBalance::new(size, 0));
        let mut ask = OrderCommand::place_order(
            TimeInForce::Gtc,
            ask_user_id,
            price,
            size,
            Side::Ask,
            market_id,
            2,
        );
        engine.pre_process_command(&mut ask, price_cache);
        assert_eq!(
            engine.get_balance(ask_user_id, base_asset_id),
            UserBalance::new(0, size)
        );
        engine.handle_cancellation(&mut ask);
        assert_eq!(
            engine.get_balance(ask_user_id, base_asset_id),
            UserBalance::new(size, 0)
        );
    }

    #[test]
    fn test_shipped_eth_settlement_releases_both_sides_and_rounds_fees() {
        let mut specs = HashMap::new();
        let market_id = add_shipped_spec(ShippedScaling::EthUsdt, &mut specs);
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 0, 1);
        let base_asset_id = base_asset(market_id);
        let quote_asset_id = quote_asset(market_id);
        let buyer_id = 20;
        let seller_id = 21;
        let price = 30_000;
        let size = 333_333_334; // Smallest fill with a non-zero quote cost at this price.
        let quote_cost = 1;

        engine.set_balance(buyer_id, quote_asset_id, UserBalance::new(quote_cost, 0));
        engine.set_balance(seller_id, base_asset_id, UserBalance::new(size, 0));
        let mut bid = OrderCommand::place_order(
            TimeInForce::Gtc,
            buyer_id,
            price,
            size,
            Side::Bid,
            market_id,
            1,
        );
        let mut ask = OrderCommand::place_order(
            TimeInForce::Gtc,
            seller_id,
            price,
            size,
            Side::Ask,
            market_id,
            2,
        );
        engine.pre_process_command(&mut bid, Arc::clone(&price_cache));
        engine.pre_process_command(&mut ask, price_cache);

        // MatcherTradeEvent implements Drop (PR #179): the functional-update form
        // would move out of a Drop type, so assign the fields instead.
        let mut event = common::MatcherTradeEvent::default();
        event.price = price;
        event.size = size;
        event.maker_user_id = seller_id;
        event.matched_order_id = ask.order_id;
        event.matched_order_completed = true;
        event.active_order_completed = true;
        event.maker_original_size = size;
        engine.handle_trade_event(
            buyer_id,
            market_id,
            Side::Bid,
            &mut event,
            Some((bid.order_id, price)),
        );
        engine.handle_trade_event(seller_id, market_id, Side::Ask, &mut event, None);

        // PR #163 rounds fees UP per fill (div_ceil), so the remainder below costs one more
        // atomic unit than the pre-#163 floor of 666_666.
        let taker_fee_in_base = 666_667;
        assert_eq!((size * 20) % 10_000, 6_680);
        assert_eq!(
            engine.get_balance(buyer_id, quote_asset_id),
            UserBalance::new(0, 0)
        );
        assert_eq!(
            engine.get_balance(buyer_id, base_asset_id),
            UserBalance::new(size - taker_fee_in_base, 0)
        );
        assert_eq!(
            engine.get_balance(seller_id, base_asset_id),
            UserBalance::new(0, 0)
        );
        // Gross quote here is one atomic unit. Pre-#163 the 10 bp maker fee floored to zero
        // and the seller kept the unit; with #163's per-fill div_ceil the fee is a whole unit,
        // so the maker nets nothing. Same ceil-per-fill overcharge as the ask round-trip test.
        let maker_fee_in_quote = (quote_cost * 10).div_ceil(10_000);
        assert_eq!(
            engine.get_balance(seller_id, quote_asset_id),
            UserBalance::new(quote_cost - maker_fee_in_quote, 0)
        );
    }

    #[test]
    fn test_shipped_eth_ask_partial_fill_cancel_round_trip() {
        let mut specs = HashMap::new();
        let market_id = add_shipped_spec(ShippedScaling::EthUsdt, &mut specs);
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 0, 1);
        let base_asset_id = base_asset(market_id);
        let quote_asset_id = quote_asset(market_id);
        let user_id = 30;
        let price = 30_000;
        let initial_base = 1_000_000_000;
        let fill_size = 333_333_334;
        let remaining_size = initial_base - fill_size;

        engine.set_balance(user_id, base_asset_id, UserBalance::new(initial_base, 0));
        let mut ask = OrderCommand::place_order(
            TimeInForce::Gtc,
            user_id,
            price,
            initial_base,
            Side::Ask,
            market_id,
            1,
        );
        engine.pre_process_command(&mut ask, price_cache);

        // MatcherTradeEvent implements Drop (PR #179): the functional-update form
        // would move out of a Drop type, so assign the fields instead.
        let mut event = common::MatcherTradeEvent::default();
        event.price = price;
        event.size = fill_size;
        event.maker_user_id = user_id;
        event.matched_order_id = ask.order_id;
        event.maker_original_size = initial_base;
        event.maker_remaining_size = remaining_size;
        engine.handle_trade_event(user_id, market_id, Side::Ask, &mut event, None);
        ask.set_size(remaining_size);
        engine.handle_cancellation(&mut ask);

        assert_eq!(
            engine.get_balance(user_id, base_asset_id),
            UserBalance::new(initial_base - fill_size, 0)
        );
        // Gross quote is one atomic unit. Pre-#163 the 10 bp fee floored to zero and the
        // seller kept the unit; with #163's per-fill div_ceil the fee is one unit, i.e. the
        // whole proceeds. This is the ceil-per-fill overcharge tracked against #163 -- the
        // stacked behaviour is asserted here so it cannot regress silently either way.
        assert_eq!(
            engine.get_balance(user_id, quote_asset_id),
            UserBalance::new(0, 0)
        );
    }

    #[test]
    // Fixed in this stack by PR #146 (zero-collateral BID rejection).
    fn test_shipped_eth_bid_with_zero_quote_cost_is_rejected() {
        let mut specs = HashMap::new();
        let market_id = add_shipped_spec(ShippedScaling::EthUsdt, &mut specs);
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 0, 1);
        let mut bid = OrderCommand::place_order(
            TimeInForce::Gtc,
            40,
            30_000,
            333_333_333,
            Side::Bid,
            market_id,
            1,
        );

        // Before #111, locking zero succeeded and admitted an uncollateralized BID.
        engine.pre_process_command(&mut bid, price_cache);

        assert_eq!(bid.status, Status::Rejected);
    }

    #[test]
    // Fixed in this stack by PR #171 (bid_collateral releases exactly what was locked).
    fn test_shipped_eth_bid_partial_fill_cancel_has_no_collateral_residue() {
        let mut specs = HashMap::new();
        let market_id = add_shipped_spec(ShippedScaling::EthUsdt, &mut specs);
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 0, 1);
        let base_asset_id = base_asset(market_id);
        let quote_asset_id = quote_asset(market_id);
        let user_id = 50;
        let price = 30_000;
        let order_size = 1_000_000_000;
        let fill_size = 333_333_334;
        let remaining_size = order_size - fill_size;
        let initial_quote = 10;

        engine.set_balance(user_id, quote_asset_id, UserBalance::new(initial_quote, 0));
        let mut bid = OrderCommand::place_order(
            TimeInForce::Gtc,
            user_id,
            price,
            order_size,
            Side::Bid,
            market_id,
            1,
        );
        engine.pre_process_command(&mut bid, price_cache);

        // MatcherTradeEvent implements Drop (PR #179): the functional-update form
        // would move out of a Drop type, so assign the fields instead.
        let mut event = common::MatcherTradeEvent::default();
        event.price = price;
        event.size = fill_size;
        event.maker_user_id = user_id + 1;
        event.matched_order_id = 2;
        event.maker_original_size = fill_size;
        engine.handle_trade_event(
            user_id,
            market_id,
            Side::Bid,
            &mut event,
            Some((bid.order_id, price)),
        );
        bid.set_size(remaining_size);
        engine.handle_cancellation(&mut bid);

        // PR #163 rounds per-fill fees UP; this expectation predated it.
        let fee = (fill_size * 20).div_ceil(10_000);
        assert_eq!(
            engine.get_balance(user_id, base_asset_id),
            UserBalance::new(fill_size - fee, 0)
        );
        assert_eq!(
            engine.get_balance(user_id, quote_asset_id),
            UserBalance::new(initial_quote - 1, 0),
            "lock(total) must equal spent(fill) + unlock(remainder)"
        );
    }

    #[test]
    // Fixed in this stack by PR #154 (fee arithmetic in u128).
    fn test_shipped_eth_one_token_fee_arithmetic_does_not_overflow() {
        let mut specs = HashMap::new();
        let market_id = add_shipped_spec(ShippedScaling::EthUsdt, &mut specs);
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 0, 1);
        let quote_asset_id = quote_asset(market_id);
        let user_id = 60;
        let price = 30_000;
        let one_eth = 1_000_000_000_000_000_000;

        engine.set_balance(user_id, quote_asset_id, UserBalance::new(3_000_000_000, 0));
        let mut bid = OrderCommand::place_order(
            TimeInForce::Gtc,
            user_id,
            price,
            one_eth,
            Side::Bid,
            market_id,
            1,
        );
        engine.pre_process_command(&mut bid, price_cache);

        // MatcherTradeEvent implements Drop (PR #179): the functional-update form
        // would move out of a Drop type, so assign the fields instead.
        let mut event = common::MatcherTradeEvent::default();
        event.price = price;
        event.size = one_eth;
        event.maker_user_id = user_id + 1;
        event.matched_order_id = 2;
        event.maker_original_size = one_eth;
        engine.handle_trade_event(
            user_id,
            market_id,
            Side::Bid,
            &mut event,
            Some((bid.order_id, price)),
        );

        assert_eq!(
            engine
                .get_balance(user_id, base_asset(market_id))
                .available(),
            998_000_000_000_000_000
        );
    }

    #[test]
    fn test_balance_lock_on_order_placement() {
        // 1. Setup
        let mut specs = HashMap::new();
        let base_asset_id = 1;
        let quote_asset_id = 2;
        // Market ID: base asset in lower 16 bits, quote in upper 16
        let market_id = ((quote_asset_id as u32) << 16) | (base_asset_id as u32);
        add_spec(market_id, &mut specs);

        let (mut producer, risk_engines, rx) = setup_tuple(specs);

        // 2. Pre-fund user account
        let user_id = 42;
        let initial_base_balance = 1_000_000;
        let initial_quote_balance = 1_000_000;

        // User 42 belongs to shard 2 (42 & 3 = 2) with 4 shards.
        let shard_mask = risk_engines.len() as u64 - 1;
        let shard_id = (user_id & shard_mask) as usize;
        let risk_engine = &risk_engines[shard_id];

        risk_engine.set_balance(
            user_id,
            base_asset_id,
            UserBalance::new(initial_base_balance, 0),
        );
        risk_engine.set_balance(
            user_id,
            quote_asset_id,
            UserBalance::new(initial_quote_balance, 0),
        );

        // 3. Publish a BID order that will be placed on the book but not filled
        let order_price = 5000;
        let order_size = 10;
        let expected_locked_amount = order_price * order_size;

        producer.publish(|cmd: &mut OrderCommand| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                user_id,
                order_price,
                order_size,
                Side::Bid,
                market_id,
                1,
            );
            cmd.market_id = market_id;
        });

        // 4. Wait for the command to be processed and receive it from the test handler
        let processed_cmd = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("Test timed out waiting for processed command");

        // 5. Assertions
        // assert_eq!(processed_cmd.order_id, 1);
        assert_eq!(processed_cmd.status, Status::Placed);

        // The `balance` field on OrderCommand is the final balance state. For a BID order, the QUOTE asset is locked.
        let final_base_balance = processed_cmd.balance[0];
        let final_quote_balance = processed_cmd.balance[1];

        // Base asset balance should be unchanged for a placed (unfilled) BID order
        assert_eq!(final_base_balance.available, initial_base_balance);
        assert_eq!(final_base_balance.locked, 0);

        // Quote asset balance should be locked
        assert_eq!(
            final_quote_balance.available,
            initial_quote_balance - expected_locked_amount
        );
        assert_eq!(final_quote_balance.locked, expected_locked_amount);
    }

    #[test]
    fn test_balance_update_on_trade() {
        // 1. Setup
        let mut specs = HashMap::new();
        let base_asset_id = 1;
        let quote_asset_id = 2;
        let market_id = ((quote_asset_id as u32) << 16) | (base_asset_id as u32);
        add_spec(market_id, &mut specs);
        let spec = specs.get_mut(&market_id).unwrap();
        spec.base_scale_k = 100_000;
        spec.quote_scale_k = 10;

        let (mut producer, risk_engines, rx) = setup_tuple(specs);
        let shard_mask = risk_engines.len() as u64 - 1;

        // 2. Define users and order details
        let taker_id = 42; // Belongs to shard 2 (42 & 3 = 2)
        let maker_id = 101; // Belongs to shard 1 (101 & 3 = 1)

        let order_price = 5000;
        let order_size = 10_000; // Use a larger size to make fees non-zero

        // 3. Pre-fund accounts
        // Taker (buyer, BID) needs QUOTE asset to buy the BASE asset.
        let taker_initial_quote = 5_000_000;
        let taker_shard_id = (taker_id & shard_mask) as usize;
        risk_engines[taker_shard_id].set_balance(
            taker_id,
            quote_asset_id,
            UserBalance::new(taker_initial_quote, 0),
        );
        risk_engines[taker_shard_id].set_balance(taker_id, base_asset_id, UserBalance::new(0, 0));

        // Maker (seller, ASK) needs BASE asset to sell.
        let maker_initial_base = order_size;
        let maker_shard_id = (maker_id & shard_mask) as usize;
        risk_engines[maker_shard_id].set_balance(
            maker_id,
            base_asset_id,
            UserBalance::new(maker_initial_base, 0),
        );
        risk_engines[maker_shard_id].set_balance(maker_id, quote_asset_id, UserBalance::new(0, 0));

        // 4. Place Maker's ASK (sell) order
        let maker_order_id = 2;
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                maker_id,
                order_price,
                order_size,
                Side::Ask,
                market_id,
                maker_order_id,
            );
            cmd.market_id = market_id;
        });

        // 5. Wait for Maker's order to be placed on the book
        let maker_placed_cmd = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("Did not receive maker's placed command");
        // assert_eq!(maker_placed_cmd.order_id, maker_order_id);
        assert_eq!(maker_placed_cmd.status, Status::Placed);

        // 6. Place Taker's BID (buy) order to trigger a trade
        let taker_order_id = 1;
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                taker_id,
                order_price,
                order_size,
                Side::Bid,
                market_id,
                taker_order_id,
            );
            cmd.market_id = market_id;
        });

        // 7. Wait for Taker's order to be filled. This command will contain the trade event.
        let taker_filled_cmd = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("Did not receive taker's filled command");
        // assert_eq!(taker_filled_cmd.order_id, taker_order_id);
        assert_eq!(taker_filled_cmd.status, Status::Filled);

        // 8. Assertions
        let trade_event = taker_filled_cmd
            .events()
            .expect("Taker command should have a trade event");

        // --- Taker (Buyer, BID) Balance Verification ---
        // Spends 5,000,000 scaled quote units. Receives 10,000 base units minus a 20-unit fee.
        assert_eq!(
            taker_filled_cmd.balance[0].total(), // base asset
            9_980,
            "Taker's base balance is incorrect"
        );
        assert_eq!(
            taker_filled_cmd.balance[1].total(), // quote asset
            0,
            "Taker's quote balance should be 0"
        );
        assert_eq!(
            risk_engines[taker_shard_id].get_balance(taker_id, base_asset_id),
            taker_filled_cmd.balance[0]
        );
        assert_eq!(
            risk_engines[taker_shard_id].get_balance(taker_id, quote_asset_id),
            taker_filled_cmd.balance[1]
        );

        // --- Maker (Seller, ASK) Balance Verification ---
        // Spends 10,000 base units. Receives 5,000,000 scaled quote units minus a 5,000-unit fee.
        assert_eq!(
            trade_event.maker_balance[0].total(), // base asset
            0,
            "Maker's base balance should be 0"
        );
        assert_eq!(
            trade_event.maker_balance[1].total(),
            4_995_000,
            "Maker's quote balance is incorrect"
        );
        assert_eq!(
            risk_engines[maker_shard_id].get_balance(maker_id, base_asset_id),
            trade_event.maker_balance[0]
        );
        assert_eq!(
            risk_engines[maker_shard_id].get_balance(maker_id, quote_asset_id),
            trade_event.maker_balance[1]
        );
    }

    #[test]
    fn test_balance_update_on_partial_multi_trade() {
        // 1. Setup
        let mut specs = HashMap::new();
        let base_asset_id = 1;
        let quote_asset_id = 2;
        let market_id = ((quote_asset_id as u32) << 16) | (base_asset_id as u32);
        add_spec(market_id, &mut specs);

        let (mut producer, risk_engines, rx) = setup_tuple(specs);
        let shard_mask = risk_engines.len() as u64 - 1;
        let get_shard_id = |user_id: u64| (user_id & shard_mask) as usize;

        // 2. Define users across different shards
        let taker_id = 50; // Shard 2 (50 & 3 = 2)
        let maker1_id = 101; // Shard 1 (101 & 3 = 1)
        let maker2_id = 102; // Shard 2 (102 & 3 = 2)
        let maker3_id = 103; // Shard 3 (103 & 3 = 3)

        let maker1_shard = get_shard_id(maker1_id);
        let maker2_shard = get_shard_id(maker2_id);
        let maker3_shard = get_shard_id(maker3_id);
        let taker_shard = get_shard_id(taker_id);

        // 3. Pre-fund accounts
        let order_price = 5000;
        let maker1_size = 10_000;
        let maker2_size = 20_000;
        let maker3_size = 30_000;
        let taker_trade_size = maker1_size + maker2_size + 15_000; // Will take all of m1, m2, and 15k of m3

        // Fund makers with BASE asset to sell
        risk_engines[maker1_shard].set_balance(
            maker1_id,
            base_asset_id,
            UserBalance::new(maker1_size, 0),
        );
        risk_engines[maker2_shard].set_balance(
            maker2_id,
            base_asset_id,
            UserBalance::new(maker2_size, 0),
        );
        risk_engines[maker3_shard].set_balance(
            maker3_id,
            base_asset_id,
            UserBalance::new(maker3_size, 0),
        );

        // Fund taker with QUOTE asset to buy
        let taker_initial_quote = order_price * taker_trade_size;
        risk_engines[taker_shard].set_balance(
            taker_id,
            quote_asset_id,
            UserBalance::new(taker_initial_quote, 0),
        );

        // 4. Place Maker orders (all at the same price to test FIFO)
        let maker_orders = [
            (maker1_id, maker1_size, 1),
            (maker2_id, maker2_size, 2),
            (maker3_id, maker3_size, 3),
        ];

        for &(id, size, order_id) in &maker_orders {
            producer.publish(|cmd| {
                *cmd = OrderCommand::place_order(
                    TimeInForce::Gtc,
                    id,
                    order_price,
                    size,
                    Side::Ask,
                    market_id,
                    order_id,
                );
            });
            let placed_cmd = rx
                .recv_timeout(Duration::from_secs(1))
                .expect("Maker order placement timed out");
            // assert orders are placed correctly
            // assert_eq!(placed_cmd.order_id, order_id);
            assert_eq!(placed_cmd.status, Status::Placed);
            // assert if the balance is locked correctly for the maker
            assert_eq!(placed_cmd.balance[0].locked, size);
            assert_eq!(placed_cmd.balance[0].available, 0); // All base is locked
            // assert that the balance in the command is correct and matches the risk engine state
            assert_eq!(
                placed_cmd.balance[0],
                risk_engines[get_shard_id(id)].get_balance(id, base_asset_id)
            );
            assert_eq!(
                placed_cmd.balance[1],
                risk_engines[get_shard_id(id)].get_balance(id, quote_asset_id)
            );
        }

        // 5. Place Taker's BID order to sweep the book
        let taker_order_id = 1;
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                taker_id,
                order_price,
                taker_trade_size,
                Side::Bid,
                market_id,
                taker_order_id,
            );
        });

        // 6. Wait for Taker's order to be filled.
        let taker_filled_cmd = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("Did not receive taker's filled command");
        // balances should be updated correctly for both taker and makers
        assert_eq!(taker_filled_cmd.balance[0].total(), 44_910); // base after 20bp fee
        assert_eq!(taker_filled_cmd.balance[1].total(), 0);
        // no balance must be locked for the taker after the trade
        assert_eq!(taker_filled_cmd.balance[1].locked, 0);
        assert_eq!(taker_filled_cmd.balance[0].locked, 0);
        // assert_eq!(taker_filled_cmd.order_id, taker_order_id);
        assert_eq!(taker_filled_cmd.status, Status::Filled);

        // 7. Assertions
        let mut event_opt = taker_filled_cmd.events();
        assert!(
            event_opt.is_some(),
            "Taker command should have trade events"
        );

        // --- Event 1: Trade with Maker 1 ---
        let event1 = event_opt.unwrap();
        assert_eq!(event1.maker_user_id, maker1_id);
        assert_eq!(event1.size, maker1_size);
        assert!(event1.matched_order_completed);
        assert!(!event1.active_order_completed);
        assert_eq!(event1.maker_balance[0].available(), 0); // Maker 1 sold all base
        assert_eq!(event1.maker_balance[1].available(), 49_950_000);
        assert_eq!(event1.maker_balance[1].locked, 0);
        assert_eq!(event1.maker_balance[0].locked, 0);
        assert_eq!(
            risk_engines[maker1_shard].get_balance(maker1_id, base_asset_id),
            event1.maker_balance[0]
        );
        assert_eq!(
            risk_engines[maker1_shard].get_balance(maker1_id, quote_asset_id),
            event1.maker_balance[1]
        );

        // --- Event 2: Trade with Maker 2 ---
        event_opt = event1.next_event.as_deref();
        let event2 = event_opt.unwrap();
        assert_eq!(event2.maker_user_id, maker2_id);
        assert_eq!(event2.size, maker2_size);
        assert!(event2.matched_order_completed);
        assert!(!event2.active_order_completed);
        assert_eq!(event2.maker_balance[0].available(), 0); // Maker
        assert_eq!(event2.maker_balance[1].available(), 99_900_000);
        assert_eq!(event2.maker_balance[1].locked, 0);
        assert_eq!(event2.maker_balance[0].locked, 0);
        assert_eq!(
            risk_engines[maker2_shard].get_balance(maker2_id, base_asset_id),
            event2.maker_balance[0]
        );
        assert_eq!(
            risk_engines[maker2_shard].get_balance(maker2_id, quote_asset_id),
            event2.maker_balance[1]
        );

        // --- Event 3: Trade with Maker 3 ---
        event_opt = event2.next_event.as_deref();
        let event3 = event_opt.unwrap();
        let maker3_trade_size = 15_000;
        assert_eq!(event3.maker_user_id, maker3_id);
        assert_eq!(event3.size, maker3_trade_size);
        assert!(
            !event3.matched_order_completed,
            "Maker 3 order should be partially filled"
        );
        assert!(
            event3.active_order_completed,
            "Taker order should be completed"
        );
        assert_eq!(
            event3.maker_balance[0].locked(),
            maker3_size - maker3_trade_size
        ); // Maker 3 partially sold
        assert_eq!(event3.maker_balance[0].available(), 0); // the rest is in locked
        assert_eq!(event3.maker_balance[1].available(), 74_925_000);
        assert_eq!(event3.maker_balance[1].locked(), 0);
        assert_eq!(
            risk_engines[maker3_shard].get_balance(maker3_id, base_asset_id),
            event3.maker_balance[0]
        );
        assert_eq!(
            risk_engines[maker3_shard].get_balance(maker3_id, quote_asset_id),
            event3.maker_balance[1]
        );
        assert!(event3.next_event.is_none(), "There should be only 3 events");

        // --- Verify Maker Balances from Events and Risk Engine State ---
        for (event, maker_id, maker_shard, expected_base_locked, expected_quote) in [
            (event1, maker1_id, maker1_shard, 0, 49_950_000),
            (event2, maker2_id, maker2_shard, 0, 99_900_000),
            (event3, maker3_id, maker3_shard, 15_000, 74_925_000),
        ] {
            assert_eq!(event.maker_balance[0].available, 0);
            assert_eq!(event.maker_balance[0].locked, expected_base_locked);
            assert_eq!(event.maker_balance[1].available, expected_quote);
            assert_eq!(event.maker_balance[1].locked, 0);

            assert_eq!(
                risk_engines[maker_shard].get_balance(maker_id, base_asset_id),
                event.maker_balance[0]
            );
            assert_eq!(
                risk_engines[maker_shard].get_balance(maker_id, quote_asset_id),
                event.maker_balance[1]
            );
        }

        // --- Final Taker Balance Verification ---
        assert_eq!(
            taker_filled_cmd.balance[0].total(), // base
            44_910,
            "Taker's base balance is incorrect"
        );
        assert_eq!(
            taker_filled_cmd.balance[1].total(), // quote
            0,
            "Taker's quote balance should be 0"
        );
        assert_eq!(
            risk_engines[taker_shard].get_balance(taker_id, base_asset_id),
            taker_filled_cmd.balance[0]
        );
        assert_eq!(
            risk_engines[taker_shard].get_balance(taker_id, quote_asset_id),
            taker_filled_cmd.balance[1]
        );

        // --- No more events ---
        assert!(
            event3.next_event.is_none(),
            "There should be no more events"
        );
    }

    #[test]
    fn test_ioc_market_order_partial_fill_and_cancel() {
        // 1. Setup
        let mut specs = HashMap::new();
        let base_asset_id = 1;
        let quote_asset_id = 2;
        let market_id = ((quote_asset_id as u32) << 16) | (base_asset_id as u32);
        add_spec(market_id, &mut specs);
        let (mut producer, risk_engines, rx) = setup_tuple(specs.clone());

        let price_cache = Arc::new(PriceCache::new(specs.keys()));

        let maker_id = 101; // Shard 1
        let taker_id = 42; // Shard 2
        let maker_shard = 1;
        let taker_shard = 2;

        // 2. Fund accounts
        let maker_ask_price = 50_000;
        let maker_ask_size = 5_000;
        risk_engines[maker_shard].set_balance(
            maker_id,
            base_asset_id,
            UserBalance::new(maker_ask_size, 0),
        ); // Fund BASE to sell

        let taker_initial_quote = 1_000_000_000;
        risk_engines[taker_shard].set_balance(
            taker_id,
            quote_asset_id,
            UserBalance::new(taker_initial_quote, 0), // Fund QUOTE to buy
        );

        // 3. Place Maker's resting ASK order
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                maker_id,
                maker_ask_price,
                maker_ask_size,
                Side::Ask,
                market_id,
                2,
            );
        });
        let maker_placed = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(maker_placed.status, Status::Placed);
        price_cache.update_prices(market_id, 0, maker_ask_price); // Manually update price cache

        // 4. Place Taker's IOC Market Buy order, larger than available liquidity
        let taker_order_size = 10_000;
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Ioc,
                taker_id,
                u64::MAX, // Market order price
                taker_order_size,
                Side::Bid,
                market_id,
                1,
            );
        });

        // 5. Wait for Taker's partially filled command
        let taker_cmd = rx.recv_timeout(Duration::from_secs(5)).unwrap();

        // 6. Assertions
        assert_eq!(taker_cmd.status, Status::PartiallyFilled);
        assert_eq!(taker_cmd.size, taker_order_size - maker_ask_size); // Remaining size

        let event = taker_cmd.events().unwrap();
        assert_eq!(event.size, maker_ask_size);
        assert_eq!(event.price, maker_ask_price);
        assert!(event.matched_order_completed); // Maker order was fully filled

        // --- Verify Balances ---
        // Taker (Buyer, BID)
        let taker_quote_spent = maker_ask_price * maker_ask_size;

        let taker_final_base = risk_engines[taker_shard].get_balance(taker_id, base_asset_id);
        let taker_final_quote = risk_engines[taker_shard].get_balance(taker_id, quote_asset_id);

        assert_eq!(taker_final_base.total(), 4_990);
        assert_eq!(
            taker_final_quote.total(),
            taker_initial_quote - taker_quote_spent
        );
        assert_eq!(
            taker_final_quote.locked, 0,
            "All locked funds for taker should be settled or released"
        );

        // Maker (Seller, ASK)
        let maker_final_base = risk_engines[maker_shard].get_balance(maker_id, base_asset_id);
        let maker_final_quote = risk_engines[maker_shard].get_balance(maker_id, quote_asset_id);

        assert_eq!(maker_final_base.total(), 0);
        assert_eq!(maker_final_quote.total(), 249_750_000);
    }

    #[test]
    fn test_market_buy_stops_at_slippage_cap_with_liquidity_beyond_it() {
        let mut specs = HashMap::new();
        let base_asset_id = 1;
        let quote_asset_id = 2;
        let market_id = ((quote_asset_id as u32) << 16) | (base_asset_id as u32);
        specs.insert(
            market_id,
            CoreMarketSpecification::builder()
                .market_id(market_id)
                .market_type(MarketType::Spot)
                .maker_fee(10)
                .taker_fee(20)
                .slippage(500)
                .build()
                .unwrap(),
        );

        let (mut producer, risk_engines, rx) = setup_tuple(specs);
        let maker_id = 101;
        let taker_id = 42;
        let maker_shard = 1;
        let taker_shard = 2;

        risk_engines[maker_shard].set_balance(maker_id, base_asset_id, UserBalance::new(30, 0));
        risk_engines[taker_shard].set_balance(taker_id, quote_asset_id, UserBalance::new(3_150, 0));

        for (client_order_id, price) in [(1, 100), (2, 110), (3, 120)] {
            producer.publish(|cmd| {
                *cmd = OrderCommand::place_order(
                    TimeInForce::Gtc,
                    maker_id,
                    price,
                    10,
                    Side::Ask,
                    market_id,
                    client_order_id,
                );
            });
            let response = rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(response.status, Status::Placed);
        }

        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Ioc,
                taker_id,
                u64::MAX,
                30,
                Side::Bid,
                market_id,
                4,
            );
        });
        let response = rx.recv_timeout(Duration::from_secs(5)).unwrap();

        assert_eq!(response.status, Status::PartiallyFilled);
        assert_eq!(
            response.price, 105,
            "response must expose the effective cap"
        );
        assert_eq!(
            response.size, 20,
            "quantity above the cap must remain unfilled"
        );

        let fill = response.events().unwrap();
        assert_eq!((fill.price, fill.size), (100, 10));
        assert!(
            fill.next_event.is_none(),
            "levels above the cap must not trade"
        );

        let l2 = response.l2_data.as_ref().unwrap();
        assert_eq!(l2.ask_prices, [110, 120]);
        assert_eq!(l2.ask_volumes, [10, 10]);
    }

    #[test]
    fn test_fok_limit_order_rejection_and_success() {
        // 1. Setup
        let mut specs = HashMap::new();
        let base_asset_id = 1;
        let quote_asset_id = 2;
        let market_id = ((quote_asset_id as u32) << 16) | (base_asset_id as u32);
        add_spec(market_id, &mut specs);
        let (mut producer, risk_engines, rx) = setup_tuple(specs);

        let maker_id = 101;
        let taker_id = 42;
        let maker_shard = 1;
        let taker_shard = 2;

        // 2. Fund accounts
        risk_engines[maker_shard].set_balance(maker_id, base_asset_id, UserBalance::new(100, 0)); // Fund BASE to sell
        let taker_initial_quote = 10_000_000;
        risk_engines[taker_shard].set_balance(
            taker_id,
            quote_asset_id,
            UserBalance::new(taker_initial_quote, 0),
        ); // Fund QUOTE to buy

        // 3. Place Maker's resting ASK order
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                maker_id,
                50_000,
                100,
                Side::Ask,
                market_id,
                2,
            );
        });
        let maker_placed_cmd = rx.recv().unwrap(); // Consume maker's placed command
        assert_eq!(maker_placed_cmd.status, Status::Placed);
        assert!(maker_placed_cmd.events().is_none());
        // balance must be locked for the maker
        assert_eq!(maker_placed_cmd.balance[0].locked, 100);
        assert_eq!(maker_placed_cmd.balance[0].available, 0);
        assert_eq!(maker_placed_cmd.balance[1].locked, 0);
        assert_eq!(maker_placed_cmd.balance[1].available, 0);

        // --- Part 1: FOK Rejection ---

        // 4. Place Taker's FOK order that is too large
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Fok,
                taker_id,
                50_000,
                101,
                Side::Bid,
                market_id,
                1,
            );
        });

        // 5. Assert rejection
        let taker_rejected_cmd = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(taker_rejected_cmd.status, Status::Cancelled);
        assert!(taker_rejected_cmd.events().is_none());

        // Balances should be unchanged
        let taker_balance_after_rejection =
            risk_engines[taker_shard].get_balance(taker_id, quote_asset_id);
        assert_eq!(taker_balance_after_rejection.total(), taker_initial_quote);
        assert_eq!(taker_balance_after_rejection.locked, 0);

        // --- Part 2: FOK Success with Price Improvement ---

        // 6. Place another maker order to provide enough liquidity with price improvement
        let maker2_id = 105; // Shard 1
        risk_engines[maker_shard].set_balance(maker2_id, base_asset_id, UserBalance::new(100, 0));
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                maker2_id,
                49_000,
                100,
                Side::Ask,
                market_id,
                2,
            );
        });
        rx.recv().unwrap(); // Consume maker2's placed command

        // 7. Place Taker's FOK order that can now be filled
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Fok,
                taker_id,
                50_000,
                150,
                Side::Bid,
                market_id,
                2,
            );
        });

        // 8. Assert success
        let taker_filled_cmd = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(taker_filled_cmd.status, Status::Filled);

        let mut event_opt = taker_filled_cmd.events();
        let event1 = event_opt.unwrap(); // Match with maker2 at 49k
        event_opt = event1.next_event.as_deref();
        let event2 = event_opt.unwrap(); // Match with maker1 at 50k

        assert_eq!(event1.price, 49_000);
        assert_eq!(event1.size, 100);
        assert_eq!(event2.price, 50_000);
        assert_eq!(event2.size, 50);

        // 9. Verify final balances
        let taker_quote_spent = (100 * 49_000) + (50 * 50_000);
        let taker_final_quote = risk_engines[taker_shard].get_balance(taker_id, quote_asset_id);

        assert_eq!(
            taker_final_quote.total(),
            taker_initial_quote - taker_quote_spent,
            "Taker's final quote balance is incorrect"
        );
        assert_eq!(taker_final_quote.locked, 0);
    }

    #[test_log::test]
    fn test_complex_scenario_with_cancellations_and_mixed_tif() {
        // This test simulates a more complex, realistic trading session involving:
        // - Building an order book with multiple makers.
        // - A GTC order that partially fills and then rests on the book.
        // - A mid-session order cancellation.
        // - An IOC order that partially fills against the resting GTC order.
        // - Verification of balances for all parties at each stage, including fee calculations
        //   and lock/unlock mechanics for both base and quote assets.

        // 1. Setup
        let mut specs = HashMap::new();
        let base_asset_id = 1;
        let quote_asset_id = 2;
        // Market ID: base asset in lower 16 bits, quote in upper 16
        let market_id = ((quote_asset_id as u32) << 16) | (base_asset_id as u32);
        add_spec(market_id, &mut specs);
        let (mut producer, risk_engines, rx) = setup_tuple(specs);

        let shard_mask = risk_engines.len() as u64 - 1;
        let get_shard_id = |user_id: u64| (user_id & shard_mask) as usize;

        // Define users and their shards
        let maker_a_id = 101; // Shard 1
        let maker_b_id = 102; // Shard 2
        let maker_c_id = 103; // Shard 3
        let taker_d_id = 50; // Shard 2
        let taker_e_id = 51; // Shard 3

        let maker_a_shard = get_shard_id(maker_a_id);
        let maker_b_shard = get_shard_id(maker_b_id);
        let maker_c_shard = get_shard_id(maker_c_id);
        let taker_d_shard = get_shard_id(taker_d_id);
        let taker_e_shard = get_shard_id(taker_e_id);

        // Fund accounts: Makers (ASK) need BASE to sell, Takers (BID) need QUOTE to buy.
        risk_engines[maker_a_shard].set_balance(
            maker_a_id,
            base_asset_id,
            UserBalance::new(100, 0),
        );
        risk_engines[maker_b_shard].set_balance(
            maker_b_id,
            base_asset_id,
            UserBalance::new(100, 0),
        );
        risk_engines[maker_c_shard].set_balance(
            maker_c_id,
            base_asset_id,
            UserBalance::new(100, 0),
        );
        risk_engines[taker_d_shard].set_balance(
            taker_d_id,
            quote_asset_id,
            UserBalance::new(1_000_000, 0),
        );
        risk_engines[taker_e_shard].set_balance(
            taker_e_id,
            base_asset_id,
            UserBalance::new(100, 0),
        ); // Taker E is ASK, needs BASE to sell QUOTE

        // --- Phase 1: Build the Order Book ---
        // Maker A places GTC ASK for 10 BASE @ 1010
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                maker_a_id,
                1010,
                10,
                Side::Ask,
                market_id,
                1,
            );
        });
        let placed_a = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(placed_a.status, Status::Placed);
        assert_eq!(
            risk_engines[maker_a_shard].get_balance(maker_a_id, base_asset_id),
            UserBalance::new(90, 10)
        );
        assert_eq!(placed_a.balance[0].locked, 10);
        assert_eq!(placed_a.balance[0].available, 90);
        assert_eq!(placed_a.balance[1].locked, 0);
        assert_eq!(placed_a.balance[1].available, 0);
        // Check that the balance in the command matches the risk engine state
        assert_eq!(
            placed_a.balance[0],
            risk_engines[maker_a_shard].get_balance(maker_a_id, base_asset_id)
        );
        assert_eq!(
            placed_a.balance[1],
            risk_engines[maker_a_shard].get_balance(maker_a_id, quote_asset_id)
        );

        // Maker B places GTC ASK for 15 BASE @ 1020
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                maker_b_id,
                1020,
                15,
                Side::Ask,
                market_id,
                2,
            );
        });
        let placed_b = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(placed_b.status, Status::Placed);
        assert_eq!(
            risk_engines[maker_b_shard].get_balance(maker_b_id, base_asset_id),
            UserBalance::new(85, 15)
        );
        assert_eq!(placed_b.balance[0].locked, 15);
        assert_eq!(placed_b.balance[0].available, 85);
        assert_eq!(placed_b.balance[1].locked, 0);
        assert_eq!(placed_b.balance[1].available, 0);
        // Check that the balance in the command matches the risk engine state
        assert_eq!(
            placed_b.balance[0],
            risk_engines[maker_b_shard].get_balance(maker_b_id, base_asset_id)
        );
        assert_eq!(
            placed_b.balance[1],
            risk_engines[maker_b_shard].get_balance(maker_b_id, quote_asset_id)
        );

        // Maker C places GTC ASK for 20 BASE @ 1020 (after B)
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                maker_c_id,
                1020,
                20,
                Side::Ask,
                market_id,
                3,
            );
        });
        let placed_c = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(placed_c.status, Status::Placed);
        assert_eq!(
            risk_engines[maker_c_shard].get_balance(maker_c_id, base_asset_id),
            UserBalance::new(80, 20)
        );
        assert_eq!(placed_c.balance[0].locked, 20);
        assert_eq!(placed_c.balance[0].available, 80);
        assert_eq!(placed_c.balance[1].locked, 0);
        assert_eq!(placed_c.balance[1].available, 0);
        // Check that the balance in the command matches the risk engine state
        assert_eq!(
            placed_c.balance[0],
            risk_engines[maker_c_shard].get_balance(maker_c_id, base_asset_id)
        );
        assert_eq!(
            placed_c.balance[1],
            risk_engines[maker_c_shard].get_balance(maker_c_id, quote_asset_id)
        );

        // --- Phase 2: Taker GTC order that partially fills and rests ---
        // Taker D places GTC BID for 30 BASE @ 1020
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                taker_d_id,
                1020,
                30,
                Side::Bid,
                market_id,
                4,
            );
        });

        // Taker D's order will match A (10@1010) and B (15@1020), then 5 will rest.
        let taker_d_cmd = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(taker_d_cmd.status, Status::Filled);
        assert_eq!(
            taker_d_cmd.size, 0,
            "0 units should be remaining as the order is fully filled"
        );
        // balance must be locked for the resting part of the order
        assert_eq!(taker_d_cmd.balance[1].locked, 0);
        assert_eq!(
            taker_d_cmd.balance[1].available,
            1_000_000 - ((10 * 1010) + (15 * 1020) + (5 * 1020))
        ); // quote after spending
        // gets equivalent base - taker fee
        assert_eq!(taker_d_cmd.balance[0].locked, 0);
        let gross_base_recv = 30;
        let taker_fee = [10u64, 15, 5]
            .map(|fill_size| (fill_size * 20).div_ceil(10000))
            .into_iter()
            .sum::<u64>();
        assert_eq!(
            taker_d_cmd.balance[0].available,
            gross_base_recv - taker_fee
        ); // base after 20bp fee

        // Verify balances after Taker D's trade
        // Maker A (fully filled)
        let maker_a_quote_recv = 10u64 * 1010;
        let maker_a_fee = (maker_a_quote_recv * 10).div_ceil(10000); // 10bp on quote
        assert_eq!(
            risk_engines[maker_a_shard].get_balance(maker_a_id, base_asset_id),
            UserBalance::new(90, 0)
        );
        assert_eq!(
            risk_engines[maker_a_shard].get_balance(maker_a_id, quote_asset_id),
            // 10_090 pre-#163 (floor fee 10); #163's div_ceil makes the 10 bp fee 11.
            UserBalance::new(maker_a_quote_recv - maker_a_fee, 0)
        );

        // Maker B (fully filled)
        let maker_b_quote_recv = 15u64 * 1020;
        let maker_b_fee = (maker_b_quote_recv * 10).div_ceil(10000); // 10bp on quote
        assert_eq!(
            risk_engines[maker_b_shard].get_balance(maker_b_id, base_asset_id),
            UserBalance::new(85, 0)
        );
        assert_eq!(
            risk_engines[maker_b_shard].get_balance(maker_b_id, quote_asset_id),
            // 15_285 pre-#163 (floor fee 15); #163's div_ceil makes it 16.
            UserBalance::new(maker_b_quote_recv - maker_b_fee, 0)
        );

        // Maker C (partially filled, 5 remaining)
        let maker_c_base_sold = 5u64; // Out of 20, 5 sold
        let maker_c_quote_recv = maker_c_base_sold * 1020;
        let maker_c_fee = (maker_c_quote_recv * 10).div_ceil(10000); // 10bp on quote
        assert_eq!(
            risk_engines[maker_c_shard].get_balance(maker_c_id, quote_asset_id),
            // 5_095 pre-#163 (floor fee 5); #163's div_ceil makes it 6.
            UserBalance::new(maker_c_quote_recv - maker_c_fee, 0)
        );
        assert_eq!(
            risk_engines[maker_c_shard].get_balance(maker_c_id, base_asset_id),
            UserBalance::new(80, 15)
        ); // 15 BASE still locked

        // --- Phase 3: Cancel an Order ---
        // Maker C cancels their order of the remaining (20 - 5) BASE @ 1020
        let maker_c_order_id = placed_c.order_id; // Use the actual generated order ID
        producer.publish(|cmd| {
            *cmd = OrderCommand::cancel_order(maker_c_order_id, maker_c_id, Side::Ask, market_id);
        });
        let cancelled_c = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(cancelled_c.status, Status::Cancelled);
        assert_eq!(
            risk_engines[maker_c_shard].get_balance(maker_c_id, base_asset_id),
            UserBalance::new(95, 0) // locked 15 is freed now
        );

        // --- Phase 4: IOC Order ---
        // Taker E places IOC ASK for 10 BASE @ 1020. This should be cancelled as order book is empty now.
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Ioc,
                taker_e_id,
                1020,
                10,
                Side::Ask,
                market_id,
                5,
            );
        });

        // Taker E's order will match 5 units from Taker D's resting order. 5 units will be cancelled.
        let taker_e_cmd = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(taker_e_cmd.status, Status::Cancelled);
        assert_eq!(
            taker_e_cmd.size, 10,
            "10 units should be remaining as everything was cancelled"
        );

        // Verify final balances after all trades
        // Taker E's balances should not change as their order was fully cancelled
        assert_eq!(
            risk_engines[taker_e_shard].get_balance(taker_e_id, base_asset_id),
            UserBalance::new(100, 0)
        );
        assert_eq!(
            risk_engines[taker_e_shard].get_balance(taker_e_id, quote_asset_id),
            UserBalance::new(0, 0)
        );

        // Taker D (taker buyer)
        let taker_d_final_base = risk_engines[taker_d_shard].get_balance(taker_d_id, base_asset_id);
        let taker_d_final_quote =
            risk_engines[taker_d_shard].get_balance(taker_d_id, quote_asset_id);

        // Taker D received:
        // As taker: 10 (from A) + 15 (from B) + 5 (from C). Fee is 20bp on base,
        // rounded up per fill. Each fill pays 1 BASE in fees, so the net is 27.
        assert_eq!(
            taker_d_final_base.total(),
            27,
            "Taker D final base balance is wrong"
        );

        // Taker D quote spent:
        // As taker: (10 * 1010) + (15 * 1020) = 10100 + 15300 = 25400
        // As maker: (5 * 1020) = 5100
        // Total spent: 25400 + 5100 = 30500
        // Final quote: 1,000,000 - 30500 = 969_500
        assert_eq!(
            taker_d_final_quote.total(),
            969_500,
            "Taker D final quote balance is wrong"
        );
        assert_eq!(
            taker_d_final_quote.locked, 0,
            "Taker D should have no locked quote funds"
        );
    }

    #[test_log::test]
    fn test_multi_market_stress_scenario() {
        // This test simulates a high-volatility session across three interconnected markets:
        // - BTC/USD: A high-liquidity pair.
        // - ETH/USD: Shares the QUOTE asset (USD) with BTC/USD.
        // - SOL/BTC: A crypto-cross pair, sharing BASE (BTC) with BTC/USD.
        // The test verifies:
        // - Correct balance locking and updating across shared assets.
        // - Proper handling of GTC, IOC, and FOK orders.
        // - Risk engine rejections due to insufficient funds locked in other markets.
        // - Mid-session cancellations and their impact on liquidity and balances.
        // - Complex fee calculations for makers and takers across multiple trades.

        // --- Phase 0: Environment Setup ---
        let mut specs = HashMap::new();
        let btc_asset = 1;
        let usd_asset = 2;
        let eth_asset = 3;
        let sol_asset = 4;

        let market_btc_usd = ((usd_asset as u32) << 16) | (btc_asset as u32);
        let market_eth_usd = ((usd_asset as u32) << 16) | (eth_asset as u32);
        let market_sol_btc = ((btc_asset as u32) << 16) | (sol_asset as u32);

        add_spec(market_btc_usd, &mut specs);
        add_spec(market_eth_usd, &mut specs);
        add_spec(market_sol_btc, &mut specs);

        let (mut producer, risk_engines, rx) = setup_tuple(specs);
        let checker = BalanceChecker::new(&risk_engines);

        // Define users with clear roles
        let alice_mm_id = 101; // Market Maker for BTC/USD and ETH/USD
        let bob_taker_id = 102; // Aggressive Taker across all markets
        let charlie_fok_ioc_id = 103; // Retail trader using FOK/IOC
        let david_sol_mm_id = 104; // Market Maker for SOL/BTC

        // Fund accounts
        // Alice needs BTC, ETH, and USD to make markets
        checker.set_balance(alice_mm_id, btc_asset, 10_000, 0); // 10k BTC
        checker.set_balance(alice_mm_id, eth_asset, 50_000, 0); // 50k ETH
        checker.set_balance(alice_mm_id, usd_asset, 100_000_000, 0); // 100M USD

        // Bob needs USD and BTC to be a taker
        checker.set_balance(bob_taker_id, usd_asset, 10_000_000, 0); // 10M USD
        // CORRECTED: Increased Bob's BTC to support his large SOL trade
        checker.set_balance(bob_taker_id, btc_asset, 5_000_000, 0); // 5M BTC

        // Charlie needs a bit of everything for his special orders
        checker.set_balance(charlie_fok_ioc_id, usd_asset, 100_000_000, 0); // 100M USD
        checker.set_balance(charlie_fok_ioc_id, btc_asset, 1_000, 0); // 1k BTC

        // David needs SOL and BTC to make the SOL/BTC market
        checker.set_balance(david_sol_mm_id, sol_asset, 1_000_000, 0); // 1M SOL
        // CORRECTED: Increased David's BTC to support his large market making bid
        checker.set_balance(david_sol_mm_id, btc_asset, 5_000_000, 0); // 5M BTC

        // --- Phase 1: Building the Order Books ---
        // Alice builds depth in BTC/USD
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                alice_mm_id,
                50000,
                1000,
                Side::Ask,
                market_btc_usd,
                1,
            )
        });
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                alice_mm_id,
                50100,
                1500,
                Side::Ask,
                market_btc_usd,
                2,
            )
        });

        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                alice_mm_id,
                49900,
                1000,
                Side::Bid,
                market_btc_usd,
                3,
            )
        });

        // Alice builds depth in ETH/USD
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                alice_mm_id,
                3000,
                20000,
                Side::Ask,
                market_eth_usd,
                4,
            )
        });

        // David builds depth in SOL/BTC
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                david_sol_mm_id,
                30,
                50000,
                Side::Ask,
                market_sol_btc,
                5,
            )
        }); // Price: 30 BTC for 1 SOL
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                david_sol_mm_id,
                29,
                50000,
                Side::Bid,
                market_sol_btc,
                6,
            )
        });
        // Consume placement messages
        for _ in 0..6 {
            rx.recv().unwrap();
        }

        // Sanity check Alice's and David's locked balances
        checker.check(
            alice_mm_id,
            btc_asset,
            10000 - 2500,
            2500,
            "Alice BTC locked for asks",
        );
        checker.check(
            alice_mm_id,
            usd_asset,
            100_000_000 - (1000 * 49900),
            1000 * 49900,
            "Alice USD locked for bid",
        );
        checker.check(
            alice_mm_id,
            eth_asset,
            50000 - 20000,
            20000,
            "Alice ETH locked for ask",
        );
        checker.check(
            david_sol_mm_id,
            sol_asset,
            1_000_000 - 50000,
            50000,
            "David SOL locked for ask",
        );
        checker.check(
            david_sol_mm_id,
            btc_asset,
            5_000_000 - (50000 * 29),
            50000 * 29,
            "David BTC locked for bid",
        );

        // --- Phase 2: IOC and Partial Fill ---
        // Charlie places an IOC buy for ETH that can only partially fill.
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Ioc,
                charlie_fok_ioc_id,
                3000,
                25000,
                Side::Bid,
                market_eth_usd,
                1,
            )
        });
        let ioc_result = rx.recv().unwrap();

        assert_eq!(
            ioc_result.status,
            Status::PartiallyFilled,
            "IOC should be marked Partially Filled as it traded"
        );
        assert_eq!(
            ioc_result.events.as_ref().unwrap().calc_filled_size(),
            20000,
            "IOC should fill 20000 units"
        );
        assert_eq!(
            ioc_result.size, 5000,
            "IOC should have 5000 units remaining (cancelled part)"
        );

        // Check balances post-IOC. Charlie's locked USD for the unfilled 5000 ETH should be free.
        checker.check(
            charlie_fok_ioc_id,
            usd_asset,
            40_000_000,
            0,
            "Charlie's USD spent on ETH",
        );
        checker.check(
            charlie_fok_ioc_id,
            eth_asset,
            19_960,
            0,
            "Charlie received ETH minus taker fee",
        );

        // Alice's ETH ask was fully filled
        checker.check(alice_mm_id, eth_asset, 30000, 0, "Alice's ETH ask is gone");
        checker.check(
            alice_mm_id,
            usd_asset,
            110_040_000,
            1000 * 49900,
            "Alice received USD minus maker fee",
        );

        // --- Phase 3: FOK Rejection and Success ---
        // Bob tries to buy 50001 SOL with BTC. David is only offering 50000. It must fail.
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Fok,
                bob_taker_id,
                30,
                50001,
                Side::Bid,
                market_sol_btc,
                1,
            )
        });
        let fok_fail = rx.recv().unwrap();
        assert_eq!(
            fok_fail.status,
            Status::Cancelled,
            "FOK should be cancelled if it cannot fully fill"
        );

        // Bob's balances must be completely unchanged.
        checker.check(
            bob_taker_id,
            btc_asset,
            5_000_000,
            0,
            "Bob's BTC unchanged after FOK fail",
        );
        checker.check(
            bob_taker_id,
            sol_asset,
            0,
            0,
            "Bob's SOL unchanged after FOK fail",
        );

        // Now Bob places a FOK that CAN be filled.
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Fok,
                bob_taker_id,
                30,
                40000,
                Side::Bid,
                market_sol_btc,
                2,
            )
        });
        let fok_success = rx.recv().unwrap();
        assert_eq!(fok_success.status, Status::Filled, "FOK should be filled");
        assert_eq!(
            fok_success.events.as_ref().unwrap().calc_filled_size(),
            40000,
            "FOK should fill 40000 units"
        );

        // Check balances after successful FOK
        let bob_btc_cost = 1_200_000;
        checker.check(
            bob_taker_id,
            btc_asset,
            3_800_000,
            0,
            "Bob spent BTC on SOL",
        );
        checker.check(
            bob_taker_id,
            sol_asset,
            39_920,
            0,
            "Bob received SOL minus fee",
        );

        checker.check(
            david_sol_mm_id,
            sol_asset,
            1_000_000 - 50000,
            10000,
            "David has 10k SOL remaining on his ask",
        );
        checker.check(
            david_sol_mm_id,
            btc_asset,
            4_748_800,
            50000 * 29,
            "David received BTC for SOL minus fee",
        );

        // --- Phase 4: Cross-Market Insufficient Funds Rejection ---
        // Bob now tries to sweep the BTC/USD book with a huge order.
        // He wants to buy 3000 BTC @ 50100. Cost = 150,300,000 USD. He has 10M USD. This is far too little.
        // However, he will first place a smaller GTC order to lock up most of his funds.
        let bob_btc_before_ask = 5_000_000 - bob_btc_cost;
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                bob_taker_id,
                49901,
                200,
                Side::Ask,
                market_btc_usd,
                4,
            )
        });
        let gtc_ask = rx.recv().unwrap(); // this will sit on the book
        assert_eq!(gtc_ask.status, Status::Placed);
        let bob_gtc_order_id = gtc_ask.order_id;

        // Bob's 200 BTC are now locked.
        checker.check(
            bob_taker_id,
            btc_asset,
            bob_btc_before_ask - 200,
            200,
            "Bob's BTC locked for GTC ask",
        );
        // NOW, he tries to buy 2000 BTC @ 50100. Estimated cost = 100,200,000 USD.
        // He has 10M USD, so this must be rejected by the risk engine.
        producer.publish(|cmd| {
            *cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                bob_taker_id,
                50100,
                2000,
                Side::Bid,
                market_btc_usd,
                4,
            )
        });
        let rejected_bid = rx.recv().unwrap();
        assert_eq!(
            rejected_bid.status,
            Status::Rejected,
            "Order should be rejected due to insufficient USD funds"
        );

        // CRUCIAL: Verify Bob's balances are unchanged by the rejected order.
        checker.check(
            bob_taker_id,
            btc_asset,
            bob_btc_before_ask - 200,
            200,
            "Bob's BTC unaffected by rejected order",
        );
        checker.check(
            bob_taker_id,
            usd_asset,
            10_000_000,
            0,
            "Bob's USD unaffected by rejected order",
        );
        // --- Phase 5: Final Clean-up and State Verification ---
        // Bob cancels his resting GTC ask on BTC/USD
        producer.publish(|cmd| {
            *cmd = OrderCommand::cancel_order(
                bob_gtc_order_id,
                bob_taker_id,
                Side::Ask,
                market_btc_usd,
            )
        });
        let cancelled_bob = rx.recv().unwrap();
        assert_eq!(cancelled_bob.status, Status::Cancelled);

        // His locked 200 BTC should now be available.
        checker.check(
            bob_taker_id,
            btc_asset,
            bob_btc_before_ask,
            0,
            "Bob's BTC is fully available after cancel",
        );

        // Final state check could be added here for all users, but the intermediate checks
        // provide a more granular verification of the system's behavior under stress.
        println!("Multi-market stress test completed successfully.");
    }
}
