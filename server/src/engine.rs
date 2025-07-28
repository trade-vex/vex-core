use crate::events::EventsHandler;
use common::cmd::OrderCommand;
use disruptor::{
    BusySpin, MultiConsumerBarrier, MultiProducer, ProcessorSettings, build_multi_producer,
};
use processors::{
    journaling::JournalingProcessor, matching_engine::MatchingEngineRouter, risk_engine::RiskEngine,
};
use std::sync::Arc;
use tracing::{info, warn};

/// The central processing unit of the exchange, running a sequential pipeline.
/// This is the equivalent of `CoreEngine.java` orchestrating the processors.
pub struct CoreEngine {
    // Store the producer in an Option so it can be taken out for returning
    _producer: Option<MultiProducer<OrderCommand, MultiConsumerBarrier>>,
    risk_engine: Arc<std::sync::Mutex<RiskEngine>>,
    // Multiple routers that run in parallel
    matching_engine_routers: Vec<Arc<std::sync::Mutex<MatchingEngineRouter>>>,
}

impl CoreEngine {
    pub fn new(
        risk_engine: RiskEngine,
        journaling_processor: JournalingProcessor,
        events_handler: Arc<dyn EventsHandler>,
    ) -> (Self, MultiProducer<OrderCommand, MultiConsumerBarrier>) {
        let factory = || OrderCommand::default();
        let buffer_size = 1024;

        let journaling_arc = Arc::new(journaling_processor);
        let risk_engine_arc = Arc::new(std::sync::Mutex::new(risk_engine));
        let events_handler_arc = events_handler.clone();

        // Create multiple empty matching engine routers
        let num_matching_engines = 4; 
        let mut matching_engine_routers = Vec::new();

        for shard_id in 0..num_matching_engines {
            let router = MatchingEngineRouter::new(shard_id, num_matching_engines as i64);
            // Start with empty routers - symbols will be added via add_symbol() method
            matching_engine_routers.push(Arc::new(std::sync::Mutex::new(router)));
        }

        let journaling_arc_stage1 = journaling_arc.clone();

        let producer = build_multi_producer(buffer_size, factory, BusySpin)
            // Stage 1: Journaling on core 1
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
            // Stage 3: Matching Engine with Direct Symbol Routing
            // This matches the exchangeCore architecture's : each command goes to exactly router
            .pin_at_core(3)
            .handle_events_with({
                let matching_engine_routers_clone = matching_engine_routers.clone();
                let events_handler_clone = events_handler_arc.clone();
                let journaling_clone = journaling_arc.clone();
                let risk_engine_clone = risk_engine_arc.clone();

                move |cmd: &OrderCommand, _, _| {
                    // Direct routing: Find the router that owns this symbol and ONLY process there
                    let symbol = cmd.symbol as i64;
                    let router_index = (symbol & 3) as usize; // 4 routers, so mask with 3

                    if let Some(router) = matching_engine_routers_clone.get(router_index) {
                        let mut matching_engine = router.lock().unwrap();
                        let mut cmd_clone = cmd.clone();

                        // Process command with the correct router (already filtered by symbol ownership)
                        matching_engine.process_order(&mut cmd_clone);

                        // Process events from this router
                        let mut current_event = cmd_clone.matcher_event.take();
                        while let Some(event_box) = current_event {
                            let mut event = *event_box;
                            current_event = event.next_event.take();

                            journaling_clone.journal_event(&event);
                            let mut risk_engine = risk_engine_clone.lock().unwrap();
                            risk_engine.handle_event(&event);
                            events_handler_clone.handle_event(event.clone());
                        }
                    }
                }
            })
            .build();

        let mut engine = Self {
            _producer: Some(producer),
            risk_engine: risk_engine_arc.clone(),
            matching_engine_routers,
        };

        let producer = engine._producer.take().unwrap();
        (engine, producer)
    }

    /// Add a symbol to only the router that owns it (efficient memory usage)
    ///
    /// This is separate from runtime processing - we configure symbols efficiently,
    /// but runtime commands still go through symbol ownership checks.
    pub fn add_symbol(&self, symbol_id: i32, book_type: orderbook::OrderBookImplType) {
        // Calculate which router owns this symbol using the same sharding logic
        let num_shards = self.matching_engine_routers.len() as i64;
        let shard_mask = num_shards - 1; // Power of 2 mask
        let owner_shard_id = (symbol_id as i64) & shard_mask;
        let router_index = owner_shard_id as usize;

        // Only add symbol to the router that owns it
        if let Some(router) = self.matching_engine_routers.get(router_index) {
            let mut matching_engine = router.lock().unwrap();
            matching_engine.add_symbol(symbol_id, book_type);

            info!(
                "Added symbol {} to router {} (shard_id={})",
                symbol_id, router_index, owner_shard_id
            );
        } else {
            warn!(
                "Failed to add symbol {}: router index {} out of bounds",
                symbol_id, router_index
            );
        }
    }

    pub async fn run(&mut self) {
        info!("\n[Sequential Core] Engine started. Waiting for commands...");
        std::thread::park();
        info!("[Sequential Core] Engine stopped.");
    }

    pub fn get_user_balance(&self, uid: u64, currency: i32) -> Option<i64> {
        let risk_engine = self.risk_engine.lock().unwrap();
        risk_engine
            .user_profiles
            .get(&(uid as i64))
            .and_then(|profile| profile.accounts.get(&currency).copied())
    }

    /// Returns the filled quantity for the given order_id, searching all order books.
    pub fn get_order_filled(&self, order_id: i64) -> Option<i64> {
        // Search across all routers
        for router in &self.matching_engine_routers {
            let matching_engine = router.lock().unwrap();
            for order_book in matching_engine.get_order_books().values() {
                if let Some(order) = order_book.get_order_by_id(order_id) {
                    return Some(order.filled());
                }
            }
        }
        None
    }
}
