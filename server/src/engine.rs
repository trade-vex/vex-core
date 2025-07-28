use crate::events::EventsHandler;
use common::cmd::OrderCommand;
use disruptor::{build_multi_producer, BusySpin, MultiConsumerBarrier, MultiProducer, ProcessorSettings};
use processors::{
    journaling::JournalingProcessor, matching_engine::MatchingEngineRouter, risk_engine::RiskEngine,
};
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

/// The central processing unit of the exchange, running a sequential pipeline.
/// This is the equivalent of `CoreEngine.java` orchestrating the processors.
pub struct CoreEngine {
    // Store the producer in an Option so it can be taken out for returning
    _producer: Option<MultiProducer<OrderCommand, MultiConsumerBarrier>>,
    risk_engines: Arc<Vec<Mutex<RiskEngine>>>,
    matching_engine_routers: Arc<Vec<Mutex<MatchingEngineRouter>>>,
}

impl CoreEngine {
    pub fn new(
        risk_engines: Vec<RiskEngine>,
        matching_engine_routers: Vec<MatchingEngineRouter>,
        journaling_processor: JournalingProcessor,
        events_handler: Arc<dyn EventsHandler>,
    ) -> (
        Self,
        MultiProducer<OrderCommand, MultiConsumerBarrier>,
    ) {
        let factory = || OrderCommand::default();
        let buffer_size = 1024;

        let journaling_arc = Arc::new(journaling_processor);
        let risk_engines_arc: Arc<Vec<Mutex<RiskEngine>>> = Arc::new(risk_engines.into_iter().map(Mutex::new).collect());
        let matching_engine_arc: Arc<Vec<Mutex<MatchingEngineRouter>>> = Arc::new(matching_engine_routers.into_iter().map(Mutex::new).collect());
        let events_handler_arc = events_handler.clone();

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
                let risk_engines_clone = risk_engines_arc.clone();
                move |cmd: &OrderCommand, _, _| {
                    let mut cmd_clone = cmd.clone();
                    // Broadcast to all risk engine shards
                    for risk_engine_mutex in risk_engines_clone.iter() {
                        let mut risk_engine = risk_engine_mutex.lock().unwrap();
                        if let Err(e) = risk_engine.pre_process_command(&mut cmd_clone) {
                            warn!(
                                "[Disruptor Core] Risk check failed: {:?}. Rejecting command.",
                                e
                            );
                        }
                    }
                }
            })
            // Stage 3: Matching Engine + event handling on core 3
            .pin_at_core(3)
            .handle_events_with({
                let matching_engine_clone = matching_engine_arc.clone();
                let events_handler_clone = events_handler_arc.clone();
                let journaling_clone = journaling_arc.clone();
                let risk_engines_clone = risk_engines_arc.clone();
                move |cmd: &OrderCommand, _, _| {
                    let mut cmd_clone = cmd.clone();
                    // Broadcast to all matching engine shards
                    for matching_engine_mutex in matching_engine_clone.iter() {
                        let mut matching_engine = matching_engine_mutex.lock().unwrap();
                        matching_engine.process_order(&mut cmd_clone);
                    }

                    let mut current_event = cmd_clone.matcher_event.take();
                    if current_event.is_none() {
                        warn!(
                            "[Disruptor Core] No events generated for command ID: {}",
                            cmd_clone.order_id
                        );
                        return;
                    }

                    while let Some(event_box) = current_event {
                        let mut event = *event_box;
                        current_event = event.next_event.take();

                        journaling_clone.journal_event(&event);
                        // Broadcast event to all risk engine shards for settlement
                        for risk_engine_mutex in risk_engines_clone.iter() {
                            let mut risk_engine = risk_engine_mutex.lock().unwrap();
                            risk_engine.handle_event(&event);
                        }
                        events_handler_clone.handle_event(event.clone());
                    }
                }
            })
            .build();

        let mut engine = Self {
            _producer: Some(producer),
            risk_engines: risk_engines_arc.clone(),
            matching_engine_routers: matching_engine_arc.clone(),
        };
        let producer = engine._producer.take().unwrap();
        (engine, producer)
    }

    pub async fn run(&mut self) {
        info!("\n[Sequential Core] Engine started. Waiting for commands...");
        std::thread::park();
        info!("[Sequential Core] Engine stopped.");
    }

    pub fn get_user_balance(&self, uid: u64, currency: i32) -> Option<i64> {
        // Find the correct shard and query balance
        for risk_engine_mutex in self.risk_engines.iter() {
            let risk_engine = risk_engine_mutex.lock().unwrap();
            if let Some(balance) = risk_engine.user_profiles.get(&(uid as i64))
                .and_then(|profile| profile.accounts.get(&currency).copied()) {
                return Some(balance);
            }
        }
        None
    }

    /// Returns the filled quantity for the given order_id, searching all order books.
    pub fn get_order_filled(&self, order_id: i64) -> Option<i64> {
        // Assuming you have access to the matching_engine_router
        // and it is wrapped in Arc<Mutex<...>>
        for router_mutex in self.matching_engine_routers.iter() {
            let matching_engine = router_mutex.lock().unwrap();
            for order_book in matching_engine.order_books.values() {
                if let Some(order) = order_book.get_order_by_id(order_id) {
                    return Some(order.filled());
                }
            }
        }
        None
    }
}
