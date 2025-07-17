use crate::events::EventsHandler;
use common::cmd::OrderCommand;
use disruptor::{build_multi_producer, BusySpin, MultiConsumerBarrier, MultiProducer, ProcessorSettings};
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
}

impl CoreEngine {
    pub fn new(
        risk_engine: RiskEngine,
        matching_engine_router: MatchingEngineRouter,
        journaling_processor: JournalingProcessor,
        events_handler: Arc<dyn EventsHandler>,
    ) -> (
        Self,
        MultiProducer<OrderCommand, MultiConsumerBarrier>,
    ) {
        let factory = || OrderCommand::default();
        let buffer_size = 1024;

        // Using Arc to share stateful processors with the main thread and the consumer threads.
        let journaling_arc = Arc::new(journaling_processor);
        let risk_engine_arc = Arc::new(std::sync::Mutex::new(risk_engine));
        let matching_engine_arc = Arc::new(std::sync::Mutex::new(matching_engine_router));
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
            .handle_events_with({
                let matching_engine_clone = matching_engine_arc.clone();
                let events_handler_clone = events_handler_arc.clone();
                let journaling_clone = journaling_arc.clone();
                let risk_engine_clone = risk_engine_arc.clone();

                move |cmd: &OrderCommand, _, _| {
                    let mut matching_engine = matching_engine_clone.lock().unwrap();
                    let mut cmd_clone = cmd.clone();
                    matching_engine.route_command(&mut cmd_clone);

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

                        let mut risk_engine = risk_engine_clone.lock().unwrap();

                        risk_engine.handle_event(&event);

                        events_handler_clone.handle_event(event.clone());
                    }
                }
            })
            .build();

        let mut engine = Self {
            _producer: Some(producer),
        };

        // Take the producer out of the engine for returning
        let producer = engine._producer.take().unwrap();
        // (engine, producer)
        (engine, producer)
    }

    pub async fn run(&mut self) {
        info!("\n[Sequential Core] Engine started. Waiting for commands...");
        std::thread::park();
        info!("[Sequential Core] Engine stopped.");
    }
}
