// Macros for creating handlers for risk and matching engines

/// Macro to generate risk engine handlers
/// This eliminates code duplication while maintaining separate handlers for each shard
#[macro_export]
macro_rules! create_risk_handler {
    ($shard_id:expr, $risk_engines:expr) => {{
        let risk_engines_clone = $risk_engines.clone();
        move |cmd: &OrderCommand, _sequence: i64, _end_of_batch: bool| {
            let mut engine = risk_engines_clone[$shard_id].lock();

            if let Err(e) = engine.pre_process_command(cmd) {
                warn!("[RiskEngine_{}] Risk check failed: {:?}", $shard_id, e);
                return;
            }
        }
    }};
}

/// Macro to generate risk engine R2 handlers (sharded by user_id)
/// Each handler processes events for users that belong to its shard (both maker and taker)
#[macro_export]
macro_rules! create_risk_r2_handler {
    ($shard_id:expr, $risk_engines:expr) => {{
        let risk_engines_clone = $risk_engines.clone();
        move |processed_cmd: &ProcessedOrderCommand, _sequence: i64, _end_of_batch: bool| {
            // Process the main event if it exists
            if let Some(event) = processed_cmd.events() {
                let num_shards = risk_engines_clone.len() as u64;
                let shard_mask = num_shards - 1;

                let market_id = processed_cmd.market_id();
                let taker_id = processed_cmd.taker_id();

                // Route to risk engine shard for both maker and taker users
                let maker_user_id = event.maker_user_id;
                let maker_shard = (maker_user_id & shard_mask) as usize;
                let taker_shard = (taker_id & shard_mask) as usize;

                // Process if either maker OR taker belongs to our shard
                if maker_shard == $shard_id || taker_shard == $shard_id {
                    if let Some(risk_engine_mutex) = risk_engines_clone.get($shard_id) {
                        let mut risk_engine = risk_engine_mutex.lock();
                        risk_engine.handle_event(event, market_id, taker_id);
                    }
                }

                // Process chained events if they exist
                let mut current_event = event.next_event.as_ref();
                while let Some(next_event) = current_event {
                    let next_maker_user_id = next_event.maker_user_id;
                    let next_maker_shard = (next_maker_user_id & shard_mask) as usize;

                    // Process chained events if maker OR taker belongs to our shard
                    if next_maker_shard == $shard_id || taker_shard == $shard_id {
                        if let Some(risk_engine_mutex) = risk_engines_clone.get($shard_id) {
                            let mut risk_engine = risk_engine_mutex.lock();
                            risk_engine.handle_event(next_event, market_id, taker_id);
                        }
                    }
                    current_event = next_event.next_event.as_ref();
                }
            }
        }
    }};
}

#[macro_export]
macro_rules! create_event_handler {
    ($events_handler:expr) => {{
        let events_handler = $events_handler.clone();
        move |processed_cmd: &ProcessedOrderCommand, _sequence: i64, _end_of_batch: bool| {
            // Handle the main event if it exists
            if let Some(event) = processed_cmd.events() {
                events_handler.handle_event(event.clone());
                
                // Handle all chained events
                let mut current_event = event.next_event.as_ref();
                while let Some(next_event) = current_event {
                    events_handler.handle_event((**next_event).clone());
                    current_event = next_event.next_event.as_ref();
                }
            }
        }
    }};
}

/// Macro to generate matching engine handlers
/// This eliminates code duplication while maintaining separate handlers for each shard
#[macro_export]
macro_rules! create_matching_handler {
    ($shard_id:expr, $routers:expr, $matcher_event_producer:expr) => {{
        use disruptor::Producer;
        let router = $routers[$shard_id].clone();
        let mut matcher_event_producer = $matcher_event_producer.clone();

        move |cmd: &OrderCommand, _sequence: i64, _end_of_batch: bool| {
            // Only lock during order processing
            let processed_order_cmd = {
                let mut router_guard = router.lock();
                // remove this lock eventually by some minor optimsations in orderbook and matching engine router
                let mut order_cmd = cmd.clone();
                let processed_order_cmd = router_guard.process_order(&mut order_cmd);
                processed_order_cmd
            };  // Lock is released here - router is free for next order

            // Publish raw events directly
            let _ = matcher_event_producer.publish(|published_event| {
                *published_event = processed_order_cmd;
            });
        }
    }};
}