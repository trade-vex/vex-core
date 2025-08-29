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
/// Each handler only processes events for users that belong to its shard
#[macro_export]
macro_rules! create_risk_r2_handler {
    ($shard_id:expr, $risk_engines:expr) => {{
        let risk_engines_clone = $risk_engines.clone();
        move |event: &MatcherTradeEvent, _sequence: i64, _end_of_batch: bool| {
            let num_shards = risk_engines_clone.len() as u64;
            let shard_mask = num_shards - 1;

            // Route to risk engine shard for active order user
            let active_order_user_id = event.active_order_user_id;
            let active_order_shard = (active_order_user_id & shard_mask) as usize;

            // Only process if this event belongs to our shard
            if active_order_shard == $shard_id {
                if let Some(risk_engine_mutex) = risk_engines_clone.get($shard_id) {
                    let mut risk_engine = risk_engine_mutex.lock();
                    risk_engine.handle_event(event);
                }
            }

            // Route to risk engine shard for maker user (if different)
            let maker_user_id = event.maker_user_id;
            if maker_user_id != active_order_user_id {
                let maker_shard = (maker_user_id & shard_mask) as usize;

                // Only process if this event belongs to our shard
                if maker_shard == $shard_id {
                    if let Some(risk_engine_mutex) = risk_engines_clone.get($shard_id) {
                        let mut risk_engine = risk_engine_mutex.lock();
                        risk_engine.handle_event(event);
                    }
                }
            }
        }
    }};
}

#[macro_export]
macro_rules! create_event_handler {
    ($events_handler:expr) => {{
        let events_handler = $events_handler.clone();
        move |event: &MatcherTradeEvent, _sequence: i64, _end_of_batch: bool| {
            events_handler.handle_event(event.clone());
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
            let events = {
                let mut router_guard = router.lock();
                // remove this lock eventually by some minor optimsations in orderbook and matching engine router
                let mut order_cmd = cmd.clone();
                router_guard.process_order(&mut order_cmd);
                order_cmd.matcher_event.take()
            };  // Lock is released here - router is free for next order

            // Publish raw events directly
            if let Some(mut event_box) = events {
                loop {
                    let mut event = *event_box;
                    let next_event = event.next_event.take();

                    // Publish raw event directly
                    let _ = matcher_event_producer.publish(|published_event| {
                        *published_event = event;
                    });

                    // Move to next event or break
                    match next_event {
                        Some(next) => event_box = next,
                        None => break,
                    }
                }
            }
        }
    }};
}
