// Macros for creating handlers for risk and matching engines

/// Macro to generate risk engine handlers
/// This eliminates code duplication while maintaining separate handlers for each shard
#[macro_export]
macro_rules! create_risk_handler {
    ($shard_id:expr, $risk_engines:expr) => {{
        let risk_engines_clone = $risk_engines.clone();
        move |cmd: &mut OrderCommand, _sequence: i64, _end_of_batch: bool| {
            let mut engine = risk_engines_clone[$shard_id].lock();
            engine.pre_process_command(cmd);
        }
    }};
}

/// Macro to generate risk engine R2 handlers (sharded by user_id)
/// Each handler processes events for users that belong to its shard (both maker and taker)
#[macro_export]
macro_rules! create_risk_r2_handler {
    ($shard_id:expr, $risk_engines:expr) => {{
        let risk_engines_clone = $risk_engines.clone();
        move |processed_cmd: &mut OrderCommand, _sequence: i64, _end_of_batch: bool| {
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
    ($events_handler:expr, $risk_engines:expr, $matching_engine_routers:expr, $orderbook_depth:expr) => {{
        let events_handler = $events_handler;
        let risk_engines = $risk_engines;
        let matching_engine_routers = $matching_engine_routers;
        let orderbook_depth = $orderbook_depth;
        move |cmd: &mut OrderCommand, _sequence: i64, _end_of_batch: bool| {
            // Get the appropriate risk engine for the taker user
            let taker_id = cmd.user_id();
            let num_shards = risk_engines.len() as u64;
            let shard_mask = num_shards - 1;
            let taker_shard = (taker_id & shard_mask) as usize;

            let risk_engine = if let Some(risk_engine_mutex) = risk_engines.get(taker_shard) {
                Some(&*risk_engine_mutex.lock())
            } else {
                None
            };

            // Get the appropriate orderbook for the market using correct sharding
            let market_id = cmd.market_id();
            let num_matching_shards = matching_engine_routers.len() as u64;
            let matching_shard_mask = num_matching_shards - 1;
            let market_shard = (market_id as u64 & matching_shard_mask) as usize;

            // Create orderbook snapshot using the proper method with configurable depth
            let orderbook_snapshot =
                if let Some(router_mutex) = matching_engine_routers.get(market_shard) {
                    let router = router_mutex.lock();
                    router.create_orderbook_snapshot(market_id, orderbook_depth)
                } else {
                    None
                };

            // Handle the processed command (for Kafka events)
            events_handler.handle_processed_command(
                cmd,
                risk_engine.as_deref(),
                orderbook_snapshot,
            );
        }
    }};
}

/// Macro to generate matching engine handlers
/// This eliminates code duplication while maintaining separate handlers for each shard
#[macro_export]
macro_rules! create_matching_handler {
    ($shard_id:expr, $routers:expr, $price_cache:expr) => {{
        let router = $routers[$shard_id].clone();
        let price_cache = $price_cache.clone();
        move |cmd: &mut OrderCommand, _sequence: i64, _end_of_batch: bool| {
            // Only lock during order processing
            let processed_order_cmd = {
                let mut router_guard = router.lock();
                // remove this lock eventually by some minor optimsations in orderbook and matching engine router
                let mut order_cmd = cmd.clone();
                let processed_order_cmd = router_guard.process_order(&mut order_cmd, price_cache.clone());
                processed_order_cmd
            };  // Lock is released here - router is free for next order
        }
    }};
}
