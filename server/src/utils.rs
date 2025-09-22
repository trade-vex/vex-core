// Macros for creating handlers for risk and matching engines

/// Macro to generate risk engine handlers
/// This eliminates code duplication while maintaining separate handlers for each shard
#[macro_export]
macro_rules! create_risk_handler {
    ($shard_id:expr, $risk_engines:expr, $price_cache:expr) => {{
        let risk_engines = $risk_engines.clone();
        let price_cache = $price_cache.clone();
        move |cmd: &mut OrderCommand, _sequence: i64, _end_of_batch: bool| {
            let risk_engine = &risk_engines[$shard_id];
            risk_engine.pre_process_command(cmd, price_cache.clone());
        }
    }};
}

/// Macro to generate risk engine R2 handlers (sharded by user_id)
/// Each handler processes events for users that belong to its shard (both maker and taker)
#[macro_export]
macro_rules! create_risk_r2_handler {
    ($shard_id:expr, $risk_engines:expr) => {{
        let risk_engines = $risk_engines.clone();
        let shard_mask = $risk_engines.len() as u64 - 1;
        move |cmd: &mut OrderCommand, _sequence: i64, _end_of_batch: bool| {
            let risk_engine = &risk_engines[$shard_id];
            let taker_id = cmd.user_id();
            let market_id = cmd.market_id();
            let mut current_event = cmd.events();
            let is_taker_shard = (taker_id & shard_mask) as usize == $shard_id;
            while current_event.is_some() {
                let event = current_event.unwrap();
                let is_maker_shard = (event.maker_user_id & shard_mask) as usize == $shard_id;

                if is_maker_shard {
                    risk_engine.handle_trade_event(
                        event.maker_user_id,
                        cmd.market_id,
                        cmd.side,
                        event,
                    );
                }

                if is_taker_shard {
                    risk_engine.handle_trade_event(taker_id, cmd.market_id, cmd.side(), event);
                }
                current_event = event.next_event.as_deref();
            }
            // handle cancellations
            if (is_taker_shard
                && ((cmd.status == Status::PartiallyFilled
                    && cmd.time_in_force != TimeInForce::Gtc)
                    || cmd.status == Status::Cancelled))
            {
                risk_engine.handle_cancellation(cmd);
            }
        }
    }};
}

#[macro_export]
macro_rules! create_event_handler {
    ($events_handler:expr, $risk_engines:expr, $matching_engine_routers:expr, $orderbook_depth:expr) => {{
        // let events_handler = $events_handler;
        // let risk_engines = $risk_engines;
        // let matching_engine_routers = $matching_engine_routers;
        // let orderbook_depth = $orderbook_depth;
        move |cmd: &mut OrderCommand, _sequence: i64, _end_of_batch: bool| {
            // Get the appropriate risk engine for the taker user
        //     let taker_id = cmd.user_id();
        //     let num_shards = risk_engines.len() as u64;
        //     let shard_mask = num_shards - 1;
        //     let taker_shard = (taker_id & shard_mask) as usize;
        //
        //     let risk_engine = if let Some(risk_engine_mutex) = risk_engines.get(taker_shard) {
        //         let risk_engine = risk_engine_mutex.lock();
        //         Some(risk_engine)
        //     } else {
        //         None
        //     };
        //
        //     // Get the appropriate orderbook for the market using correct sharding
        //     let market_id = cmd.market_id();
        //     let num_matching_shards = matching_engine_routers.len() as u64;
        //     let matching_shard_mask = num_matching_shards - 1;
        //     let market_shard = (market_id as u64 & matching_shard_mask) as usize;
        //
        //     // Create orderbook snapshot using the proper method with configurable depth
        //     let orderbook_snapshot =
        //         if let Some(router_mutex) = matching_engine_routers.get(market_shard) {
        //             let router = router_mutex.lock();
        //             router.create_orderbook_snapshot(market_id, orderbook_depth)
        //         } else {
        //             None
        //         };
        //
        //     // Handle the processed command (for Kafka events)
        //     events_handler.handle_processed_command(
        //         cmd,
        //         risk_engine.as_deref(),
        //         orderbook_snapshot,
        //     );
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
            let mut router_guard = router.lock();
            router_guard.process_order(cmd, price_cache.clone());
        }
    }};
}
