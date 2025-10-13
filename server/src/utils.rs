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
            if (cmd.status == Status::Rejected || cmd.status == Status::Processed) {
                return;
            }
            let risk_engine = &risk_engines[$shard_id];
            let taker_id = cmd.user_id();
            let market_id = cmd.market_id();
            let taker_side = cmd.side();
            let taker_price = cmd.price();
            let mut current_event = cmd.events_mut();
            let is_taker_shard = (taker_id & shard_mask) as usize == $shard_id;
            while current_event.is_some() {
                let event = current_event.unwrap();
                let is_maker_shard = (event.maker_user_id & shard_mask) as usize == $shard_id;

                if is_maker_shard {
                    // Maker's side is the opposite of the taker's
                    let maker_side = taker_side.op_side();
                    risk_engine.handle_trade_event(
                        event.maker_user_id,
                        market_id,
                        maker_side,
                        event,
                        None, // No taker command for maker settlement
                    );
                }

                if is_taker_shard {
                    risk_engine.handle_trade_event(
                        taker_id,
                        market_id,
                        taker_side,
                        event,
                        Some(taker_price),
                    );
                }
                current_event = event.next_event.as_deref_mut();
            }
            // handle cancellations
            if (is_taker_shard) {
                if ((cmd.status == Status::PartiallyFilled
                    && cmd.time_in_force != TimeInForce::Gtc)
                    || cmd.status == Status::Cancelled)
                {
                    risk_engine.handle_cancellation(cmd);
                }

                cmd.balance[0] = risk_engine.get_balance(cmd.user_id(), base_asset(market_id));
                cmd.balance[1] = risk_engine.get_balance(cmd.user_id(), quote_asset(market_id));
            }
        }
    }};
}

#[macro_export]
macro_rules! create_event_handler {
    ($events_handler:expr) => {{
        // let events_handler = $events_handler;
        move |cmd: &mut OrderCommand, _sequence: i64, _end_of_batch: bool| {
            $events_handler.handle_processed_command(cmd);
        }
    }};
}

/// Macro to generate matching engine handlers
/// This eliminates code duplication while maintaining separate handlers for each shard
#[macro_export]
macro_rules! create_matching_handler {
    ($shard_id:expr, $routers:expr, $price_cache:expr) => {{
        let routers = $routers.clone();
        let shard_id = $shard_id;
        let price_cache = $price_cache.clone();
        move |cmd: &mut OrderCommand, _sequence: i64, _end_of_batch: bool| {
            // Non-op commands for order book processing
            if cmd.command == OrderCommandType::DepositFunds
                || cmd.command == OrderCommandType::WithdrawFunds
                || cmd.status == Status::Rejected
            {
                return;
            }
            let router = &routers[shard_id];
            let price_cache = price_cache.clone();
            router.process_order(cmd, price_cache);
        }
    }};
}
