//! Utility macros for handler generation
//!
//! This module provides hygienic macros for creating handlers in the disruptor pipeline.
//! These macros eliminate code duplication while maintaining type safety and proper scoping.

/// Creates a risk engine R1 handler for pre-processing orders
///
/// This macro generates a closure that routes commands to the appropriate
/// risk engine shard based on user ID.
///
/// # Arguments
///
/// * `$shard_id` - The shard ID for this handler (compile-time constant)
/// * `$risk_engines` - Arc reference to the vector of risk engines
/// * `$price_cache` - Arc reference to the shared price cache
///
/// # Example
///
/// ```ignore
/// let handler = create_risk_handler!(0, risk_engines_arc, price_cache);
/// ```
#[macro_export]
macro_rules! create_risk_handler {
    ($shard_id:expr, $risk_engines:expr, $price_cache:expr) => {{
        // Clone Arc pointers to move into the closure
        let risk_engines = ::std::clone::Clone::clone($risk_engines);
        let price_cache = ::std::clone::Clone::clone($price_cache);

        move |cmd: &mut $crate::engine::OrderCommand, _sequence: i64, _end_of_batch: bool| {
            let risk_engine = &risk_engines[$shard_id];
            risk_engine.pre_process_command(cmd, ::std::clone::Clone::clone(&price_cache));
        }
    }};
}

/// Creates a risk engine R2 handler for settlement processing
///
/// This macro generates a closure for the second risk engine stage (R2)
/// which handles trade settlement and balance updates after order matching.
///
/// # Arguments
///
/// * `$shard_id` - The shard ID for this handler (compile-time constant)
/// * `$risk_engines` - Arc reference to the vector of risk engines
///
/// # Processing Logic
///
/// 1. Skips rejected orders
/// 2. Processes trade events for both maker and taker based on shard ownership
/// 3. Handles order cancellations for partially filled IOC/FOK orders
/// 4. Updates balance snapshots in the command
///
/// # Example
///
/// ```ignore
/// let handler = create_risk_r2_handler!(0, risk_engines_arc);
/// ```
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

            // Determine shard ownership for taker
            let is_taker_shard = ((taker_id & shard_mask) as usize) == $shard_id;

            // Process all trade events in the chain
            let mut current_event = cmd.events_mut();
            while let Some(event) = current_event {
                let maker_id = event.maker_user_id;
                let is_maker_shard = ((maker_id & shard_mask) as usize) == $shard_id;

                // Settle maker if this shard owns the maker's user ID
                if is_maker_shard {
                    let maker_side = taker_side.op_side();
                    risk_engine.handle_trade_event(
                        maker_id,
                        market_id,
                        maker_side,
                        event,
                        None, // Maker uses matched price, not limit price
                    );
                }

                // Settle taker if this shard owns the taker's user ID
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

            // Handle cancellations for taker (IOC/FOK partial fills or explicit cancels)
            if is_taker_shard {
                let should_handle_cancellation = matches!(
                    (cmd.status, cmd.time_in_force),
                    (Status::PartiallyFilled, TimeInForce::Ioc | TimeInForce::Fok)
                        | (Status::Cancelled, _)
                );

                if should_handle_cancellation {
                    risk_engine.handle_cancellation(cmd);
                }

                // Update balance snapshot in command for response
                cmd.balance[0] = risk_engine.get_balance(taker_id, base_asset(market_id));
                cmd.balance[1] = risk_engine.get_balance(taker_id, quote_asset(market_id));
            }
        }
    }};
}

/// Creates an events handler for publishing trade events
///
/// This macro generates a closure that forwards processed commands to
/// the events handler for market data distribution (e.g., Kafka).
///
/// # Arguments
///
/// * `$events_handler` - Arc reference to the events handler implementation
///
/// # Example
///
/// ```ignore
/// let handler = create_event_handler!(events_handler_arc);
/// ```
#[macro_export]
macro_rules! create_event_handler {
    ($events_handler:expr) => {{
        move |cmd: &mut common::OrderCommand, _sequence: i64, _end_of_batch: bool| {
            $events_handler.handle_processed_command(cmd);
        }
    }};
}

/// Creates a matching engine handler for order processing
///
/// This macro generates a closure that routes orders to the appropriate
/// matching engine shard based on symbol ID.
///
/// # Arguments
///
/// * `$shard_id` - The shard ID for this handler (compile-time constant)
/// * `$routers` - Mutable reference to the vector of matching engine routers
/// * `$price_cache` - Arc reference to the shared price cache
///
/// # Note
///
/// This macro is less commonly used as the engine now creates handlers
/// inline during initialization. Kept for compatibility.
///
/// # Example
///
/// ```ignore
/// let handler = create_matching_handler!(0, routers, price_cache);
/// ```
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
