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
        let shard_mask = risk_engines.len() as u64 - 1;

        move |cell: &::std::cell::UnsafeCell<$crate::engine::OrderCommand>,
              sequence: i64,
              _end_of_batch: bool| {
            // SAFETY: This is a shared scalar read; peer R1 handlers may read user_id
            // concurrently, and none of them mutates it.
            let user_id = unsafe { (*cell.get()).user_id };
            if (user_id & shard_mask) != $shard_id as u64 {
                return;
            }
            // SAFETY: Journaling is ordered before R1, and the shard predicate above is
            // exclusive, so only this R1 handler creates a mutable reference for this sequence.
            let cmd = unsafe { &mut *cell.get() };
            let risk_engine = &risk_engines[$shard_id];

            // R2 of earlier sequences may still owe this user credits (proceeds, released
            // collateral), and R2 only ever raises `available` and lowers `locked`. So an order
            // that passes now also passes on the settled ledger, and one that fails is decided
            // again once this shard's R2 has settled every earlier sequence. A market buy reads
            // the book, and a deposit's or withdrawal's balance is published as final: those
            // decide on the settled ledger from the start. A replay, which publishes commands
            // back to back, then reproduces every decision made live.
            let on_settled_ledger = match cmd.command {
                ::common::OrderCommandType::DepositFunds
                | ::common::OrderCommandType::WithdrawFunds => true,
                ::common::OrderCommandType::PlaceOrder => {
                    cmd.side == ::common::Side::Bid && cmd.is_market_order()
                }
                _ => false,
            };
            if on_settled_ledger {
                risk_engine.await_settled(sequence - 1);
            }
            risk_engine.pre_process_command(cmd, ::std::clone::Clone::clone(&price_cache));
            if cmd.status == ::common::Status::Rejected
                && !on_settled_ledger
                && cmd.command == ::common::OrderCommandType::PlaceOrder
            {
                // A rejected command is unchanged but for its status.
                risk_engine.await_settled(sequence - 1);
                cmd.status = ::common::Status::Processing;
                risk_engine.pre_process_command(cmd, ::std::clone::Clone::clone(&price_cache));
            }
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
/// 2. Snapshots the balance of a deposit or withdrawal (applied in R1) and stops there
/// 3. Processes trade events for both maker and taker based on shard ownership
/// 4. Handles order cancellations for partially filled IOC/FOK orders
/// 5. Updates balance snapshots in the command
/// 6. Marks the sequence settled on its shard's `RiskEngine`, for every sequence, which R1 of the
///    same shard waits on (`create_risk_handler!`)
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
        move |cell: &::std::cell::UnsafeCell<OrderCommand>, sequence: i64, _end_of_batch: bool| {
            // SAFETY (every `unsafe` below): R2 is the one handler group where two shards work on
            // the same command at once, the taker's and a maker's. So no shard forms a reference
            // to the command or to an event: each reads the fields no R2 writes, and writes only
            // its own (the taker shard `balance` and `taker_fee`, the maker shard `maker_balance`
            // and `maker_fee`), through raw pointers.
            let cmd: *mut OrderCommand = cell.get();
            let risk_engine = &risk_engines[$shard_id];
            'settle: {
                let status = unsafe { (*cmd).status };
                if status == Status::Rejected {
                    break 'settle;
                }

                let taker_id = unsafe { (*cmd).user_id };
                let market_id = unsafe { (*cmd).market_id };
                let taker_side = unsafe { (*cmd).side };

                // Determine shard ownership for taker
                let is_taker_shard = ((taker_id & shard_mask) as usize) == $shard_id;

                // A deposit or withdrawal (`Processed`) was applied in R1. Snapshot it here, in
                // sequence order with this shard's settlements, so the published balance includes
                // every earlier trade of the user.
                if status == Status::Processed {
                    if is_taker_shard {
                        // For deposits and withdrawals `market_id` carries the asset id.
                        let balance = risk_engine.get_balance(taker_id, market_id as u16);
                        unsafe { (&raw mut (*cmd).balance[0]).write(balance) };
                    }
                    break 'settle;
                }

                let taker_order = unsafe { ((*cmd).order_id, (*cmd).price) };

                // Process all trade events in the chain
                let mut event = unsafe { OrderCommand::events_ptr(cmd) };
                while !event.is_null() {
                    let fill = unsafe { ::common::MatcherTradeEvent::fill(event) };

                    // Settle maker if this shard owns the maker's user ID
                    if ((fill.maker_user_id & shard_mask) as usize) == $shard_id {
                        let settled = risk_engine.handle_trade_event(
                            fill.maker_user_id,
                            market_id,
                            taker_side.op_side(),
                            fill,
                            None, // Maker order ID is carried by the trade event
                        );
                        if let Some(balance) = settled.maker_balance {
                            unsafe { (&raw mut (*event).maker_balance).write(balance) };
                        }
                        unsafe { (&raw mut (*event).maker_fee).write(settled.fee) };
                    }

                    // Settle taker if this shard owns the taker's user ID
                    if is_taker_shard {
                        let settled = risk_engine.handle_trade_event(
                            taker_id,
                            market_id,
                            taker_side,
                            fill,
                            Some(taker_order),
                        );
                        unsafe { (&raw mut (*event).taker_fee).write(settled.fee) };
                    }

                    event = unsafe { ::common::MatcherTradeEvent::next_ptr(event) };
                }

                // Handle cancellations for taker (IOC/FOK partial fills or explicit cancels)
                if is_taker_shard {
                    let should_handle_cancellation = matches!(
                        (status, unsafe { (*cmd).time_in_force }),
                        (Status::PartiallyFilled, TimeInForce::Ioc | TimeInForce::Fok)
                            | (Status::Cancelled, _)
                    );

                    if should_handle_cancellation {
                        risk_engine.handle_cancellation(
                            taker_order.0,
                            taker_id,
                            market_id,
                            taker_side,
                            unsafe { (*cmd).size },
                        );
                    }

                    // Update balance snapshot in command for response
                    let balance = [
                        risk_engine.get_balance(taker_id, base_asset(market_id)),
                        risk_engine.get_balance(taker_id, quote_asset(market_id)),
                    ];
                    unsafe { (&raw mut (*cmd).balance).write(balance) };
                }
            }
            // Every sequence, settled or skipped, moves the cursor R1 waits on.
            risk_engine.mark_settled(sequence);
        }
    }};
}
