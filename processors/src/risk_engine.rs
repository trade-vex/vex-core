use crate::error::{Result, RiskEngineError};
use common::BalanceError;
use common::BalanceStore;
use common::CoreMarketSpecification;
use common::MatcherTradeEvent;
use common::OrderCommand;
use common::OrderCommandType;
use common::PriceCache;
use common::Side;
use common::Status;
use common::UserBalance;
use common::{base_asset, order_debug, order_warn, quote_asset};
use hashbrown::HashMap;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use tracing::{debug, error, warn};

/// Manages all user profiles and performs risk checks as well as settlements
pub struct RiskEngine {
    balances: Arc<Mutex<BalanceStore>>,
    /// Fees credited per asset by this shard. `u128` so the running total can never overflow
    /// in practice: a `u64` ran out after ~9,223 ETH of 20 bp taker buys at 18 decimals, and
    /// every later fee-bearing fill on that asset then failed to settle after it had matched.
    accrued_fees: Mutex<HashMap<u16, u128>>,
    bid_collateral: Mutex<HashMap<u64, u64>>,
    pub symbol_specs: HashMap<u32, CoreMarketSpecification>,
    shard_id: u32,
    shard_mask: u64,
    /// Highest pipeline sequence this shard's R2 has settled; -1 before the first. R1 of the same
    /// shard waits on it when its decision depends on credits R2 still owes the user.
    settled_up_to: AtomicI64,
}

impl RiskEngine {
    pub fn new(
        symbol_specs: HashMap<u32, CoreMarketSpecification>,
        shard_id: u32,
        num_shards: u32,
    ) -> Self {
        if num_shards.count_ones() != 1 {
            panic!("Number of shards must be a power of 2");
        }
        Self {
            balances: Arc::new(Mutex::new(BalanceStore::new())),
            accrued_fees: Mutex::new(HashMap::new()),
            bid_collateral: Mutex::new(HashMap::new()),
            symbol_specs,
            shard_id,
            shard_mask: (num_shards - 1) as u64,
            settled_up_to: AtomicI64::new(-1),
        }
    }

    /// Called by this shard's R2 after every sequence, settled or skipped.
    pub fn mark_settled(&self, sequence: i64) {
        self.settled_up_to.store(sequence, Ordering::Release);
    }

    /// Spins until this shard's R2 has settled every sequence up to `sequence`.
    pub fn await_settled(&self, sequence: i64) {
        while self.settled_up_to.load(Ordering::Acquire) < sequence {
            std::hint::spin_loop();
        }
    }

    /// Checks if a user ID is handled by this risk engine instance.
    #[inline]
    fn user_id_for_this_handler(&self, user_id: u64) -> bool {
        (user_id & self.shard_mask) == self.shard_id as u64
    }

    /// Pre-processes a command to validate it and hold funds
    pub fn pre_process_command(&self, cmd: &mut OrderCommand, price_cache: Arc<PriceCache>) {
        // Process only if the command is for a user managed by this shard
        if !self.user_id_for_this_handler(cmd.user_id) {
            return; // Not for this shard, skip
        }

        // Validate the command arguments
        order_debug!(
            "risk_preprocess_start",
            cmd,
            stage = "risk_r1",
            shard_id = self.shard_id
        );
        match cmd.command {
            OrderCommandType::DepositFunds | OrderCommandType::WithdrawFunds
                if u16::try_from(cmd.market_id).is_err() || cmd.size == 0 =>
            {
                // market_id carries the asset id for these commands; `as u16` below would wrap it.
                order_debug!(
                    "risk_invalid_asset_or_amount",
                    cmd,
                    stage = "risk_r1",
                    shard_id = self.shard_id
                );
                cmd.set_status(Status::Rejected);
            }
            OrderCommandType::PlaceOrder => {
                if self.symbol_specs.get(&cmd.market_id).is_none() {
                    order_debug!(
                        "risk_missing_market_spec",
                        cmd,
                        stage = "risk_r1",
                        shard_id = self.shard_id
                    );
                    cmd.status = common::Status::Rejected;
                    return;
                }

                // A market order can never rest. Reject Market + GTC here, before any funds are
                // locked and before reserve_funds_for_order() rewrites a market buy's sentinel to
                // its slippage cap, after which the book can no longer tell it from a limit order.
                if cmd.time_in_force == common::TimeInForce::Gtc && cmd.is_market_order() {
                    order_debug!(
                        "risk_market_gtc_rejected",
                        cmd,
                        stage = "risk_r1",
                        shard_id = self.shard_id
                    );
                    cmd.set_status(Status::Rejected);
                    return;
                }

                // Note: Fees are always in the receiving asset, hence are cut on post-processing (settlement)
                if let Err(err) = self.reserve_funds_for_order(cmd, price_cache) {
                    // Often transient: R1 retries a failed order once R2 has settled the
                    // earlier sequences (`create_risk_handler!`). The final `Rejected` is in the
                    // response.
                    order_debug!(
                        "risk_reserve_failed",
                        cmd,
                        stage = "risk_r1",
                        shard_id = self.shard_id,
                        error = ?err
                    );
                    cmd.set_status(Status::Rejected);
                }
            }
            OrderCommandType::DepositFunds => match self.handle_deposit(cmd) {
                Ok(_) => {
                    // R2 snapshots the balance, in sequence order with settlements.
                    cmd.set_status(Status::Processed);
                    order_debug!(
                        "risk_deposit_applied",
                        cmd,
                        stage = "risk_r1",
                        shard_id = self.shard_id
                    );
                }
                Err(err) => {
                    order_warn!(
                        "risk_deposit_failed",
                        cmd,
                        stage = "risk_r1",
                        shard_id = self.shard_id,
                        error = ?err
                    );
                    cmd.set_status(Status::Rejected);
                }
            },
            OrderCommandType::WithdrawFunds => match self.handle_withdrawal(cmd) {
                Ok(_) => {
                    // R2 snapshots the balance, in sequence order with settlements.
                    cmd.set_status(Status::Processed);
                    order_debug!(
                        "risk_withdrawal_applied",
                        cmd,
                        stage = "risk_r1",
                        shard_id = self.shard_id
                    );
                }
                Err(err) => {
                    order_debug!(
                        "risk_withdrawal_failed",
                        cmd,
                        stage = "risk_r1",
                        shard_id = self.shard_id,
                        error = ?err
                    );
                    cmd.set_status(Status::Rejected);
                }
            },
            _ => {} // no balance change happens in case of cancel
        }

        if cmd.status != Status::Rejected {
            order_debug!(
                "risk_preprocess_complete",
                cmd,
                stage = "risk_r1",
                shard_id = self.shard_id
            );
        }
    }

    /// Handles a single trade event from the matching engine to settle funds
    /// This is called by the R2 handler for each individual event in the linked list.
    /// A settlement that fails aborts the process: R1 admitted both orders, so it cannot happen
    /// unless the ledger is already wrong.
    pub fn handle_trade_event(
        &self,
        user_id: u64,
        market_id: u32,
        user_side: Side,
        event: &mut MatcherTradeEvent,
        taker_order: Option<(u64, u64)>,
    ) {
        debug!(
            target: "risk_engine",
            event = "settlement_start",
            shard_id = self.shard_id,
            user_id,
            market_id,
            maker_user_id = event.maker_user_id,
            price = event.price,
            size = event.size
        );

        // Get market specification for fee calculations
        let spec = match self.symbol_specs.get(&market_id) {
            Some(spec) => spec,
            None => {
                warn!(
                    target: "risk_engine",
                    event = "missing_market_spec",
                    shard_id = self.shard_id,
                    market_id
                );
                return;
            }
        };

        if let Err(err) = self.settle_trade(user_id, market_id, user_side, event, spec, taker_order)
        {
            error!(
                target: "risk_engine",
                event = "settlement_failed",
                shard_id = self.shard_id,
                user_id,
                market_id,
                user_side = ?user_side,
                maker_user_id = event.maker_user_id,
                price = event.price,
                size = event.size,
                taker_order = ?taker_order,
                error = ?err,
                "R1 admitted both orders, so this is a ledger invariant violation; aborting the exchange rather than settle one side"
            );
            // The counterparty may already be settled on another shard. Stopping here keeps the
            // events stage from publishing a one-sided trade; the journal replays to this command,
            // so this is a page, not a restart.
            std::process::abort();
        } else {
            debug!(
                target: "risk_engine",
                event = "settlement_complete",
                shard_id = self.shard_id,
                user_id,
                market_id
            );
        }
    }

    fn settle_trade(
        &self,
        user_id: u64,
        market_id: u32,
        user_side: Side,
        event: &mut MatcherTradeEvent,
        spec: &CoreMarketSpecification,
        taker_order: Option<(u64, u64)>,
    ) -> Result<()> {
        let is_maker = user_id == event.maker_user_id;
        let (order_id, order_completed) = if is_maker {
            (event.matched_order_id, event.matched_order_completed)
        } else if let Some((order_id, _)) = taker_order {
            (order_id, event.active_order_completed)
        } else {
            (0, event.active_order_completed)
        };
        let fee = if is_maker {
            spec.maker_fee
        } else {
            spec.taker_fee
        };

        // --- Price Improvement Refund Logic (for Taker only) ---
        // If the taker gets a better price than their limit, refund the difference.
        let refund_amount = if !is_maker
            && let Some((_, limit_price)) = taker_order
            && user_side == Side::Bid
        // Price improvement only applies to BID orders where QUOTE currency was locked.
        {
            let execution_price = event.price;
            if execution_price < limit_price {
                let price_diff = limit_price - execution_price;
                spec.calculate_quote_cost(price_diff, event.size)
                    .ok_or(BalanceError::Overflow)?
            } else {
                0
            }
        } else {
            0
        };

        let (
            asset_to_subtract,
            amount_to_subtract,
            asset_to_add,
            amount_to_add,
            fee_asset,
            fee_amount,
        ) = match user_side {
            // User is BUYING base asset with quote asset.
            // Spends quote, receives base. Fee is on base asset received.
            Side::Bid => {
                let quote_spent = spec
                    .calculate_quote_cost(event.price, event.size)
                    .ok_or(BalanceError::Overflow)?;
                // Fee is on the base asset received, in basis points (20 bp = 0.2%). It is rounded
                // UP per fill: each fill pays its exact share plus less than one atomic unit, so
                // splitting an order into many fills can never pay less than a single fill would
                // (floor rounding made sub-10_000/fee-unit fills free). The cost of that is a
                // bounded overcharge: at most one unit per fill, i.e. N fills of one order pay at
                // most N - 1 units more than one fill of the same total.
                let fee_in_base = u64::try_from((event.size as u128 * fee as u128).div_ceil(10000))
                    .unwrap_or(u64::MAX);
                let base_received_net = event.size.saturating_sub(fee_in_base);
                (
                    quote_asset(market_id),
                    quote_spent,
                    base_asset(market_id),
                    base_received_net,
                    base_asset(market_id),
                    fee_in_base,
                )
            }
            // User is SELLING base asset for quote asset.
            // Spends base, receives quote. Fee is on quote asset received.
            Side::Ask => {
                let base_spent = event.size;
                let quote_received_gross = spec
                    .calculate_quote_cost(event.price, event.size)
                    .ok_or(BalanceError::Overflow)?;
                // Fee is on the quote asset received, rounded up per fill with the same bound as
                // the bid fee above.
                let fee_in_quote =
                    u64::try_from((quote_received_gross as u128 * fee as u128).div_ceil(10000))
                        .unwrap_or(u64::MAX);
                let quote_received_net = quote_received_gross.saturating_sub(fee_in_quote);
                (
                    base_asset(market_id),
                    base_spent,
                    quote_asset(market_id),
                    quote_received_net,
                    quote_asset(market_id),
                    fee_in_quote,
                )
            }
        };

        // Compute and validate every mutation against copies before changing the store.
        let mut store = self.balances.lock();

        // Fee accrual is overflow-checked here too, so a saturated accumulator can never leave a
        // half-settled trade. Lock order everywhere: balances -> accrued_fees -> bid_collateral.
        let mut accrued_fees = (fee_amount != 0).then(|| self.accrued_fees.lock());
        let accrued_fee_total = accrued_fees
            .as_ref()
            .map(|fees| {
                fees.get(&fee_asset)
                    .copied()
                    .unwrap_or_default()
                    .checked_add(u128::from(fee_amount))
                    .ok_or(BalanceError::Overflow)
            })
            .transpose()?;

        // A bid's collateral record decides what this fill consumes and, when the order completes,
        // the rounding residue R1 locked beyond its fills. Decided here so the residue's unlock is
        // part of the one commit below.
        let mut collateral = (user_side == Side::Bid).then(|| self.bid_collateral.lock());
        let consumed = refund_amount
            .checked_add(amount_to_subtract)
            .ok_or(BalanceError::Overflow)?;
        let residue = match collateral.as_deref() {
            Some(collateral) if order_completed => match collateral.get(&order_id) {
                Some(locked) => locked.saturating_sub(consumed),
                None => {
                    warn!(
                        target: "risk_engine",
                        event = "missing_bid_collateral",
                        shard_id = self.shard_id,
                        order_id,
                        user_id
                    );
                    0
                }
            },
            _ => 0,
        };
        let locked_needed = consumed
            .checked_add(residue)
            .ok_or(BalanceError::Overflow)?;
        let back_to_available = refund_amount
            .checked_add(residue)
            .ok_or(BalanceError::Overflow)?;

        let mut balance_sub = store.get_balance(user_id, asset_to_subtract);
        if balance_sub.locked < locked_needed {
            return Err(BalanceError::InsufficientLockedFunds {
                locked: balance_sub.locked,
                needed: locked_needed,
            }
            .into());
        }
        balance_sub.locked -= locked_needed;
        balance_sub.available = balance_sub
            .available
            .checked_add(back_to_available)
            .ok_or(BalanceError::Overflow)?;
        // A separate copy: the config rejects a market whose base and quote are one asset.
        let mut balance_add = store.get_balance(user_id, asset_to_add);
        balance_add.available = balance_add
            .available
            .checked_add(amount_to_add)
            .ok_or(BalanceError::Overflow)?;

        // Commit. Nothing below can fail. set_balance inserts-or-updates, so an asset the user
        // receives for the first time gets its row here.
        store.set_balance(user_id, asset_to_subtract, balance_sub);
        store.set_balance(user_id, asset_to_add, balance_add);
        if let (Some(fees), Some(total)) = (accrued_fees.as_mut(), accrued_fee_total) {
            fees.insert(fee_asset, total);
        }
        if let Some(collateral) = collateral.as_mut() {
            if order_completed {
                collateral.remove(&order_id);
            } else if let Some(locked) = collateral.get_mut(&order_id) {
                *locked = locked.saturating_sub(consumed);
            }
        }

        if is_maker {
            event.maker_balance = if asset_to_subtract == base_asset(market_id) {
                // Maker was on ASK side, selling base for quote.
                [balance_sub, balance_add]
            } else {
                // Maker was on BID side, buying base with quote.
                [balance_add, balance_sub]
            };
        }
        Ok(())
    }

    /// handle cancellations -> release funds
    ///
    /// R2 calls this once matching has decided the order's fate: a cancel removed it from the
    /// book, or the unfilled part of an IOC/FOK/market order was cancelled (after any trades). That
    /// status must stand even when the release fails: `Rejected` would publish nothing and hide a
    /// real cancel and real trades (user decision 3). Such a failure is a ledger inconsistency and
    /// is logged for an operator.
    pub fn handle_cancellation(&self, cmd: &mut OrderCommand) {
        if let Err(err) = self.release_funds_for_order(
            cmd.order_id,
            cmd.user_id,
            cmd.market_id,
            cmd.side,
            cmd.size,
        ) {
            common::order_error!(
                "risk_cancel_release_failed",
                cmd,
                stage = "risk_r2",
                shard_id = self.shard_id,
                error = ?err
            );
        } else {
            debug!(
                target: "risk_engine",
                event = "cancel_released",
                shard_id = self.shard_id,
                order_id = cmd.order_id
            );
        }
    }

    /// Reserves funds for a new order.
    /// This is called by the pre-orderbook risk engine.
    fn reserve_funds_for_order(
        &self,
        cmd: &mut OrderCommand,
        price_cache: Arc<PriceCache>,
    ) -> Result<()> {
        if cmd.size == 0 {
            return Err(RiskEngineError::InvalidArguments {
                price: cmd.price,
                size: cmd.size,
            });
        }

        let (asset_to_lock, amount_to_lock, price) = match cmd.side {
            // For a Bid (buy), we lock the quote currency. Amount = price * size.
            Side::Bid => {
                let (amount, price) = self.bid_amount(cmd, price_cache)?;
                if amount == 0 {
                    return Err(RiskEngineError::InvalidArguments {
                        price: cmd.price,
                        size: cmd.size,
                    });
                }
                (quote_asset(cmd.market_id), amount, price)
            }
            // For an Ask (sell), we lock the base currency. Amount = size.
            Side::Ask => {
                // A market sell carries the price-0 sentinel and is priced by the book at match
                // time, so there is no order-time notional to check. A *limit* ask whose quote
                // proceeds floor to zero would hand over base for nothing, so reject it.
                if !cmd.is_market_order() && self.ask_proceeds(cmd)? == 0 {
                    return Err(RiskEngineError::InvalidArguments {
                        price: cmd.price,
                        size: cmd.size,
                    });
                }
                (base_asset(cmd.market_id), cmd.size, cmd.price)
            }
        };

        // Acquire a lock on the store and perform the operation
        let mut store = self.balances.lock();
        store.lock_funds(cmd.user_id, asset_to_lock, amount_to_lock)?;
        // Only a locked order carries its lock price (so a cancel releases exactly what was
        // locked); a rejected market buy keeps its sentinel.
        cmd.price = price;
        if cmd.side == Side::Bid {
            self.bid_collateral
                .lock()
                .insert(cmd.order_id, amount_to_lock);
        }
        Ok(())
    }

    /// Returns (quote to lock, effective price) for a bid: the R1 funding bound for a market BUY,
    /// `cmd.price` otherwise.
    ///
    /// Market BUY orders are capped, not swept: once its funds are locked, the `u64::MAX` market
    /// sentinel is replaced with `best_ask + floor(best_ask * slippage_bps / 10_000)`. The
    /// matching engine then uses that effective price as a limit, so liquidity above the cap
    /// remains unfilled. The response carries the ordinary status for its time-in-force (IOC:
    /// `Filled`, `PartiallyFilled` or `Cancelled`; FOK: `Filled` or `Cancelled`) and returns the
    /// effective cap in `price`. Market + GTC never reaches this point: `pre_process_command`
    /// rejects it first.
    #[inline]
    fn bid_amount(&self, cmd: &OrderCommand, price_cache: Arc<PriceCache>) -> Result<(u64, u64)> {
        // Get spec early
        let spec =
            self.symbol_specs
                .get(&cmd.market_id)
                .ok_or(RiskEngineError::MarketSpecNotFound {
                    market_id: cmd.market_id,
                })?;

        if cmd.is_market_order() {
            // Market buy order
            let slippage = spec.slippage;
            let best_ask = price_cache
                .get_prices(cmd.market_id)
                .map_or(0, |(_, best_ask)| best_ask);

            if best_ask == 0 || best_ask == u64::MAX {
                // No liquidity on ask side (sentinel value), cannot determine price for market order.
                return Err(RiskEngineError::InvalidArguments {
                    price: cmd.price,
                    size: cmd.size,
                });
            }

            // Assuming slippage is in basis points (e.g., 5bp = 0.05%). Nothing bounds the
            // configured slippage, so the adjustment can exceed u64: reject rather than truncate
            // it to an arbitrary cap. The u128 product cannot overflow (it is below 2^96).
            let slippage_adjustment = u64::try_from(best_ask as u128 * slippage as u128 / 10000)
                .map_err(|_| BalanceError::Overflow)?;

            let conservative_price = best_ask
                .checked_add(slippage_adjustment)
                .ok_or(BalanceError::Overflow)?;

            let amount = spec
                .calculate_quote_cost(conservative_price, cmd.size)
                .ok_or(BalanceError::Overflow)?;
            Ok((amount, conservative_price))
        } else {
            // Limit order
            let amount = spec
                .calculate_quote_cost(cmd.price, cmd.size)
                .ok_or(BalanceError::Overflow)?;
            Ok((amount, cmd.price))
        }
    }

    /// Quote proceeds a *limit* ask would earn if it filled entirely at its own price.
    #[inline]
    fn ask_proceeds(&self, cmd: &OrderCommand) -> Result<u64> {
        let spec =
            self.symbol_specs
                .get(&cmd.market_id)
                .ok_or(RiskEngineError::MarketSpecNotFound {
                    market_id: cmd.market_id,
                })?;

        spec.calculate_quote_cost(cmd.price, cmd.size)
            .ok_or(BalanceError::Overflow.into())
    }

    /// Releases previously reserved funds from a canceled or filled order.
    /// This is called by the post-orderbook risk engine or order cancellation logic.
    fn release_funds_for_order(
        &self,
        order_id: u64,
        user_id: u64,
        market_id: u32,
        side: Side,
        size: u64,
    ) -> Result<()> {
        let mut store = self.balances.lock();
        match side {
            Side::Bid => self.release_remaining_bid_collateral(
                &mut store,
                order_id,
                user_id,
                quote_asset(market_id),
            ),
            Side::Ask => store
                .unlock_funds(user_id, base_asset(market_id), size)
                .map_err(RiskEngineError::BalanceError),
        }
    }

    fn release_remaining_bid_collateral(
        &self,
        store: &mut BalanceStore,
        order_id: u64,
        user_id: u64,
        asset_id: u16,
    ) -> Result<()> {
        let mut collateral = self.bid_collateral.lock();
        let Some(amount) = collateral.get(&order_id).copied() else {
            warn!(
                target: "risk_engine",
                event = "missing_bid_collateral",
                shard_id = self.shard_id,
                order_id,
                user_id
            );
            return Ok(());
        };

        if amount != 0 {
            store.unlock_funds(user_id, asset_id, amount)?;
        }
        collateral.remove(&order_id);
        Ok(())
    }

    pub fn get_balance(&self, user_id: u64, asset_id: u16) -> UserBalance {
        let store = self.balances.lock();
        store.get_balance(user_id, asset_id)
    }

    pub fn try_get_balance(&self, user_id: u64, asset_id: u16) -> Result<UserBalance> {
        let store = self.balances.lock();
        Ok(store.try_get_balance(user_id, asset_id)?)
    }

    /// Returns fees accrued for an asset by this risk-engine shard.
    pub fn get_accrued_fee(&self, asset_id: u16) -> u128 {
        self.accrued_fees
            .lock()
            .get(&asset_id)
            .copied()
            .unwrap_or_default()
    }

    /// Returns every asset's accrued fee total on this risk-engine shard.
    pub fn accrued_fees(&self) -> Vec<(u16, u128)> {
        self.accrued_fees
            .lock()
            .iter()
            .map(|(&asset_id, &amount)| (asset_id, amount))
            .collect()
    }

    pub fn set_balance(&self, user_id: u64, asset_id: u16, balance: UserBalance) {
        let mut store = self.balances.lock();
        store.set_balance(user_id, asset_id, balance);
    }

    /// Handles deposit funds command
    /// The market_id field is used to represent the asset_id for deposits
    fn handle_deposit(&self, cmd: &mut OrderCommand) -> Result<()> {
        let asset_id = cmd.market_id as u16;
        let amount = cmd.size;

        let mut store = self.balances.lock();
        store.add_funds(cmd.user_id, asset_id, amount)?;

        Ok(())
    }

    /// Handles withdrawal funds command
    /// The market_id field is used to represent the asset_id for withdrawals
    fn handle_withdrawal(&self, cmd: &mut OrderCommand) -> Result<()> {
        let asset_id = cmd.market_id as u16;
        let amount = cmd.size;

        let mut store = self.balances.lock();
        store.subtract_funds(cmd.user_id, asset_id, amount)?;

        Ok(())
    }
}

impl Default for RiskEngine {
    fn default() -> Self {
        Self::new(HashMap::new(), 0, 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{MarketType, TimeInForce};

    fn get_spec(market_id: u32) -> CoreMarketSpecification {
        CoreMarketSpecification::builder()
            .market_id(market_id)
            .market_type(MarketType::Spot)
            .maker_fee(10) // 0.1%
            .taker_fee(20) // 0.2%
            .slippage(5)
            .build()
            .unwrap()
    }

    fn settled_bid_fees(
        maker_fee: u64,
        taker_fee: u64,
        is_maker: bool,
        fill_sizes: &[u64],
    ) -> Vec<u64> {
        let base_asset_id = 2u16;
        let quote_asset_id = 1u16;
        let market_id = ((quote_asset_id as u32) << 16) | base_asset_id as u32;
        let user_id = 102;
        let locked_quote = fill_sizes.iter().sum();

        let spec = CoreMarketSpecification::builder()
            .market_id(market_id)
            .market_type(MarketType::Spot)
            .maker_fee(maker_fee)
            .taker_fee(taker_fee)
            .slippage(5)
            .build()
            .unwrap();
        let mut specs = HashMap::new();
        specs.insert(market_id, spec);
        let engine = RiskEngine::new(specs, 0, 1);
        engine.set_balance(user_id, quote_asset_id, UserBalance::new(0, locked_quote));

        let mut previous_base_balance = 0;
        let fees = fill_sizes
            .iter()
            .map(|&size| {
                let mut trade_event = MatcherTradeEvent {
                    price: 1,
                    size,
                    maker_user_id: if is_maker { user_id } else { user_id - 1 },
                    active_order_completed: false,
                    matched_order_id: 1,
                    matched_order_completed: true,
                    next_event: None,
                    maker_balance: [UserBalance::default(); 2],
                    maker_remaining_size: 0,
                    maker_original_size: size,
                };

                engine.handle_trade_event(
                    user_id,
                    market_id,
                    Side::Bid,
                    &mut trade_event,
                    (!is_maker).then_some((1, 1)),
                );

                let base_balance = engine.get_balance(user_id, base_asset_id).total();
                let fee = size - (base_balance - previous_base_balance);
                previous_base_balance = base_balance;
                fee
            })
            .collect();

        assert_eq!(engine.get_balance(user_id, quote_asset_id).total(), 0);
        fees
    }

    fn system_asset_total(engine: &RiskEngine, users: &[u64], asset_id: u16) -> u128 {
        users
            .iter()
            .map(|&user_id| engine.get_balance(user_id, asset_id).total() as u128)
            .sum::<u128>()
            + engine.get_accrued_fee(asset_id)
    }

    fn settle_trade_and_assert_conservation(
        spec: CoreMarketSpecification,
        price: u64,
        size: u64,
    ) -> RiskEngine {
        let market_id = spec.market_id;
        let base_asset_id = base_asset(market_id);
        let quote_asset_id = quote_asset(market_id);
        let gross_quote = spec
            .calculate_quote_cost(price, size)
            .expect("test notional fits");
        let taker_fee = u64::try_from((size as u128 * spec.taker_fee as u128) / 10000).unwrap();
        let maker_fee =
            u64::try_from((gross_quote as u128 * spec.maker_fee as u128) / 10000).unwrap();

        let mut specs = HashMap::new();
        specs.insert(market_id, spec);
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 0, 1);
        let maker_id = 101;
        let taker_id = 102;
        let users = [maker_id, taker_id];

        engine.set_balance(taker_id, quote_asset_id, UserBalance::new(gross_quote, 0));
        engine.set_balance(maker_id, base_asset_id, UserBalance::new(size, 0));

        let base_total_before = system_asset_total(&engine, &users, base_asset_id);
        let quote_total_before = system_asset_total(&engine, &users, quote_asset_id);

        let mut taker_cmd = OrderCommand::place_order(
            TimeInForce::Gtc,
            taker_id,
            price,
            size,
            Side::Bid,
            market_id,
            1,
        );
        engine
            .reserve_funds_for_order(&mut taker_cmd, price_cache.clone())
            .unwrap();

        let mut maker_cmd = OrderCommand::place_order(
            TimeInForce::Gtc,
            maker_id,
            price,
            size,
            Side::Ask,
            market_id,
            2,
        );
        engine
            .reserve_funds_for_order(&mut maker_cmd, price_cache)
            .unwrap();

        let mut trade_event = MatcherTradeEvent {
            price,
            size,
            maker_user_id: maker_id,
            active_order_completed: false,
            matched_order_id: 2,
            matched_order_completed: true,
            next_event: None,
            maker_balance: [UserBalance::default(); 2],
            maker_remaining_size: 0,
            maker_original_size: size,
        };

        engine.handle_trade_event(
            taker_id,
            market_id,
            Side::Bid,
            &mut trade_event,
            Some((1, price)),
        );
        engine.handle_trade_event(maker_id, market_id, Side::Ask, &mut trade_event, None);

        assert_eq!(
            system_asset_total(&engine, &users, base_asset_id),
            base_total_before
        );
        assert_eq!(
            system_asset_total(&engine, &users, quote_asset_id),
            quote_total_before
        );
        assert_eq!(engine.get_accrued_fee(base_asset_id), u128::from(taker_fee));
        assert_eq!(
            engine.get_accrued_fee(quote_asset_id),
            u128::from(maker_fee)
        );

        engine
    }

    #[test]
    fn test_trade_conserves_assets_at_unit_scale() {
        let market_id = (1_u32 << 16) | 3_u32;
        settle_trade_and_assert_conservation(get_spec(market_id), 1_000, 10_000);
    }

    #[test]
    fn test_trade_conserves_assets_at_shipped_eth_usdt_scale() {
        let market_id = (1_u32 << 16) | 3_u32;
        let spec = CoreMarketSpecification::builder()
            .market_id(market_id)
            .market_type(MarketType::Spot)
            .quote_scale_k(10)
            .base_native_scale(1_000_000_000)
            .quote_native_scale(1_000_000)
            .maker_fee(10)
            .taker_fee(20)
            .slippage(150)
            .build()
            .unwrap();

        settle_trade_and_assert_conservation(spec, 20_000, 1_000_000_000);
    }

    #[test]
    fn test_zero_fees_do_not_create_accumulator_entries() {
        let market_id = (1_u32 << 16) | 3_u32;
        let mut spec = get_spec(market_id);
        spec.maker_fee = 0;
        spec.taker_fee = 0;

        let engine = settle_trade_and_assert_conservation(spec, 1_000, 10_000);
        assert!(engine.accrued_fees.lock().is_empty());
    }

    #[test]
    fn fee_accrual_past_u64_max_still_settles() {
        let market_id = (1_u32 << 16) | 3_u32;
        let base_asset_id = base_asset(market_id);
        let quote_asset_id = quote_asset(market_id);
        let mut specs = HashMap::new();
        specs.insert(market_id, get_spec(market_id));
        let engine = RiskEngine::new(specs, 0, 1);
        let taker_id = 102;
        engine.set_balance(taker_id, quote_asset_id, UserBalance::new(0, 10_000));
        // Where a u64 accumulator saturated: ~9,223 ETH of 20 bp taker buys at 18 decimals.
        engine
            .accrued_fees
            .lock()
            .insert(base_asset_id, u128::from(u64::MAX));

        let mut trade_event = MatcherTradeEvent::default();
        trade_event.price = 1;
        trade_event.size = 10_000;
        trade_event.maker_user_id = 101;
        engine.handle_trade_event(taker_id, market_id, Side::Bid, &mut trade_event, None);

        // 20 bp of 10_000 base units: a fee of 20.
        assert_eq!(
            engine.get_balance(taker_id, quote_asset_id),
            UserBalance::new(0, 0)
        );
        assert_eq!(
            engine.get_balance(taker_id, base_asset_id),
            UserBalance::new(10_000 - 20, 0)
        );
        assert_eq!(
            engine.get_accrued_fee(base_asset_id),
            u128::from(u64::MAX) + 20
        );
    }

    #[test]
    #[should_panic]
    fn test_new_risk_engine_panics_with_non_power_of_two_shards() {
        RiskEngine::new(HashMap::new(), 0, 3);
    }

    #[test]
    fn test_user_id_sharding() {
        let engine_shard0 = RiskEngine::new(HashMap::new(), 0, 4);
        let engine_shard1 = RiskEngine::new(HashMap::new(), 1, 4);

        assert!(engine_shard0.user_id_for_this_handler(0));
        assert!(engine_shard0.user_id_for_this_handler(4));
        assert!(!engine_shard0.user_id_for_this_handler(1));

        assert!(engine_shard1.user_id_for_this_handler(1));
        assert!(engine_shard1.user_id_for_this_handler(5));
        assert!(!engine_shard1.user_id_for_this_handler(0));
        let symbol_spec = HashMap::new();
        let price_cache = Arc::new(PriceCache::new(symbol_spec.keys()));
        let mut cmd = OrderCommand::place_order(TimeInForce::Gtc, 1, 100, 10, Side::Bid, 1, 1);

        // shard 0 should not process user 1's command, will be skipped
        engine_shard0.pre_process_command(&mut cmd, price_cache.clone());
        assert_eq!(cmd.status, Status::Processing);

        // shard 1 should process user 1's command
        // it will be rejected because no market spec
        engine_shard1.pre_process_command(&mut cmd, price_cache);
        assert_eq!(cmd.status, Status::Rejected);
    }

    #[test]
    fn market_gtc_orders_are_rejected_before_locking() {
        let user_id = 1;
        let market_id = (2_u32 << 16) | 1_u32;
        let mut specs = HashMap::new();
        specs.insert(market_id, get_spec(market_id));
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        // A best ask exists, so without the GTC check the buy would be capped and funded.
        price_cache.update_prices(market_id, 90, 100);
        let engine = RiskEngine::new(specs, 0, 1);
        let funded = UserBalance::new(1_000_000, 0);
        engine.set_balance(user_id, base_asset(market_id), funded);
        engine.set_balance(user_id, quote_asset(market_id), funded);

        for (side, sentinel) in [(Side::Bid, u64::MAX), (Side::Ask, 0)] {
            let mut cmd = OrderCommand::place_order(
                TimeInForce::Gtc,
                user_id,
                sentinel,
                10,
                side,
                market_id,
                1,
            );
            engine.pre_process_command(&mut cmd, Arc::clone(&price_cache));

            assert_eq!(cmd.status, Status::Rejected, "{side:?} market GTC");
            assert_eq!(cmd.price, sentinel, "the sentinel must not be rewritten");
        }
        assert_eq!(engine.get_balance(user_id, base_asset(market_id)), funded);
        assert_eq!(engine.get_balance(user_id, quote_asset(market_id)), funded);

        // The same market buy as IOC is still accepted and capped.
        let mut ioc = OrderCommand::place_order(
            TimeInForce::Ioc,
            user_id,
            u64::MAX,
            10,
            Side::Bid,
            market_id,
            1,
        );
        engine.pre_process_command(&mut ioc, price_cache);
        assert_ne!(ioc.status, Status::Rejected);
        assert_eq!(ioc.price, 100);
    }

    #[test]
    fn deposit_with_asset_id_above_u16_is_rejected() {
        let engine = RiskEngine::default();
        let price_cache = Arc::new(PriceCache::new(HashMap::new().keys()));
        engine.set_balance(1, 1, UserBalance::new(100, 0));

        // 65537 would wrap to asset 1.
        for mut cmd in [
            OrderCommand::deposit_funds(1, 100, 1),
            OrderCommand::withdraw_funds(1, 100, 1),
        ] {
            cmd.market_id = 65537;
            engine.pre_process_command(&mut cmd, Arc::clone(&price_cache));
            assert_eq!(cmd.status, Status::Rejected, "{:?}", cmd.command);
            assert_eq!(engine.get_balance(1, 1), UserBalance::new(100, 0));
        }
    }

    #[test]
    fn zero_amount_deposit_and_withdrawal_are_rejected() {
        let engine = RiskEngine::default();
        let price_cache = Arc::new(PriceCache::new(HashMap::new().keys()));

        for mut cmd in [
            OrderCommand::deposit_funds(1, 0, 1),
            OrderCommand::withdraw_funds(1, 0, 1),
        ] {
            engine.pre_process_command(&mut cmd, Arc::clone(&price_cache));
            assert_eq!(cmd.status, Status::Rejected, "{:?}", cmd.command);
        }
        assert_eq!(
            engine.try_get_balance(1, 1),
            Err(RiskEngineError::from(BalanceError::UserAssetNotFound(1, 1)))
        );
    }

    #[test]
    fn withdrawal_takes_only_available_funds() {
        let market_id = (1_u32 << 16) | 2; // quote 1, base 2
        let mut specs = HashMap::new();
        specs.insert(market_id, get_spec(market_id));
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 0, 1);
        let (user, quote) = (1, quote_asset(market_id));

        let mut deposit = OrderCommand::deposit_funds(user, 100, quote);
        engine.pre_process_command(&mut deposit, Arc::clone(&price_cache));
        assert_eq!(deposit.status, Status::Processed);
        // A bid locking 60 quote (price * size at get_spec's unit scales).
        let mut bid =
            OrderCommand::place_order(TimeInForce::Gtc, user, 60, 1, Side::Bid, market_id, 1);
        engine.pre_process_command(&mut bid, Arc::clone(&price_cache));
        assert_eq!(engine.get_balance(user, quote), UserBalance::new(40, 60));

        let mut withdrawal = OrderCommand::withdraw_funds(user, 50, quote);
        engine.pre_process_command(&mut withdrawal, Arc::clone(&price_cache));
        assert_eq!(withdrawal.status, Status::Rejected);
        assert_eq!(engine.get_balance(user, quote), UserBalance::new(40, 60));

        let mut withdrawal = OrderCommand::withdraw_funds(user, 40, quote);
        engine.pre_process_command(&mut withdrawal, price_cache);
        assert_eq!(withdrawal.status, Status::Processed);
        assert_eq!(engine.get_balance(user, quote), UserBalance::new(0, 60));
    }

    #[test]
    fn test_balance_management() {
        let engine = RiskEngine::default();
        let user_id = 1;
        let asset_id = 1;
        let initial_balance = UserBalance::new(1000, 0);

        engine.set_balance(user_id, asset_id, initial_balance);

        let balance = engine.get_balance(user_id, asset_id);
        assert_eq!(balance.available(), 1000);
        assert_eq!(balance.locked(), 0);
        assert_eq!(balance.total(), 1000);

        let balance_res = engine.try_get_balance(user_id, asset_id).unwrap();
        assert_eq!(balance_res, initial_balance);

        let res = engine.try_get_balance(user_id, 2); // unknown asset
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err(),
            RiskEngineError::from(BalanceError::UserAssetNotFound(user_id, 2))
        );
    }

    #[test]
    fn test_reserve_and_cancel_bid() {
        let user_id = 1;
        let market_id = (2_u32 << 16) | 1_u32; // base=1 (e.g. BTC), quote=2 (e.g. USD)
        let quote_asset = quote_asset(market_id);
        let price = 100;
        let size = 10;
        let required_quote = price * size;

        let spec = get_spec(market_id);
        let mut specs = HashMap::new();
        specs.insert(market_id, spec);
        let engine = RiskEngine::new(specs.clone(), 0, 1);

        engine.set_balance(user_id, quote_asset, UserBalance::new(required_quote, 0));

        let mut cmd = OrderCommand::place_order(
            TimeInForce::Gtc,
            user_id,
            price,
            size,
            Side::Bid,
            market_id,
            1,
        );

        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        engine
            .reserve_funds_for_order(&mut cmd, price_cache)
            .unwrap();

        let balance = engine.get_balance(user_id, quote_asset);
        assert_eq!(balance.available(), 0);
        assert_eq!(balance.locked(), required_quote);
        // Cancel the order and check if funds are released
        engine.handle_cancellation(&mut cmd);

        let balance = engine.get_balance(user_id, quote_asset);
        assert_eq!(balance.available(), required_quote);
        assert_eq!(balance.locked(), 0);
    }

    #[test]
    fn test_rejects_bid_whose_quote_cost_rounds_to_zero() {
        let user_id = 1;
        let market_id = (2_u32 << 16) | 1_u32;
        let spec = CoreMarketSpecification::builder()
            .market_id(market_id)
            .market_type(MarketType::Spot)
            .base_native_scale(100)
            .quote_native_scale(1)
            .build()
            .unwrap();
        let mut specs = HashMap::new();
        specs.insert(market_id, spec);
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 0, 1);
        let mut cmd =
            OrderCommand::place_order(TimeInForce::Gtc, user_id, 1, 1, Side::Bid, market_id, 1);

        engine.pre_process_command(&mut cmd, price_cache);

        assert_eq!(cmd.status, Status::Rejected);
    }

    #[test]
    fn test_rejects_ask_whose_quote_cost_rounds_to_zero() {
        let user_id = 1;
        let market_id = (2_u32 << 16) | 1_u32;
        let spec = CoreMarketSpecification::builder()
            .market_id(market_id)
            .market_type(MarketType::Spot)
            .base_native_scale(100)
            .quote_native_scale(1)
            .build()
            .unwrap();
        let mut specs = HashMap::new();
        specs.insert(market_id, spec);
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 0, 1);
        engine.set_balance(user_id, base_asset(market_id), UserBalance::new(1, 0));
        let mut cmd =
            OrderCommand::place_order(TimeInForce::Gtc, user_id, 1, 1, Side::Ask, market_id, 1);

        let error = engine
            .reserve_funds_for_order(&mut cmd, price_cache)
            .unwrap_err();

        assert!(matches!(error, RiskEngineError::InvalidArguments { .. }));
    }

    #[test]
    fn market_buy_rejects_truncating_slippage() {
        let user_id = 1;
        let market_id = (2_u32 << 16) | 1_u32;
        let mut spec = get_spec(market_id);
        // 400%: best_ask * slippage / 10_000 == 2^64, which `as u64` truncated to 0.
        spec.slippage = 40_000;
        let mut specs = HashMap::new();
        specs.insert(market_id, spec);
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        price_cache.update_prices(market_id, 0, 1 << 62);
        let engine = RiskEngine::new(specs, 0, 1);
        let funded = UserBalance::new(u64::MAX, 0);
        engine.set_balance(user_id, quote_asset(market_id), funded);
        let mut cmd = OrderCommand::place_order(
            TimeInForce::Ioc,
            user_id,
            u64::MAX,
            1,
            Side::Bid,
            market_id,
            1,
        );

        assert_eq!(
            engine.reserve_funds_for_order(&mut cmd, price_cache),
            Err(RiskEngineError::BalanceError(BalanceError::Overflow))
        );
        assert_eq!(engine.get_balance(user_id, quote_asset(market_id)), funded);
        assert_eq!(cmd.price, u64::MAX);
    }

    /// A market sell carries the price-0 sentinel (`orderbook/src/lib.rs:425`). Reserving
    /// collateral for it must succeed: the ask locks `size` of the base asset, which the
    /// `cmd.size == 0` guard already proves is non-zero.
    #[test]
    fn test_market_sell_reserves_collateral() {
        let user_id = 1;
        let market_id = (2_u32 << 16) | 1_u32;
        let size = 10;
        let mut specs = HashMap::new();
        specs.insert(market_id, get_spec(market_id));
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 0, 1);
        engine.set_balance(user_id, base_asset(market_id), UserBalance::new(size, 0));
        // price == 0 is the market-SELL sentinel.
        let mut cmd =
            OrderCommand::place_order(TimeInForce::Ioc, user_id, 0, size, Side::Ask, market_id, 1);

        engine
            .reserve_funds_for_order(&mut cmd, price_cache)
            .expect("a market sell must be able to reserve collateral");

        let balance = engine.get_balance(user_id, base_asset(market_id));
        assert_eq!(balance.locked, size, "market sell must lock `size` of base");
    }

    #[test]
    fn test_normal_ask_locks_expected_base_amount() {
        let user_id = 1;
        let market_id = (2_u32 << 16) | 1_u32;
        let price = 100;
        let size = 10;
        let mut specs = HashMap::new();
        specs.insert(market_id, get_spec(market_id));
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 0, 1);
        let base_asset = base_asset(market_id);
        engine.set_balance(user_id, base_asset, UserBalance::new(size, 0));
        let mut cmd = OrderCommand::place_order(
            TimeInForce::Gtc,
            user_id,
            price,
            size,
            Side::Ask,
            market_id,
            1,
        );

        engine
            .reserve_funds_for_order(&mut cmd, price_cache)
            .unwrap();

        assert_eq!(
            engine.get_balance(user_id, base_asset),
            UserBalance::new(0, size)
        );
    }

    #[test]
    fn test_rejects_zero_size_order() {
        let user_id = 1;
        let market_id = (2_u32 << 16) | 1_u32;
        let mut specs = HashMap::new();
        specs.insert(market_id, get_spec(market_id));
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 0, 1);
        let mut cmd =
            OrderCommand::place_order(TimeInForce::Gtc, user_id, 100, 0, Side::Ask, market_id, 1);

        engine.pre_process_command(&mut cmd, price_cache);

        assert_eq!(cmd.status, Status::Rejected);
    }

    #[test]
    fn test_normal_bid_locks_expected_quote_amount() {
        let user_id = 1;
        let market_id = (2_u32 << 16) | 1_u32;
        let price = 100;
        let size = 10;
        let required_quote = price * size;
        let mut specs = HashMap::new();
        specs.insert(market_id, get_spec(market_id));
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 0, 1);
        let quote_asset = quote_asset(market_id);
        engine.set_balance(user_id, quote_asset, UserBalance::new(required_quote, 0));
        let mut cmd = OrderCommand::place_order(
            TimeInForce::Gtc,
            user_id,
            price,
            size,
            Side::Bid,
            market_id,
            1,
        );

        engine.pre_process_command(&mut cmd, price_cache);

        assert_ne!(cmd.status, Status::Rejected);
        assert_eq!(
            engine.get_balance(user_id, quote_asset),
            UserBalance::new(0, required_quote)
        );
    }

    #[test]
    fn test_reserve_and_cancel_ask() {
        let user_id = 1;
        let market_id = (2_u32 << 16) | 1_u32; // base=1 (e.g. BTC), quote=2 (e.g. USD)
        let base_asset = base_asset(market_id);
        let price = 100;
        let size = 10;

        let spec = get_spec(market_id);
        let mut specs = HashMap::new();
        specs.insert(market_id, spec);
        let engine = RiskEngine::new(specs.clone(), 0, 1);

        engine.set_balance(user_id, base_asset, UserBalance::new(size, 0));

        let mut cmd = OrderCommand::place_order(
            TimeInForce::Gtc,
            user_id,
            price,
            size,
            Side::Ask,
            market_id,
            1,
        );

        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        engine
            .reserve_funds_for_order(&mut cmd, price_cache)
            .unwrap();

        let balance = engine.get_balance(user_id, base_asset);
        assert_eq!(balance.available(), 0);
        assert_eq!(balance.locked(), size);
        // Cancel the order and check if funds are released
        engine.handle_cancellation(&mut cmd);

        let balance = engine.get_balance(user_id, base_asset);
        assert_eq!(balance.available(), size);
        assert_eq!(balance.locked(), 0);
    }

    #[test]
    fn failed_cancellation_release_keeps_matching_status() {
        let user_id = 1;
        let market_id = (2_u32 << 16) | 1_u32;
        let mut specs = HashMap::new();
        specs.insert(market_id, get_spec(market_id));
        let engine = RiskEngine::new(specs, 0, 1);
        // PR #171 made the BID path release only what was actually tracked, so a missing
        // collateral record is deliberately NOT an error (see
        // test_bid_cancel_without_collateral_record_is_safe). The ASK path unlocks base
        // directly, so locking less than the order's size makes its release genuinely fail.
        let balance = UserBalance::new(5, 3);
        engine.set_balance(user_id, base_asset(market_id), balance);

        // By the time R2 releases, the book has already removed the order (Cancelled) or traded
        // part of it (IOC PartiallyFilled); neither may be reported as Rejected.
        for (time_in_force, status) in [
            (TimeInForce::Gtc, Status::Cancelled),
            (TimeInForce::Ioc, Status::PartiallyFilled),
        ] {
            let mut cmd =
                OrderCommand::place_order(time_in_force, user_id, 100, 10, Side::Ask, market_id, 1);
            cmd.set_status(status);

            engine.handle_cancellation(&mut cmd);

            assert_eq!(cmd.status, status);
            assert_eq!(engine.get_balance(user_id, base_asset(market_id)), balance);
        }
    }

    #[test]
    fn test_insufficient_funds() {
        let user_id = 1;
        let market_id = (2_u32 << 16) | 1_u32;
        let quote_asset = quote_asset(market_id);
        let price = 100;
        let size = 10;
        let required_quote = price * size;

        let spec = get_spec(market_id);
        let mut specs = HashMap::new();
        specs.insert(market_id, spec);
        let engine = RiskEngine::new(specs.clone(), 0, 1);

        engine.set_balance(
            user_id,
            quote_asset,
            UserBalance::new(required_quote - 1, 0),
        );

        let mut cmd = OrderCommand::place_order(
            TimeInForce::Gtc,
            user_id,
            price,
            size,
            Side::Bid,
            market_id,
            1,
        );

        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let res = engine.reserve_funds_for_order(&mut cmd, price_cache);
        assert!(res.is_err());
        match res.unwrap_err() {
            RiskEngineError::BalanceError(BalanceError::InsufficientAvailableFunds { .. }) => (),
            e => panic!("Unexpected error: {e:?}"),
        }
    }

    #[test]
    fn test_trade_settlement() {
        // Market: BTC/USD -> base=BTC(2), quote=USD(1)
        let btc_asset_id = 2u16;
        let usd_asset_id = 1u16;
        let market_id = ((usd_asset_id as u32) << 16) | (btc_asset_id as u32);

        let mut specs = HashMap::new();
        specs.insert(market_id, get_spec(market_id));
        let price_cache = Arc::new(PriceCache::new(specs.keys()));

        let engine = RiskEngine::new(specs, 0, 1);

        let maker_id = 101; // Sells BTC for USD (ASK)
        let taker_id = 102; // Buys BTC with USD (BID)

        let price = 50_000;
        let size = 20_000; // amount of BTC

        // --- Initial Balances ---
        // Taker (buyer) needs USD (quote) to buy BTC (base).
        let taker_initial_quote = price * size;
        // Maker (seller) needs BTC (base) to sell for USD (quote).
        let maker_initial_base = size;

        let mut balances = engine.balances.lock();
        balances.set_balance(
            taker_id,
            usd_asset_id,
            UserBalance::new(taker_initial_quote, 0),
        );
        balances.set_balance(
            maker_id,
            btc_asset_id,
            UserBalance::new(maker_initial_base, 0),
        );
        drop(balances);

        // --- Reserve funds ---
        // Taker places BID order to buy BTC
        let mut taker_cmd = OrderCommand::place_order(
            TimeInForce::Gtc,
            taker_id,
            price,
            size,
            Side::Bid,
            market_id,
            1,
        );
        engine
            .reserve_funds_for_order(&mut taker_cmd, price_cache.clone())
            .unwrap();

        // Maker places ASK order to sell BTC
        let mut maker_cmd = OrderCommand::place_order(
            TimeInForce::Gtc,
            maker_id,
            price,
            size,
            Side::Ask,
            market_id,
            2,
        );
        engine
            .reserve_funds_for_order(&mut maker_cmd, price_cache.clone())
            .unwrap();

        // --- A trade occurs ---
        let mut trade_event = MatcherTradeEvent {
            price,
            size,
            maker_user_id: maker_id,
            active_order_completed: false,
            matched_order_id: 2,
            matched_order_completed: true,
            next_event: None,
            maker_balance: [UserBalance::default(); 2],
            maker_remaining_size: 0,
            maker_original_size: size,
        };

        // Settle for Taker (Buyer, Bid side) - buys base (BTC) with quote (USD)
        engine.handle_trade_event(
            taker_id,
            market_id,
            Side::Bid,
            &mut trade_event,
            Some((taker_cmd.order_id, price)),
        );

        // Settle for Maker (Seller, Ask side) - sells base (BTC) for quote (USD)
        engine.handle_trade_event(maker_id, market_id, Side::Ask, &mut trade_event, None);

        // --- Final Balances ---
        // Taker (buyer): Spends `price * size` of USD (quote). Receives `size` of BTC (base), minus taker fee (20bp on base).
        let taker_fee_in_base = (size * 20) / 10000;
        let net_base_received = size - taker_fee_in_base;
        assert_eq!(engine.get_balance(taker_id, usd_asset_id).total(), 0);
        assert_eq!(
            engine.get_balance(taker_id, btc_asset_id).total(),
            net_base_received
        );

        // Maker (seller): Spends `size` of BTC (base). Receives `price * size` of USD (quote), minus maker fee (10bp on quote).
        let gross_quote_received = price * size;
        let maker_fee_in_quote = (gross_quote_received * 10) / 10000;
        let net_quote_received = gross_quote_received - maker_fee_in_quote;
        assert_eq!(
            engine.get_balance(maker_id, usd_asset_id).total(),
            net_quote_received
        );
        assert_eq!(engine.get_balance(maker_id, btc_asset_id).total(), 0);
    }

    #[test]
    fn test_trade_settlement_subtract_failure_leaves_balances_unchanged() {
        let base_asset_id = 2u16;
        let quote_asset_id = 1u16;
        let market_id = ((quote_asset_id as u32) << 16) | base_asset_id as u32;
        let spec = get_spec(market_id);
        let mut specs = HashMap::new();
        specs.insert(market_id, spec.clone());
        let engine = RiskEngine::new(specs, 0, 1);

        let maker_id = 101;
        let taker_id = 102;
        let execution_price = 100;
        let limit_price = 110;
        let size = 10;

        engine.set_balance(taker_id, base_asset_id, UserBalance::new(7, 0));
        engine.set_balance(taker_id, quote_asset_id, UserBalance::new(3, 150));
        engine.set_balance(maker_id, base_asset_id, UserBalance::new(5, size));
        engine.set_balance(maker_id, quote_asset_id, UserBalance::new(11, 0));

        let balances_before = [
            engine.get_balance(taker_id, base_asset_id),
            engine.get_balance(taker_id, quote_asset_id),
            engine.get_balance(maker_id, base_asset_id),
            engine.get_balance(maker_id, quote_asset_id),
        ];
        // MatcherTradeEvent implements Drop (iterative chain teardown, PR #179), so the
        // functional-update form would move out of a Drop type. Assign the fields instead.
        let mut trade_event = MatcherTradeEvent::default();
        trade_event.price = execution_price;
        trade_event.size = size;
        trade_event.maker_user_id = maker_id;

        let result = engine.settle_trade(
            taker_id,
            market_id,
            Side::Bid,
            &mut trade_event,
            &spec,
            Some((1, limit_price)),
        );

        assert_eq!(
            result,
            Err(RiskEngineError::BalanceError(
                BalanceError::InsufficientLockedFunds {
                    locked: 150,
                    needed: 1_100,
                }
            ))
        );
        assert_eq!(
            [
                engine.get_balance(taker_id, base_asset_id),
                engine.get_balance(taker_id, quote_asset_id),
                engine.get_balance(maker_id, base_asset_id),
                engine.get_balance(maker_id, quote_asset_id),
            ],
            balances_before
        );
    }

    #[test]
    fn settle_trade_failure_leaves_the_store_and_collateral_untouched() {
        let base_asset_id = 2u16;
        let quote_asset_id = 1u16;
        let market_id = ((quote_asset_id as u32) << 16) | base_asset_id as u32;
        let spec = get_spec(market_id);
        let mut specs = HashMap::new();
        specs.insert(market_id, spec.clone());
        let engine = RiskEngine::new(specs, 0, 1);
        let user_id = 102;
        let order_id = 7;

        // The order's record says 100 is locked, the store only 60: this completing fill of 50
        // also releases the 50 residue, which the store cannot cover.
        engine.bid_collateral.lock().insert(order_id, 100);
        engine.set_balance(user_id, quote_asset_id, UserBalance::new(0, 60));
        let mut trade_event = MatcherTradeEvent::default();
        trade_event.price = 1;
        trade_event.size = 50;
        trade_event.maker_user_id = 101;
        trade_event.active_order_completed = true;

        let result = engine.settle_trade(
            user_id,
            market_id,
            Side::Bid,
            &mut trade_event,
            &spec,
            Some((order_id, 1)),
        );

        assert_eq!(
            result,
            Err(RiskEngineError::BalanceError(
                BalanceError::InsufficientLockedFunds {
                    locked: 60,
                    needed: 100,
                }
            ))
        );
        assert_eq!(
            engine.get_balance(user_id, quote_asset_id),
            UserBalance::new(0, 60)
        );
        assert_eq!(
            engine.get_balance(user_id, base_asset_id),
            UserBalance::default()
        );
        assert_eq!(engine.bid_collateral.lock().get(&order_id), Some(&100));
        assert_eq!(engine.get_accrued_fee(base_asset_id), 0);
    }

    #[test]
    fn test_trade_settlement_fee_at_1e18_units() {
        let base_asset_id = 2u16;
        let quote_asset_id = 1u16;
        let market_id = ((quote_asset_id as u32) << 16) | base_asset_id as u32;
        let user_id = 102;
        let price = 1;
        let size = 1_000_000_000_000_000_000;

        let mut specs = HashMap::new();
        specs.insert(market_id, get_spec(market_id));
        let engine = RiskEngine::new(specs, 0, 1);
        engine.set_balance(user_id, quote_asset_id, UserBalance::new(0, size));

        let mut trade_event = MatcherTradeEvent {
            price,
            size,
            maker_user_id: 101,
            active_order_completed: false,
            matched_order_id: 1,
            matched_order_completed: true,
            next_event: None,
            maker_balance: [UserBalance::default(); 2],
            maker_remaining_size: 0,
            maker_original_size: size,
        };

        engine.handle_trade_event(
            user_id,
            market_id,
            Side::Bid,
            &mut trade_event,
            Some((1, price)),
        );

        let expected_fee = 2_000_000_000_000_000;
        assert_eq!(
            engine.get_balance(user_id, base_asset_id).total(),
            size - expected_fee
        );
        assert_eq!(engine.get_balance(user_id, quote_asset_id).total(), 0);
    }

    #[test]
    fn test_sliced_fills_each_pay_a_fee() {
        let fees = settled_bid_fees(0, 20, false, &[499; 20]);

        assert_eq!(fees, vec![1; 20]);
        assert_eq!(fees.iter().sum::<u64>(), 20);
    }

    #[test]
    fn test_zero_maker_fee_stays_zero() {
        assert_eq!(settled_bid_fees(0, 20, true, &[499]), vec![0]);
    }

    #[test]
    fn test_exact_multiple_fee_is_unchanged() {
        assert_eq!(settled_bid_fees(0, 20, false, &[1_000]), vec![2]);
    }

    #[test]
    fn test_one_unit_fill_pays_one_unit_fee() {
        assert_eq!(settled_bid_fees(0, 20, false, &[1]), vec![1]);
    }

    /// Pins the documented per-fill rounding bound: every fill pays at least its exact share and
    /// less than one unit more, so slicing is never cheaper than a single fill.
    #[test]
    fn test_per_fill_fee_overcharge_is_below_one_unit() {
        let fill_sizes = [1, 49, 50, 499, 500, 501, 4_999, 10_000, 123_457];
        let total_size: u64 = fill_sizes.iter().sum();
        for fee_bps in [1, 20, 9_999] {
            let fees = settled_bid_fees(0, fee_bps, false, &fill_sizes);
            for (&size, &fee) in fill_sizes.iter().zip(&fees) {
                // Compare in units of 1/10_000 so the exact share needs no rounding.
                let exact = u128::from(size) * u128::from(fee_bps);
                let charged = u128::from(fee) * 10_000;
                assert!(charged >= exact, "{fee_bps} bp on {size}: under-charged");
                assert!(
                    charged < exact + 10_000,
                    "{fee_bps} bp on {size}: over-charged by a unit or more"
                );
            }

            let single_fill_fee = settled_bid_fees(0, fee_bps, false, &[total_size])[0];
            let sliced_fee: u64 = fees.iter().sum();
            assert!(
                sliced_fee >= single_fill_fee,
                "{fee_bps} bp: slicing was cheaper"
            );
            assert!(sliced_fee - single_fill_fee < fill_sizes.len() as u64);
        }
    }

    #[test]
    fn test_market_order_reservation() {
        // --- Setup ---
        // Market: BTC/USD -> base=BTC(2), quote=USD(1)
        let btc_asset_id = 2u16;
        let usd_asset_id = 1u16;
        let market_id = ((usd_asset_id as u32) << 16) | (btc_asset_id as u32);

        let mut specs = HashMap::new();
        specs.insert(
            market_id,
            get_spec(market_id), // slippage is 5bp
        );
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 0, 1);

        let user_id = 1;
        let size = 10; // size in base asset (BTC)

        // --- Test 1: Market Buy with no liquidity ---
        let mut market_buy_cmd = OrderCommand::place_order(
            TimeInForce::Gtc,
            user_id,
            u64::MAX,
            size,
            Side::Bid,
            market_id,
            1,
        );

        let res = engine.reserve_funds_for_order(&mut market_buy_cmd, price_cache.clone());
        assert!(res.is_err());
        match res.unwrap_err() {
            RiskEngineError::InvalidArguments { .. } => (),
            e => panic!("Expected InvalidArguments, got {e:?}"),
        }

        // --- Test 2: Market Buy with liquidity ---
        let best_ask = 50000;
        price_cache.update_prices(market_id, 49990, best_ask);

        // Slippage is 5bp. Conservative price = 50000 + (50000 * 5 / 10000) = 50000 + 25 = 50025
        let conservative_price = 50025;
        let required_quote = conservative_price * size;
        engine.set_balance(user_id, usd_asset_id, UserBalance::new(required_quote, 0));

        engine
            .reserve_funds_for_order(&mut market_buy_cmd, price_cache.clone())
            .unwrap();

        let balance = engine.get_balance(user_id, usd_asset_id);
        assert_eq!(balance.available(), 0);
        assert_eq!(balance.locked(), required_quote);
        assert_eq!(
            market_buy_cmd.price, conservative_price,
            "a locked market buy carries its cap"
        );

        // --- Test 3: Market Buy with insufficient funds ---
        // Step 2 locked and so re-priced its command; a market buy must start from the sentinel.
        let mut market_buy_cmd = OrderCommand::place_order(
            TimeInForce::Gtc,
            user_id,
            u64::MAX,
            size,
            Side::Bid,
            market_id,
            1,
        );
        engine.set_balance(
            user_id,
            usd_asset_id,
            UserBalance::new(required_quote - 1, 0),
        );
        let res = engine.reserve_funds_for_order(&mut market_buy_cmd, price_cache.clone());
        assert!(res.is_err());
        match res.unwrap_err() {
            RiskEngineError::BalanceError(BalanceError::InsufficientAvailableFunds { .. }) => (),
            e => panic!("Expected InsufficientAvailableFunds, got {e:?}"),
        }
        assert_eq!(
            market_buy_cmd.price,
            u64::MAX,
            "a rejected market buy keeps the market sentinel"
        );

        // --- Test 4: Market Sell ---
        // A market SELL carries price 0; u64::MAX is the market BUY sentinel. This previously
        // passed only because calculate_quote_cost saturated an unrepresentable notional
        // (u64::MAX * size) to u64::MAX instead of rejecting it -- finding C4-1.
        engine.set_balance(user_id, btc_asset_id, UserBalance::new(size, 0));
        let mut market_sell_cmd =
            OrderCommand::place_order(TimeInForce::Gtc, user_id, 0, size, Side::Ask, market_id, 1);

        engine
            .reserve_funds_for_order(&mut market_sell_cmd, price_cache.clone())
            .unwrap();
        let balance = engine.get_balance(user_id, btc_asset_id);
        assert_eq!(balance.available(), 0);
        assert_eq!(balance.locked(), size);

        // --- Test 5: a limit ask whose notional cannot be represented is rejected (C4-1) ---
        let mut unrepresentable_ask = OrderCommand::place_order(
            TimeInForce::Gtc,
            user_id,
            u64::MAX,
            size,
            Side::Ask,
            market_id,
            1,
        );
        assert!(
            engine
                .reserve_funds_for_order(&mut unrepresentable_ask, price_cache)
                .is_err()
        );
    }

    #[test]
    fn market_buy_rejects_zero_reference_price() {
        let user_id = 1;
        let usd_asset_id = 1u16;
        let btc_asset_id = 2u16;
        let market_id = ((usd_asset_id as u32) << 16) | btc_asset_id as u32;
        let mut specs = HashMap::new();
        specs.insert(market_id, get_spec(market_id));
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        price_cache.update_prices(market_id, 0, 0);
        let engine = RiskEngine::new(specs, 0, 1);
        engine.set_balance(user_id, usd_asset_id, UserBalance::new(1_000, 0));
        let mut cmd = OrderCommand::place_order(
            TimeInForce::Gtc,
            user_id,
            u64::MAX,
            10,
            Side::Bid,
            market_id,
            1,
        );

        assert!(matches!(
            engine.reserve_funds_for_order(&mut cmd, price_cache),
            Err(RiskEngineError::InvalidArguments { .. })
        ));
        assert_eq!(
            engine.get_balance(user_id, usd_asset_id),
            UserBalance::new(1_000, 0)
        );
    }

    #[test]
    fn test_parallel_markets_and_settlement() {
        // This test simulates a user trading on two different markets concurrently.
        // It verifies that funds are locked, settled, and cancelled correctly across markets.
        // Market 1: BTC/USD -> base=BTC(2), quote=USD(1)
        // Market 2: ETH/USD -> base=ETH(3), quote=USD(1)
        let usd_asset_id = 1u16;
        let btc_asset_id = 2u16;
        let eth_asset_id = 3u16;

        let market_id_btc_usd = ((usd_asset_id as u32) << 16) | (btc_asset_id as u32);
        let market_id_eth_usd = ((usd_asset_id as u32) << 16) | (eth_asset_id as u32);

        let mut specs = HashMap::new();
        specs.insert(market_id_btc_usd, get_spec(market_id_btc_usd));
        specs.insert(market_id_eth_usd, get_spec(market_id_eth_usd));
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 0, 1);

        let user_id = 100;

        // --- Initial Balances ---
        // User has 100,000,000 USD, 10,000 BTC, 50,000 ETH
        let mut balances = engine.balances.lock();
        balances.set_balance(user_id, usd_asset_id, UserBalance::new(100_000_000, 0));
        balances.set_balance(user_id, btc_asset_id, UserBalance::new(10_000, 0));
        balances.set_balance(user_id, eth_asset_id, UserBalance::new(50_000, 0));
        drop(balances);

        // --- Action 1: User places a BID order on BTC/USD market ---
        // Buy 1,000 BTC (base) for 50,000 USD (quote) each. Total cost: 50,000,000 USD
        let btc_price = 50_000;
        let btc_size = 1_000;
        let mut btc_buy_cmd = OrderCommand::place_order(
            TimeInForce::Gtc,
            user_id,
            btc_price,
            btc_size,
            Side::Bid,
            market_id_btc_usd,
            1,
        );
        engine
            .reserve_funds_for_order(&mut btc_buy_cmd, price_cache.clone())
            .unwrap();

        // --- Check balances after BTC order reservation ---
        assert_eq!(
            engine.get_balance(user_id, usd_asset_id).available(),
            50_000_000
        ); // 100M - 50M
        assert_eq!(
            engine.get_balance(user_id, usd_asset_id).locked(),
            50_000_000
        );
        assert_eq!(engine.get_balance(user_id, btc_asset_id).total(), 10_000); // unchanged
        assert_eq!(engine.get_balance(user_id, eth_asset_id).total(), 50_000); // unchanged

        // --- Action 2: User places an ASK order on ETH/USD market ---
        // Sell 2,000 ETH (base) for 3,000 USD (quote) each.
        let eth_price = 3_000;
        let eth_size = 2_000;
        let mut eth_sell_cmd = OrderCommand::place_order(
            TimeInForce::Gtc,
            user_id,
            eth_price,
            eth_size,
            Side::Ask,
            market_id_eth_usd,
            2,
        );
        engine
            .reserve_funds_for_order(&mut eth_sell_cmd, price_cache.clone())
            .unwrap();

        // --- Check balances after both reservations ---
        assert_eq!(
            engine.get_balance(user_id, usd_asset_id).total(),
            100_000_000
        );
        assert_eq!(
            engine.get_balance(user_id, usd_asset_id).locked(),
            50_000_000
        );
        assert_eq!(engine.get_balance(user_id, btc_asset_id).total(), 10_000);
        assert_eq!(
            engine.get_balance(user_id, eth_asset_id).available(),
            48_000
        ); // 50k - 2k
        assert_eq!(engine.get_balance(user_id, eth_asset_id).locked(), 2_000);

        // --- Action 3: The BTC buy order is filled (as taker) ---
        let mut btc_trade_event = MatcherTradeEvent {
            price: btc_price,
            size: btc_size,
            maker_user_id: 200,
            active_order_completed: true,
            matched_order_id: 99,
            matched_order_completed: true,
            next_event: None,
            maker_balance: [UserBalance::default(); 2],
            maker_remaining_size: 0,
            maker_original_size: btc_size,
        };
        engine.handle_trade_event(
            user_id,
            market_id_btc_usd,
            Side::Bid,
            &mut btc_trade_event,
            Some((btc_buy_cmd.order_id, btc_price)),
        );

        // --- Check balances after BTC trade settlement ---
        // User (taker) spent 50,000,000 USD, received 1,000 BTC (minus 0.2% taker fee)
        let btc_taker_fee = (btc_size * 20) / 10000; // 0.2% of 1000 = 2
        let net_btc_received = btc_size - btc_taker_fee;

        assert_eq!(
            engine.get_balance(user_id, usd_asset_id).total(),
            50_000_000
        ); // 100M - 50M
        assert_eq!(engine.get_balance(user_id, usd_asset_id).locked(), 0);
        assert_eq!(
            engine.get_balance(user_id, btc_asset_id).total(),
            10_000 + net_btc_received
        ); // 10k + 998
        assert_eq!(engine.get_balance(user_id, eth_asset_id).total(), 50_000); // ETH balance is untouched by BTC trade
        assert_eq!(engine.get_balance(user_id, eth_asset_id).locked(), 2_000); // ETH lock is untouched

        // --- Action 4: The ETH sell order is cancelled ---
        engine.handle_cancellation(&mut eth_sell_cmd);

        // --- Final Balances ---
        assert_eq!(
            engine.get_balance(user_id, usd_asset_id).total(),
            50_000_000
        );
        assert_eq!(engine.get_balance(user_id, btc_asset_id).total(), 10_998);
        assert_eq!(engine.get_balance(user_id, eth_asset_id).total(), 50_000);
        assert_eq!(
            engine.get_balance(user_id, eth_asset_id).available(),
            50_000
        ); // Funds unlocked
        assert_eq!(engine.get_balance(user_id, eth_asset_id).locked(), 0);
    }

    fn get_shipped_eth_spec(market_id: u32) -> CoreMarketSpecification {
        CoreMarketSpecification::builder()
            .market_id(market_id)
            .market_type(MarketType::Spot)
            .quote_scale_k(10)
            .base_native_scale(1_000_000_000)
            .quote_native_scale(1_000_000)
            .maker_fee(0)
            .taker_fee(0)
            .slippage(5)
            .build()
            .unwrap()
    }

    fn assert_shipped_eth_bid_accounting(price: u64, fill_sizes: &[u64], fully_filled: bool) {
        let user_id = 50;
        let market_id = (2_u32 << 16) | 1_u32;
        let order_size = 1_000_000_000;
        // Covers the 1 ETH lock at 30_000 (3_000_000_000) and at 30_001 (3_000_100_000).
        let initial_quote = 3_000_100_000;
        let order_id = 42;

        let spec = get_shipped_eth_spec(market_id);
        let mut specs = HashMap::new();
        specs.insert(market_id, spec.clone());
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 2, 4);
        engine.set_balance(
            user_id,
            quote_asset(market_id),
            UserBalance::new(initial_quote, 0),
        );

        let mut cmd = OrderCommand::place_order(
            TimeInForce::Gtc,
            user_id,
            price,
            order_size,
            Side::Bid,
            market_id,
            1,
        );
        cmd.order_id = order_id;
        engine
            .reserve_funds_for_order(&mut cmd, price_cache)
            .unwrap();

        let mut spent = 0;
        for (index, &fill_size) in fill_sizes.iter().enumerate() {
            let completed = fully_filled && index + 1 == fill_sizes.len();
            let mut event = MatcherTradeEvent {
                active_order_completed: false,
                matched_order_id: order_id,
                maker_user_id: user_id,
                matched_order_completed: completed,
                price,
                size: fill_size,
                next_event: None,
                maker_balance: [UserBalance::default(); 2],
                maker_remaining_size: 0,
                maker_original_size: fill_size,
            };
            engine.handle_trade_event(user_id, market_id, Side::Bid, &mut event, None);
            cmd.size -= fill_size;
            spent += spec
                .calculate_quote_cost(price, fill_size)
                .expect("test notional fits");
        }

        if !fully_filled {
            engine.handle_cancellation(&mut cmd);
        }

        let balance = engine.get_balance(user_id, quote_asset(market_id));
        assert_eq!(balance.available(), initial_quote - spent);
        assert_eq!(balance.locked(), 0);
        assert_eq!(balance.total() + spent, initial_quote);
        assert!(!engine.bid_collateral.lock().contains_key(&order_id));
    }

    #[test]
    fn test_shipped_eth_bid_full_fill_has_no_collateral_residue() {
        assert_shipped_eth_bid_accounting(30_000, &[333_333_334, 666_666_666], true);
    }

    #[test]
    fn test_shipped_eth_bid_full_fill_releases_rounding_residue() {
        // At 30_001 the lock is 3_000_100_000 and the fills cost 1_000_033_335 + 2_000_066_664:
        // the last fill releases the 1 unit residue along with its own settlement.
        assert_shipped_eth_bid_accounting(30_001, &[333_333_334, 666_666_666], true);
    }

    #[test]
    fn test_shipped_eth_bid_partial_fill_cancel_has_no_collateral_residue() {
        assert_shipped_eth_bid_accounting(30_000, &[333_333_334], false);
    }

    #[test]
    fn test_shipped_eth_bid_cancel_without_fill_has_no_collateral_residue() {
        assert_shipped_eth_bid_accounting(30_000, &[], false);
    }

    #[test]
    fn test_shipped_eth_bid_multiple_partial_fills_cancel_has_no_collateral_residue() {
        assert_shipped_eth_bid_accounting(30_000, &[333_333_334, 333_333_334], false);
    }

    #[test]
    fn test_bid_cancel_without_collateral_record_is_safe() {
        let user_id = 50;
        let market_id = (2_u32 << 16) | 1_u32;
        let mut specs = HashMap::new();
        specs.insert(market_id, get_shipped_eth_spec(market_id));
        let engine = RiskEngine::new(specs, 2, 4);
        engine.set_balance(user_id, quote_asset(market_id), UserBalance::new(10, 0));
        let mut cmd = OrderCommand::cancel_order(404, user_id, Side::Bid, market_id);

        engine.handle_cancellation(&mut cmd);

        assert_eq!(
            engine.get_balance(user_id, quote_asset(market_id)),
            UserBalance::new(10, 0)
        );
    }

    #[test]
    fn test_shipped_eth_ask_partial_fill_cancel_uses_raw_size() {
        let user_id = 50;
        let market_id = (2_u32 << 16) | 1_u32;
        let price = 30_000;
        let order_size = 1_000_000_000;
        let fill_size = 333_333_334;
        let mut specs = HashMap::new();
        specs.insert(market_id, get_shipped_eth_spec(market_id));
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 2, 4);
        engine.set_balance(
            user_id,
            base_asset(market_id),
            UserBalance::new(order_size, 0),
        );
        let mut cmd = OrderCommand::place_order(
            TimeInForce::Gtc,
            user_id,
            price,
            order_size,
            Side::Ask,
            market_id,
            1,
        );
        cmd.order_id = 42;
        engine
            .reserve_funds_for_order(&mut cmd, price_cache)
            .unwrap();
        let mut event = MatcherTradeEvent {
            active_order_completed: false,
            matched_order_id: cmd.order_id,
            maker_user_id: user_id,
            matched_order_completed: false,
            price,
            size: fill_size,
            next_event: None,
            maker_balance: [UserBalance::default(); 2],
            maker_remaining_size: 0,
            maker_original_size: fill_size,
        };
        engine.handle_trade_event(user_id, market_id, Side::Ask, &mut event, None);
        cmd.size -= fill_size;

        engine.handle_cancellation(&mut cmd);

        let balance = engine.get_balance(user_id, base_asset(market_id));
        assert_eq!(balance.available(), order_size - fill_size);
        assert_eq!(balance.locked(), 0);
        assert_eq!(balance.total() + fill_size, order_size);
    }
}
