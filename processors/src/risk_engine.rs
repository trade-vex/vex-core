use crate::error::{Result, RiskEngineError};
use common::BalanceError;
use common::BalanceStore;
use common::CoreMarketSpecification;
use common::MatcherTradeEvent;
use common::OrderCommand;
use common::OrderCommandType;
use common::Side;
use common::Status;
use common::UserBalance;
use hashbrown::HashMap;
use parking_lot::Mutex;
use std::sync::Arc;
use tracing::error;
use tracing::{info, warn};

/// Manages all user profiles and performs risk checks as well as settlements
pub struct RiskEngine {
    balances: Arc<Mutex<BalanceStore>>,
    pub symbol_specs: HashMap<u32, CoreMarketSpecification>,
    shard_id: u32,
    shard_mask: u64,
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
            symbol_specs,
            shard_id,
            shard_mask: (num_shards - 1) as u64,
        }
    }

    /// Checks if a user ID is handled by this risk engine instance.
    #[inline]
    fn user_id_for_this_handler(&self, user_id: u64) -> bool {
        (user_id & self.shard_mask) == self.shard_id as u64
    }

    /// Pre-processes a command to validate it and hold funds
    pub fn pre_process_command(&self, cmd: &mut OrderCommand) {
        // Process only if the command is for a user managed by this shard
        if !self.user_id_for_this_handler(cmd.user_id) {
            return; // Not for this shard, skip
        }

        info!(
            "[RiskEngine_{}] Pre-processing command: {:?}",
            self.shard_id, cmd
        );

        // Validate the command arguments
        info!(
            "[RiskEngine] Validating arguments for order {}",
            cmd.order_id
        );
        if matches!(cmd.command, OrderCommandType::PlaceOrder) {
            info!(
                "[RiskEngine] Looking up market_id spec for market_id {}",
                cmd.market_id
            );

            if self.symbol_specs.get(&cmd.market_id).is_none() {
                warn!(
                    "[RiskEngine] Market spec not found for market_id {}",
                    cmd.market_id
                );
                cmd.status = common::Status::Rejected;
                return;
            }

            info!(
                "[RiskEngine] Found market_id spec for market_id {}",
                cmd.market_id
            );

            // TODO: Handle Market Order
            // Note: The Fee's are always in receiving asset, hense are cut on post processing
            if let Err(err) = self.reserve_funds_for_order(cmd) {
                warn!(
                    "[RiskEngine] Insufficient funds for user {}: {:?}",
                    cmd.user_id, err
                );
                cmd.set_status(Status::Rejected);
            }
        }

        info!(
            "[RiskEngine] Pre-processing and approving command for user {}",
            cmd.user_id
        );
    }

    /// Handles a single trade event from the matching engine to settle funds
    /// This is called by the R2 handler for each individual event in the linked list
    pub fn handle_trade_event(
        &self,
        user_id: u64,
        market_id: u32,
        side: Side,
        event: &MatcherTradeEvent,
    ) {
        info!(
            "[RiskEngine_{}] Processing settelement for user: {}, event: maker={:?}, price={}, size={}",
            self.shard_id,
            user_id,
            event.maker_user_id,
            event.price,
            event.size
        );

        // Get market specification for fee calculations
        let spec = match self.symbol_specs.get(&market_id) {
            Some(spec) => spec,
            None => {
                warn!(
                    "[RiskEngine_{}] Market spec not found for market_id {}",
                    self.shard_id, market_id
                );
                return;
            }
        };

        if let Err(err) = self.settle_trade(user_id, market_id, side, event, spec) {
            error!(
                "[RiskEngine_{}] Failed to settle trade for user {}: {:?}",
                self.shard_id, user_id, err
            );
        } else {
            info!(
                "[RiskEngine_{}] Successfully settled trade for user {}",
                self.shard_id, user_id
            );
        }
    }

    fn settle_trade(
        &self,
        user_id: u64,
        market_id: u32,
        side: Side,
        event: &MatcherTradeEvent,
        spec: &CoreMarketSpecification,
    ) -> Result<()> {
        let fee = if user_id == event.maker_user_id {
            spec.maker_fee
        } else {
            spec.taker_fee
        };
        let (asset_to_subract, amount_to_subract, asset_to_add, amount_to_add) = match side {
            // For a Bid (buy), we will unlock the base currency. amount = price * size.
            Side::Bid => {
                let amount = event.price * event.size;
                let amout_to_add = event.size - (fee / 100) * event.size;
                (
                    base_asset(market_id),
                    amount,
                    quote_asset(market_id),
                    amout_to_add,
                )
            }
            // For an Ask (sell), we lock the quote currency. Amount = size.
            Side::Ask => {
                let mut amount_to_add = event.price * event.size;
                amount_to_add -= (fee / 100) * amount_to_add;
                (
                    quote_asset(market_id),
                    event.size,
                    base_asset(market_id),
                    amount_to_add,
                )
            }
        };

        // Acquire a lock on the store and perform the operation
        let mut store = self.balances.lock();
        store.subtract_locked_funds(user_id, asset_to_subract, amount_to_subract)?;
        store.add_funds(user_id, asset_to_add, amount_to_add)?;
        Ok(())
    }

    /// handle cancellations -> release funds
    pub fn handle_cancellation(&self, cmd: &OrderCommand) {
        if let Err(err) =
            self.release_funds_for_order(cmd.user_id, cmd.market_id, cmd.side, cmd.price, cmd.size)
        {
            error!(
                "[RiskEngine_{}] Failed to release funds for cancelled order {}: {:?}",
                self.shard_id, cmd.order_id, err
            );
        } else {
            info!(
                "[RiskEngine_{}] Successfully released funds for cancelled order {}",
                self.shard_id, cmd.order_id
            );
        }
    }

    /// Reserves funds for a new order.
    /// This is called by the pre-orderbook risk engine.
    pub fn reserve_funds_for_order(&self, cmd: &mut OrderCommand) -> Result<()> {
        let (asset_to_lock, amount_to_lock) = match cmd.side {
            // For a Bid (buy), we lock the base currency. Amount = price * size.
            Side::Bid => {
                let amount = self.bid_amount(cmd)?;
                (base_asset(cmd.market_id), amount)
            }
            // For an Ask (sell), we lock the quote currency. Amount = size.
            Side::Ask => (quote_asset(cmd.market_id), cmd.size),
        };

        // Acquire a lock on the store and perform the operation
        let mut store = self.balances.lock();
        store.lock_funds(cmd.user_id, asset_to_lock, amount_to_lock)?;
        Ok(())
    }

    #[inline]
    fn bid_amount(&self, cmd: &mut OrderCommand) -> Result<u64> {
        if cmd.price == u64::MAX {
            // this is a market order, will be converted to IOC Limit order
            let slippage = self.symbol_specs.get(&cmd.market_id).unwrap().slippage;
            cmd.price = (slippage as u64 / 100) * cmd.price;
            let amt = cmd
                .price
                .checked_mul(cmd.size)
                .ok_or(BalanceError::Overflow)?;
            Ok(amt)
        } else {
            let amt = cmd
                .price
                .checked_mul(cmd.size)
                .ok_or(BalanceError::Overflow)?;
            Ok(amt)
        }
    }

    /// Releases previously reserved funds from a canceled or filled order.
    /// This is called by the post-orderbook risk engine or order cancellation logic.
    pub fn release_funds_for_order(
        &self,
        user_id: u64,
        market_id: u32,
        side: Side,
        price: u64,
        size: u64,
    ) -> Result<()> {
        let (asset_to_unlock, amount_to_unlock) = match side {
            Side::Bid => {
                let amount = price.checked_mul(size).ok_or(BalanceError::Overflow)?;
                (quote_asset(market_id), amount)
            }
            Side::Ask => (base_asset(market_id), size),
        };

        // Acquire a lock on the store and perform the operation
        let mut store = self.balances.lock();
        store
            .unlock_funds(user_id, asset_to_unlock, amount_to_unlock)
            .map_err(|err| RiskEngineError::BalanceError(err))
    }

    pub fn get_balance(&self, user_id: u64, asset_id: u16) -> UserBalance {
        let store = self.balances.lock();
        store.get_balance(user_id, asset_id)
    }

    pub fn try_get_balance(&self, user_id: u64, asset_id: u16) -> Result<UserBalance> {
        let store = self.balances.lock();
        Ok(store.try_get_balance(user_id, asset_id)?)
    }

    #[cfg(test)]
    pub fn set_balance(&mut self, user_id: u64, asset_id: u16, balance: UserBalance) {
        let mut store = self.balances.lock();
        *store.get_balance_mut(user_id, asset_id) = balance;
    }
}

impl Default for RiskEngine {
    fn default() -> Self {
        Self::new(HashMap::new(), 0, 1)
    }
}

#[inline]
pub fn base_asset(market_id: u32) -> u16 {
    (market_id & 0xFFFF) as u16
}

#[inline]
pub fn quote_asset(market_id: u32) -> u16 {
    (market_id >> 16) as u16
}
