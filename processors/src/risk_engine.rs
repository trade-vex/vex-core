use crate::error::{Result, RiskEngineError};
use common::BalanceError;
use common::BalanceStore;
use common::CoreMarketSpecification;
use common::OrderCommand;
use common::OrderCommandType;
use common::Side;
use common::UserBalance;
use hashbrown::HashMap;
use parking_lot::Mutex;
use std::sync::Arc;
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
    pub fn pre_process_command(&mut self, cmd: &OrderCommand) -> Result<()> {
        // Process only if the command is for a user managed by this shard
        if !self.user_id_for_this_handler(cmd.user_id) {
            return Ok(()); // Not for this shard, skip
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
            let spec = self.symbol_specs.get(&cmd.market_id).ok_or(
                RiskEngineError::MarketSpecNotFound {
                    market_id: cmd.market_id,
                },
            )?;

            info!(
                "[RiskEngine] Found market_id spec: {:?} for market_id {}",
                spec, cmd.market_id
            );

            // TODO: Handle Market Order
            // Note: The Fee's are always in receiving asset, hense are cut on post processing
            if let Err(balance_error) = self.reserve_funds_for_order(
                cmd.user_id,
                cmd.market_id,
                cmd.side,
                cmd.price,
                cmd.size,
            ) {
                // Log detailed balance error
                warn!(
                    "[RiskEngine] Insufficient funds for user {} to place order {}: {:?}",
                    cmd.user_id, cmd.order_id, balance_error
                );

                return Err(balance_error);
            }
        }

        info!(
            "[RiskEngine] Pre-processing and approving command for user {}",
            cmd.user_id
        );
        Ok(())
    }

    /// Handles a single trade event from the matching engine to settle funds
    /// This is called by the R2 handler for each individual event in the linked list
    pub fn handle_event(&mut self, cmd: &OrderCommand) {
        info!(
            "[RiskEngine_{}] Processing command: status={:?}, order_id={}, user_id={}, market_id={}",
            self.shard_id,
            cmd.status(),
            cmd.order_id(),
            cmd.user_id(),
            cmd.market_id()
        );

        // Get market specification for fee calculations
        let _spec = match self.symbol_specs.get(&cmd.market_id()) {
            Some(spec) => spec,
            None => {
                warn!(
                    "[RiskEngine_{}] Market spec not found for market_id {}",
                    self.shard_id,
                    cmd.market_id()
                );
                return;
            }
        };

        // Process maker settlement (consume locked funds and apply maker fee)
        // if self.user_id_for_this_handler(event.maker_user_id) {
        //     if let Some(maker_profile) = self.user_balances.get_mut(&event.maker_user_id) {
        //         let trade_amount = event.price * event.size;
        //         let maker_fee = spec.maker_fee * event.size;
        //         let total_maker_amount = trade_amount + maker_fee;

        //         // Consume the locked funds for the maker (they pay the trade amount + maker fees)
        //         match maker_profile.consume_locked(
        //             event.maker_user_id,
        //             market_id,
        //             total_maker_amount,
        //         ) {
        //             Ok(()) => {
        //                 info!(
        //                     "[RiskEngine_{}] Successfully consumed locked funds for maker {}: amount={}",
        //                     self.shard_id, event.maker_user_id, total_maker_amount
        //                 );
        //             }
        //             Err(e) => {
        //                 warn!(
        //                     "[RiskEngine_{}] Failed to consume locked funds for maker {}: {:?}",
        //                     self.shard_id, event.maker_user_id, e
        //                 );
        //             }
        //         }
        //     }
        // }

        // Process taker settlement (consume locked funds for the trade)
        // if self.user_id_for_this_handler(taker_id) {
        //     if let Some(taker_profile) = self.user_balances.get_mut(&taker_id) {
        //         let total_trade_amount = event.price * event.size;
        //         let taker_fee = spec.taker_fee * event.size;
        //         let total_required = total_trade_amount + taker_fee;

        //         // Consume the locked funds for the taker (they pay the full amount + fees)
        //         match taker_profile.consume_locked(taker_id, market_id, total_required) {
        //             Ok(()) => {
        //                 info!(
        //                     "[RiskEngine_{}] Successfully consumed locked funds for taker {}: amount={}",
        //                     self.shard_id, taker_id, total_required
        //                 );
        //             }
        //             Err(e) => {
        //                 warn!(
        //                     "[RiskEngine_{}] Failed to consume locked funds for taker {}: {:?}",
        //                     self.shard_id, taker_id, e
        //                 );
        //             }
        //         }
        //     }
        // }
    }

    /// Reserves funds for a new order.
    /// This is called by the pre-orderbook risk engine.
    ///
    /// # Arguments
    /// * `user_id` - The ID of the user placing the order.
    /// * `market_id` - The market ID, containing quote and base asset IDs.
    /// * `side` - `Bid` (buy) or `Ask` (sell).
    /// * `price` - The price of the order.
    /// * `size` - The amount of the base asset to be traded.
    pub fn reserve_funds_for_order(
        &self,
        user_id: u64,
        market_id: u32,
        side: Side,
        price: u64,
        size: u64,
    ) -> Result<()> {
        let (asset_to_lock, amount_to_lock) = match side {
            // For a Bid (buy), we lock the quote currency. Amount = price * size.
            Side::Bid => {
                let amount = price.checked_mul(size).ok_or(BalanceError::Overflow)?;
                (quote_asset(market_id), amount)
            }
            // For an Ask (sell), we lock the base currency. Amount = size.
            Side::Ask => (base_asset(market_id), size),
        };

        // Acquire a lock on the store and perform the operation
        let mut store = self.balances.lock();
        store.lock_funds(user_id, asset_to_lock, amount_to_lock)?;
        Ok(())
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
