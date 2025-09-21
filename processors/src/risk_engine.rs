use crate::error::{Result, RiskEngineError};
use common::BalanceStore;
use common::CoreMarketSpecification;
use common::MatcherTradeEvent;
use common::OrderCommand;
use common::OrderCommandType;
use common::Side;
use common::Status;
use common::UserBalance;
use hashbrown::HashMap;
use tracing::{info, warn};

/// Manages all user profiles and performs risk checks as well as settlements
pub struct RiskEngine {
    pub user_balances: HashMap<u64, BalanceStore>,
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
            user_balances: HashMap::new(),
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
    pub fn pre_process_command(&mut self, cmd: &mut OrderCommand) {
        // Process only if the command is for a user managed by this shard
        if !self.user_id_for_this_handler(cmd.user_id) {
            return; // Not for this shard, skip
        }

        info!(
            "[RiskEngine_{}] Pre-processing command: {:?}",
            self.shard_id, cmd
        );
        let user_profile =
            self.user_balances
                .get_mut(&cmd.user_id)
                .ok_or(RiskEngineError::UserNotFound {
                    user_id: cmd.user_id,
                })?;

        // Validate the command arguments
        info!(
            "[RiskEngine] Validating arguments for order {}",
            cmd.order_id
        );
        if matches!(cmd.command, OrderCommandType::PlaceOrder) {
            if cmd.size == 0 || cmd.price == 0 {
                return Err(RiskEngineError::InvalidArguments {
                    price: cmd.price,
                    size: cmd.size,
                });
            }
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
            // Calculate required funds based on order side and market specification
            let required_funds = if cmd.side == Side::Bid {
                // For BID orders: need to lock the total cost (price * size) plus taker fee
                let base_amount = cmd.price * cmd.size;
                let taker_fee = spec.taker_fee * cmd.size;
                base_amount + taker_fee
            } else {
                // For ASK orders: need to lock the size (quantity being sold)
                cmd.size
            };

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
    pub fn handle_event(&mut self, cmd: &OrderCommand) {
        info!(
            "[RiskEngine_{}] Processing single trade event: price={}, size={}, maker={}, taker={}",
            self.shard_id, event.price, event.size, event.maker_user_id, taker_id
        );

        // Get market specification for fee calculations
        let _spec = match self.symbol_specs.get(&cmd.market_id()) {
            Some(spec) => spec,
            None => {
                warn!(
                    "[RiskEngine_{}] Market spec not found for market_id {}",
                    self.shard_id, market_id
                );
                return;
            }
        };

        // Process maker settlement (consume locked funds and apply maker fee)
        if self.user_id_for_this_handler(event.maker_user_id)
            && let Some(maker_profile) = self.user_balances.get_mut(&event.maker_user_id)
        {
            let trade_amount = event.price * event.size;
            let maker_fee = spec.maker_fee * event.size;
            let total_maker_amount = trade_amount + maker_fee;

            // Consume the locked funds for the maker (they pay the trade amount + maker fees)
            match maker_profile.consume_locked(event.maker_user_id, market_id, total_maker_amount) {
                Ok(()) => {
                    info!(
                        "[RiskEngine_{}] Successfully consumed locked funds for maker {}: amount={}",
                        self.shard_id, event.maker_user_id, total_maker_amount
                    );
                }
                Err(e) => {
                    warn!(
                        "[RiskEngine_{}] Failed to consume locked funds for maker {}: {:?}",
                        self.shard_id, event.maker_user_id, e
                    );
                }
            }
        }

        // Process taker settlement (consume locked funds for the trade)
        if self.user_id_for_this_handler(taker_id)
            && let Some(taker_profile) = self.user_balances.get_mut(&taker_id)
        {
            let total_trade_amount = event.price * event.size;
            let taker_fee = spec.taker_fee * event.size;
            let total_required = total_trade_amount + taker_fee;

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
