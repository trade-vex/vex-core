use crate::error::{Result, RiskEngineError};
use common::BalanceStore;
use common::CoreMarketSpecification;
use common::MatcherTradeEvent;
use common::OrderCommand;
use common::OrderCommandType;
use common::Side;
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

    /// Pre-processes a command to validate it(DONE) and hold funds(TODOs)
    pub fn pre_process_command(&mut self, cmd: &OrderCommand) -> Result<()> {
        // Process only if the command is for a user managed by this shard
        if !self.user_id_for_this_handler(cmd.user_id) {
            return Ok(()); // Not for this shard, skip
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

        // check 2: Validate the command arguments.
        info!(
            "[RiskEngine] Validating arguments for order {}",
            cmd.order_id
        );
        if matches!(cmd.command, OrderCommandType::PlaceOrder) {
            if cmd.size <= 0 || cmd.price <= 0 {
                return Err(RiskEngineError::InvalidArguments {
                    price: cmd.price,
                    size: cmd.size,
                });
            }
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

            let amount = required_funds;

            if let Err(balance_error) = user_profile.lock_funds(cmd.user_id, cmd.market_id, amount)
            {
                warn!(
                    "[RiskEngine] Insufficient funds for user {} to place order {}: {:?}",
                    cmd.user_id, cmd.order_id, balance_error
                );

                // Get actual available balance for error reporting
                let available_balance = user_profile
                    .get_balance(cmd.user_id, cmd.market_id)
                    .map(|balance| balance.available())
                    .unwrap_or(0);

                return Err(RiskEngineError::InsufficientFunds {
                    user_id: cmd.user_id,
                    required: required_funds,
                    available: available_balance,
                });
            }
        }

        info!(
            "[RiskEngine] Pre-processing and approving command for user {}",
            cmd.user_id
        );
        Ok(())
    }

    /// Handles events coming from the matching engine to settle funds
    pub fn handle_event(
        &mut self,
        event: &MatcherTradeEvent,
        market_id: u32,
        taker_side: Side,
        taker_id: u64,
    ) {
        info!(
            "[RiskEngine_{}] Processing trade event: price={}, size={}, maker={}, taker={}",
            self.shard_id, event.price, event.size, event.maker_user_id, taker_id
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

        // Process maker settlement (consume locked funds)
        if self.user_id_for_this_handler(event.maker_user_id) {
            if let Some(maker_profile) = self.user_balances.get_mut(&event.maker_user_id) {
                let trade_amount = event.price * event.size;

                match maker_profile.consume_locked(event.maker_user_id, market_id, trade_amount) {
                    Ok(()) => {
                        info!(
                            "[RiskEngine_{}] Successfully consumed locked funds for maker {}: amount={}",
                            self.shard_id, event.maker_user_id, trade_amount
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
        }

        // Process taker settlement (unlock remaining funds and apply fees)
        if self.user_id_for_this_handler(taker_id) {
            if let Some(taker_profile) = self.user_balances.get_mut(&taker_id) {
                let total_trade_amount = event.price * event.size;
                let taker_fee = spec.taker_fee * event.size;
                let total_required = total_trade_amount + taker_fee;

                // Unlock the excess funds that were locked but not used
                if let Err(e) = taker_profile.unlock_funds(taker_id, market_id, total_required) {
                    warn!(
                        "[RiskEngine_{}] Failed to unlock excess funds for taker {}: {:?}",
                        self.shard_id, taker_id, e
                    );
                } else {
                    info!(
                        "[RiskEngine_{}] Successfully unlocked excess funds for taker {}: amount={}",
                        self.shard_id, taker_id, total_required
                    );
                }
            }
        }

        // Process chained events if they exist
        let mut current_event = event.next_event.as_ref();
        while let Some(next_event) = current_event {
            info!(
                "[RiskEngine_{}] Processing chained event: price={}, size={}, maker={}",
                self.shard_id, next_event.price, next_event.size, next_event.maker_user_id
            );

            // Process maker settlement for chained event
            if self.user_id_for_this_handler(next_event.maker_user_id) {
                if let Some(maker_profile) = self.user_balances.get_mut(&next_event.maker_user_id) {
                    let trade_amount = next_event.price * next_event.size;

                    match maker_profile.consume_locked(
                        next_event.maker_user_id,
                        market_id,
                        trade_amount,
                    ) {
                        Ok(()) => {
                            info!(
                                "[RiskEngine_{}] Successfully consumed locked funds for chained maker {}: amount={}",
                                self.shard_id, next_event.maker_user_id, trade_amount
                            );
                        }
                        Err(e) => {
                            warn!(
                                "[RiskEngine_{}] Failed to consume locked funds for chained maker {}: {:?}",
                                self.shard_id, next_event.maker_user_id, e
                            );
                        }
                    }
                }
            }

            current_event = next_event.next_event.as_ref();
        }
    }
}

impl Default for RiskEngine {
    fn default() -> Self {
        Self::new(HashMap::new(), 0, 1)
    }
}
