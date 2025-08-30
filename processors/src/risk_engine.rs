use common::cmd::MatcherTradeEvent;
use common::cmd::OrderCommand;
use common::OrderCommandType;
use common::MatcherEventType;
use common::Side;
use common::model::market_specification::CoreMarketSpecification;
use common::model::user_profile::BalanceStore;
use hashbrown::HashMap;
use tracing::{info, warn};
use crate::error::{Result, RiskEngineError};

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
        let user_profile = self
            .user_balances
            .get_mut(&cmd.user_id)
            .ok_or(RiskEngineError::UserNotFound { user_id: cmd.user_id })?;

        // check 2: Validate the command arguments.
        info!(
            "[RiskEngine] Validating arguments for order {}",
            cmd.order_id
        );
        if matches!(
            cmd.command,
            OrderCommandType::PlaceOrder 
        ) {
            if cmd.size <= 0 || cmd.price <= 0 {
                return Err(RiskEngineError::InvalidArguments { 
                    price: cmd.price, 
                    size: cmd.size 
                });
            }
            info!(
                "[RiskEngine] Looking up market_id spec for market_id {}",
                cmd.market_id
            );
            let spec = self
                .symbol_specs
                .get(&cmd.market_id)
                .ok_or(RiskEngineError::MarketSpecNotFound { market_id: cmd.market_id })?;

            info!(
                "[RiskEngine] Found market_id spec: {:?} for market_id {}",
                spec, cmd.market_id
            );
            let required_funds = if cmd.side == Side::Bid {
                cmd.price * cmd.size
            } else {
                cmd.size
            };

            let amount = cmd.price * cmd.size;

            if let Err(balance_error) = user_profile.lock_funds(cmd.user_id, cmd.market_id, amount) {
                warn!(
                    "[RiskEngine] Insufficient funds for user {} to place order {}: {:?}",
                    cmd.user_id, cmd.order_id, balance_error
                );
                return Err(RiskEngineError::InsufficientFunds { 
                    user_id: cmd.user_id, 
                    required: required_funds, 
                    available: 0 // TODO: Get actual available balance
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
    pub fn handle_event(&mut self, event: &MatcherTradeEvent, market_id: u32, taker_side: Side) {
        // Process only if the event is for a user managed by this shard
        if !self.user_id_for_this_handler(event.maker_user_id) {
            return; // Not for this shard, skip
        }

        info!("[RiskEngine_{}] Handling event: {:?}", self.shard_id, event);

        // Determine maker side (opposite of taker side)
        let maker_side = if taker_side == Side::Bid { Side::Ask } else { Side::Bid };
        
        if let Some(spec) = self.symbol_specs.get(&market_id) {
            if let Some(maker_profile) = self.user_balances.get_mut(&event.maker_user_id) {
                // For trade events, we need to settle the trade
                // The maker's locked funds should be consumed
                let trade_amount = event.price * event.size;
                
                if let Err(e) = maker_profile.consume_locked(event.maker_user_id, market_id, trade_amount) {
                    warn!("[RiskEngine_{}] Failed to consume locked funds for maker {}: {:?}", 
                          self.shard_id, event.maker_user_id, e);
                }
            }
        }
        
        // Process next event in chain if it exists
        let mut current_event = event.next_event.as_ref();
        while let Some(next_event) = current_event {
            if self.user_id_for_this_handler(next_event.maker_user_id) {
                if let Some(spec) = self.symbol_specs.get(&market_id) {
                    if let Some(maker_profile) = self.user_balances.get_mut(&next_event.maker_user_id) {
                        let trade_amount = next_event.price * next_event.size;
                        
                        if let Err(e) = maker_profile.consume_locked(next_event.maker_user_id, market_id, trade_amount) {
                            warn!("[RiskEngine_{}] Failed to consume locked funds for maker {}: {:?}", 
                                  self.shard_id, next_event.maker_user_id, e);
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
