use common::cmd::MatcherTradeEvent;
use common::cmd::OrderCommand;
use common::cmd::OrderCommandType;
use common::model::enums::MatcherEventType;
use common::model::enums::Side;
use common::model::symbol_specification::CoreSymbolSpecification;
use common::model::user_profile::UserProfile;
use hashbrown::HashMap;
use orderbook::OrderBookError;
use tracing::{info, warn};

pub struct RiskEngine {
    #[allow(dead_code)]
    user_balances: BalanceStore,
}

impl RiskEngine {
    pub fn new() -> Self {
        Self {
            user_profiles: HashMap::new(),
            symbol_specs,
            shard_id,
            shard_mask: (num_shards - 1) as i64,
        }
    }

    /// Checks if a user ID is handled by this risk engine instance.
    fn user_id_for_this_handler(&self, user_id: i64) -> bool {
        (user_id & self.shard_mask) == self.shard_id as i64
    }

    /// Pre-processes a command to validate it(DONE) and hold funds(TODOs).
    /// This is the first stage(excali-5a, excali-5b) of processing for any command that can affect a user.
    pub fn pre_process_command(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        // Process only if the command is for a user managed by this shard
        if !self.user_id_for_this_handler(cmd.user_id) {
            return Ok(()); // Not for this shard, skip
        }

        info!(
            "[RiskEngine_{}] Pre-processing command: {:?}",
            self.shard_id, cmd
        );
        let user_profile = self
            .user_profiles
            .get_mut(&cmd.user_id)
            .ok_or(OrderBookError::UnsupportedCommand)?;

        // check 1:Check `user_profile.user_status`.
        info!(
            "[RiskEngine] Checking user status for user {}: {:?}",
            cmd.user_id, user_profile.user_status
        );
        if user_profile.user_status != common::model::user_profile::UserStatus::Active {
            return Err(OrderBookError::UnsupportedCommand);
        }
        // check 2: Validate the command arguments.
        info!(
            "[RiskEngine] Validating arguments for order {}",
            cmd.order_id
        );
        if matches!(
            cmd.command,
            OrderCommandType::PlaceOrder | OrderCommandType::ReduceOrder
        ) {
            if cmd.size <= 0 || cmd.price <= 0 {
                return Err(OrderBookError::InvalidArguments);
            }
            info!(
                "[RiskEngine] Looking up symbol_id spec for symbol_id {}",
                cmd.symbol_id
            );
            let spec = self
                .symbol_specs
                .get(&cmd.symbol_id)
                .ok_or(OrderBookError::UnsupportedCommand)?;

            info!(
                "[RiskEngine] Found symbol_id spec: {:?} for symbol_id {}",
                spec, cmd.symbol_id
            );
            let required_funds = if cmd.side == Side::Bid {
                cmd.price * cmd.size
            } else {
                cmd.size
            };

            if !user_profile.hold_funds(spec, required_funds, cmd.side) {
                warn!(
                    "[RiskEngine] Insufficient funds for user {} to place order {}",
                    cmd.user_id, cmd.order_id
                );
                return Err(OrderBookError::InsufficientFunds);
            }
        } else if matches!(cmd.command, OrderCommandType::MoveOrder) {
            if cmd.price <= 0 {
                return Err(OrderBookError::InvalidArguments);
            }
            // Do NOT call hold_funds for MoveOrder!
        }

        info!(
            "[RiskEngine] Pre-processing and approving command for user {}",
            cmd.user_id
        );
        Ok(())
    }

    /// Handles events coming from the matching engine to settle funds.
    /// This is a final stage(excali-8a) in the pipeline for events that have financial impact.
    pub fn handle_event(&mut self, event: &MatcherTradeEvent) {
        // Process only if the event is for a user managed by this shard
        if !self.user_id_for_this_handler(event.active_order_user_id)
            && !self.user_id_for_this_handler(event.maker_user_id)
        {
            return; // Not for this shard, skip
        }

        info!("[RiskEngine_{}] Handling event: {:?}", self.shard_id, event);

        match event.event_type {
            MatcherEventType::Trade => {
                let spec = self.symbol_specs.get(&event.symbol_id).unwrap();

                if let Some(maker_profile) = self.user_profiles.get_mut(&event.maker_user_id) {
                    maker_profile.settle_trade(
                        spec,
                        event.price,
                        event.size,
                        if event.taker_action == Side::Ask {
                            Side::Bid
                        } else {
                            Side::Ask
                        },
                    );
                }
                if let Some(taker_profile) = self.user_profiles.get_mut(&event.active_order_user_id) {
                    taker_profile.settle_trade(spec, event.price, event.size, event.taker_action);
                }
            }
            MatcherEventType::Reduce | MatcherEventType::Cancel => {
                if let Some(user_profile) = self.user_profiles.get_mut(&event.active_order_user_id) {
                    let released_amount = if event.taker_action == Side::Bid {
                        event.price * event.size
                    } else {
                        event.size
                    };
                    user_profile.release_funds(event.symbol_id, released_amount, event.taker_action);
                }
            }
            MatcherEventType::OrderPlaced => {
                // Do nothing
            }
            _ => {
                // Other event types like Reject or BinaryEvent might not have financial impact
            }
        }
    }
}

impl Default for RiskEngine {
    fn default() -> Self {
        Self::new()
    }
}
