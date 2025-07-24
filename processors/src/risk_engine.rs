use common::cmd::MatcherTradeEvent;
use common::cmd::OrderCommand;
use common::model::user_profile::UserProfile;
use hashbrown::HashMap;
use orderbook::OrderBookError;
use tracing::{info, warn};
use common::model::enums::OrderAction;
use common::model::symbol_specification::CoreSymbolSpecification;
use common::model::enums::MatcherEventType;
use common::cmd::OrderCommandType;
/// Manages all user profiles and performs risk checks as well as settlements
/// This is the Rust equivalent of `RiskEngine.java`.
pub struct RiskEngine {
    pub user_profiles: HashMap<i64, UserProfile>,
    pub symbol_specs: HashMap<i32, CoreSymbolSpecification>,
}

impl RiskEngine {
    pub fn new(symbol_specs: HashMap<i32, CoreSymbolSpecification>) -> Self {
        Self {
            user_profiles: HashMap::new(),
            symbol_specs,
        }
    }

    /// Pre-processes a command to validate it(DONE) and hold funds(TODOs).
    /// This is the first stage(excali-5a, excali-5b) of processing for any command that can affect a user.
    pub fn pre_process_command(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        info!("[RiskEngine] Pre-processing command: {:?}", cmd);
        let user_profile = self
            .user_profiles
            .get_mut(&cmd.uid)
            .ok_or(OrderBookError::UnsupportedCommand)?;

        // check 1:Check `user_profile.user_status`.
        info!(
            "[RiskEngine] Checking user status for user {}: {:?}",
            cmd.uid, user_profile.user_status
        );
        if user_profile.user_status != common::model::user_profile::UserStatus::Active {
            return Err(OrderBookError::UnsupportedCommand);
        }
        // check 2: Validate the command arguments.
        info!(
            "[RiskEngine] Validating arguments for order {}",
            cmd.order_id
        );
        if matches!(cmd.command, OrderCommandType::PlaceOrder | OrderCommandType::ReduceOrder) {
            if cmd.size <= 0 || cmd.price <= 0 {
                return Err(OrderBookError::InvalidArguments);
            }
            info!(
                "[RiskEngine] Looking up symbol spec for symbol {}",
                cmd.symbol
            );
            let spec = self
                .symbol_specs
                .get(&cmd.symbol)
                .ok_or(OrderBookError::UnsupportedCommand)?;

            info!(
                "[RiskEngine] Found symbol spec: {:?} for symbol {}",
                spec, cmd.symbol
            );
            let required_funds = if cmd.action == OrderAction::Bid {
                cmd.price * cmd.size
            } else {
                cmd.size
            };

            if !user_profile.hold_funds(spec, required_funds, cmd.action) {
                warn!(
                    "[RiskEngine] Insufficient funds for user {} to place order {}",
                    cmd.uid, cmd.order_id
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
            cmd.uid
        );
        Ok(())
    }

    /// Handles events coming from the matching engine to settle funds.
    /// This is a final stage(excali-8a) in the pipeline for events that have financial impact.
    pub fn handle_event(&mut self, event: &MatcherTradeEvent) {
        match event.event_type {
            MatcherEventType::Trade => {
                let spec = self.symbol_specs.get(&event.symbol).unwrap();

                if let Some(maker_profile) = self.user_profiles.get_mut(&event.maker_uid) {
                    maker_profile.settle_trade(
                        spec,
                        event.price,
                        event.size,
                        if event.taker_action == OrderAction::Ask {
                            OrderAction::Bid
                        } else {
                            OrderAction::Ask
                        },
                    );
                }
                if let Some(taker_profile) = self.user_profiles.get_mut(&event.active_order_uid) {
                    taker_profile.settle_trade(
                        spec,
                        event.price,
                        event.size,
                        event.taker_action,
                    );
                }
            }
            MatcherEventType::Reduce | MatcherEventType::Cancel => {
                if let Some(user_profile) = self.user_profiles.get_mut(&event.active_order_uid) {
                    let released_amount = if event.taker_action == OrderAction::Bid {
                        event.price * event.size
                    } else {
                        event.size
                    };
                    user_profile.release_funds(
                        event.symbol,
                        released_amount,
                        event.taker_action,
                    );
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
        Self::new(HashMap::new())
    }
}