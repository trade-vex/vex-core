use common::cmd::MatcherTradeEvent;
use common::cmd::OrderCommand;
use common::model::user_profile::UserProfile;
use hashbrown::HashMap;
use orderbook::OrderBookError;
use tracing::{info, warn};
use common::model::enums::OrderAction;
use common::model::symbol_specification::CoreSymbolSpecification;
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
        let user_profile = self
            .user_profiles
            .get_mut(&cmd.uid)
            .ok_or(OrderBookError::UnsupportedCommand)?;

        // check 1:Check `user_profile.user_status`.
        if user_profile.user_status != common::model::user_profile::UserStatus::Active {
            return Err(OrderBookError::UnsupportedCommand);
        }
        // check 2: Validate the command arguments.
        if cmd.size <= 0 || cmd.price <= 0 {
            return Err(OrderBookError::InvalidArguments);
        }

        let spec = self
            .symbol_specs
            .get(&cmd.symbol)
            .ok_or(OrderBookError::UnsupportedCommand)?;

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
        info!(
            "[RiskEngine] Pre-processing and approving command for user {}",
            cmd.uid
        );
        Ok(())
    }

    /// Handles events coming from the matching engine to settle funds.
    /// This is a final stage(excali-8a) in the pipeline for events that have financial impact.
    pub fn handle_event(&mut self, event: &MatcherTradeEvent) {
        // TODOs
        // 1. Look at `event.event_type`.
        // 2. If it's a TRADE, find the buyer and seller profiles and settle funds.
        // 3. If it's a REDUCE/CANCEL, find the user and release the held funds.
        info!(
            "[RiskEngine] Handling settlement for event: {:?}",
            event.event_type
        );
        
    }
}

impl Default for RiskEngine {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}