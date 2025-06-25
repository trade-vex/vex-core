use common::model::user_profile::UserProfile;
use common::cmd::OrderCommand;
use common::cmd::MatcherTradeEvent;
use orderbook::OrderBookError;
use hashbrown::HashMap;


/// Manages all user profiles and performs risk checks as well as settlements
/// This is the Rust equivalent of `RiskEngine.java`.
pub struct RiskEngine {
    pub user_profiles: HashMap<i64, UserProfile>,
}

impl RiskEngine {
    pub fn new() -> Self {
        Self { user_profiles: HashMap::new() }
    }

    /// Pre-processes a command to validate it(DONE) and hold funds(TODOs). 
    /// This is the first stage(excali-5a, excali-5b) of processing for any command that can affect a user.
    pub fn pre_process_command(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        let _user_profile = self.user_profiles.get_mut(&cmd.uid)
            .ok_or(OrderBookError::UnsupportedCommand)?;
        
        // check 1:Check `user_profile.user_status`.
        if _user_profile.user_status != common::model::user_profile::UserStatus::Active {
            return Err(OrderBookError::UnsupportedCommand);
        }
        // check 2: Validate the command arguments.
        if cmd.size <= 0 || cmd.price <= 0 {
            return Err(OrderBookError::InvalidArguments);
        }
        // TODOs
        // 3. Check if the user's account balance is sufficient.
        // 4. If so, debit the user's account to put funds on hold.
        println!("[RiskEngine] Pre-processing and approving command for user {}", cmd.uid);
        Ok(())
    }

    /// Handles events coming from the matching engine to settle funds. 
    /// This is a final stage(excali-8a) in the pipeline for events that have financial impact.
    pub fn handle_event(&mut self, event: &MatcherTradeEvent) {
        
        // TODOs
        // 1. Look at `event.event_type`.
        // 2. If it's a TRADE, find the buyer and seller profiles and settle funds.
        // 3. If it's a REDUCE/CANCEL, find the user and release the held funds.
        println!("[RiskEngine] Handling settlement for event: {:?}", event.event_type);
    }
}