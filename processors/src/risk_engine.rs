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

/// Manages all user profiles and performs risk checks as well as settlements
/// This is the Rust equivalent of `RiskEngine.java`.
pub struct RiskEngine {
    pub user_profiles: HashMap<u64, UserProfile>,
    pub symbol_specs: HashMap<u32, CoreSymbolSpecification>,
    // Sharding configuration
    shard_id: u32,
    shard_mask: u64,
}

impl RiskEngine {
    pub fn new(
        symbol_specs: HashMap<u32, CoreSymbolSpecification>,
        shard_id: u32,
        num_shards: u32,
    ) -> Self {
        if num_shards.count_ones() != 1 {
            panic!("Number of shards must be a power of 2");
        }
        Self {
            user_profiles: HashMap::new(),
            symbol_specs,
            shard_id,
            shard_mask: (num_shards - 1) as u64,
        }
    }

    /// Checks if a user ID is handled by this risk engine instance.
    fn user_id_for_this_handler(&self, user_id: u64) -> bool {
        (user_id & self.shard_mask) == self.shard_id as u64
    }

    /// Pre-processes a command to validate it(DONE) and hold funds(TODOs).
    /// This is the first stage(excali-5a, excali-5b) of processing for any command that can affect a user.
    pub fn pre_process_command(&mut self, cmd: &OrderCommand) -> Result<(), OrderBookError> {
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
            OrderCommandType::PlaceLimitOrder | OrderCommandType::PlaceMarketOrder
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
        Self::new(HashMap::new(), 0, 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::model::enums::{OrderType, SymbolType};
    use common::model::user_profile::UserStatus;

    fn create_test_symbol_spec(symbol_id: u32) -> CoreSymbolSpecification {
        CoreSymbolSpecification {
            symbol_id,
            symbol_type: SymbolType::CurrencyExchangePair,
            base_currency: 1,   // BTC
            quote_currency: 2,  // USD
            base_scale_k: 1,
            quote_scale_k: 1,
            taker_fee: 0,
            maker_fee: 0,
            margin_buy: 0,
            margin_sell: 0,
        }
    }

    fn create_test_user_profile(user_id: u64, status: UserStatus) -> UserProfile {
        let mut profile = UserProfile::new(user_id, status);
        // Add some initial funds
        profile.accounts.insert(1, 1000); // 1000 BTC
        profile.accounts.insert(2, 50000); // 50000 USD
        profile
    }

    fn create_test_order_command(
        user_id: u64,
        symbol_id: u32,
        price: u64,
        size: u64,
        side: Side,
        command_type: OrderCommandType,
    ) -> OrderCommand {
        OrderCommand {
            command: command_type,
            order_id: 12345,
            symbol_id,
            user_id,
            price,
            reserve_bid_price: 0,
            size,
            side,
            order_type: OrderType::Gtc,
            timestamp: 0,
            matcher_event: None,
        }
    }

    #[test]
    fn test_new_risk_engine() {
        let mut symbol_specs = HashMap::new();
        symbol_specs.insert(1, create_test_symbol_spec(1));
        
        let risk_engine = RiskEngine::new(symbol_specs.clone(), 0, 4);
        
        assert_eq!(risk_engine.shard_id, 0);
        assert_eq!(risk_engine.shard_mask, 3); // 4-1 = 3
        assert_eq!(risk_engine.symbol_specs.len(), 1);
        assert!(risk_engine.user_profiles.is_empty());
    }

    #[test]
    #[should_panic(expected = "Number of shards must be a power of 2")]
    fn test_new_risk_engine_invalid_shards() {
        let symbol_specs = HashMap::new();
        RiskEngine::new(symbol_specs, 0, 3); // 3 is not a power of 2
    }

    #[test]
    fn test_user_id_for_this_handler() {
        let risk_engine = RiskEngine::new(HashMap::new(), 1, 4);
        
        // User ID 5 should be handled by shard 1 (5 & 3 = 1)
        assert!(risk_engine.user_id_for_this_handler(5));
        
        // User ID 6 should not be handled by shard 1 (6 & 3 = 2)
        assert!(!risk_engine.user_id_for_this_handler(6));
        
        // User ID 1 should be handled by shard 1 (1 & 3 = 1)
        assert!(risk_engine.user_id_for_this_handler(1));
    }

    #[test]
    fn test_pre_process_command_user_not_found() {
        let mut risk_engine = RiskEngine::new(HashMap::new(), 0, 1);
        let mut cmd = create_test_order_command(1, 1, 100, 10, Side::Bid, OrderCommandType::PlaceLimitOrder);
        
        let result = risk_engine.pre_process_command(&mut cmd);
        // TODO : Error should be user not found , look at this later
        assert!(matches!(result, Err(OrderBookError::UnsupportedCommand)));
    }

    #[test]
    fn test_pre_process_command_user_suspended() {
        let mut risk_engine = RiskEngine::new(HashMap::new(), 0, 1);
        let user_profile = create_test_user_profile(1, UserStatus::Suspended);
        risk_engine.user_profiles.insert(1, user_profile);
        
        let mut cmd = create_test_order_command(1, 1, 100, 10, Side::Bid, OrderCommandType::PlaceLimitOrder);
        
        let result = risk_engine.pre_process_command(&mut cmd);
        // TODO : Error should be User Suspended
        assert!(matches!(result, Err(OrderBookError::UnsupportedCommand)));
    }

    #[test]
    fn test_pre_process_command_invalid_arguments() {
        let mut risk_engine = RiskEngine::new(HashMap::new(), 0, 1);
        let user_profile = create_test_user_profile(1, UserStatus::Active);
        risk_engine.user_profiles.insert(1, user_profile);
        
        // Test with zero price
        let mut cmd = create_test_order_command(1, 1, 0, 10, Side::Bid, OrderCommandType::PlaceLimitOrder);
        let result = risk_engine.pre_process_command(&mut cmd);
        assert!(matches!(result, Err(OrderBookError::InvalidArguments)));
        
        // Test with zero size
        let mut cmd = create_test_order_command(1, 1, 100, 0, Side::Bid, OrderCommandType::PlaceLimitOrder);
        let result = risk_engine.pre_process_command(&mut cmd);
        assert!(matches!(result, Err(OrderBookError::InvalidArguments)));
    }

    #[test]
    fn test_pre_process_command_symbol_not_found() {
        let mut risk_engine = RiskEngine::new(HashMap::new(), 0, 1);
        let user_profile = create_test_user_profile(1, UserStatus::Active);
        risk_engine.user_profiles.insert(1, user_profile);
        
        let mut cmd = create_test_order_command(1, 999, 100, 10, Side::Bid, OrderCommandType::PlaceLimitOrder);
        
        let result = risk_engine.pre_process_command(&mut cmd);
        assert!(matches!(result, Err(OrderBookError::UnsupportedCommand)));
    }

    #[test]
    fn test_pre_process_command_insufficient_funds_bid() {
        let mut risk_engine = RiskEngine::new(HashMap::new(), 0, 1);
        let user_profile = create_test_user_profile(1, UserStatus::Active);
        risk_engine.user_profiles.insert(1, user_profile);
        
        let symbol_spec = create_test_symbol_spec(1);
        risk_engine.symbol_specs.insert(1, symbol_spec);
        
        // Try to buy with insufficient USD (price * size = 1000 * 100 = 100000, but only have 50000)
        let mut cmd = create_test_order_command(1, 1, 1000, 100, Side::Bid, OrderCommandType::PlaceLimitOrder);
        
        let result = risk_engine.pre_process_command(&mut cmd);
        assert!(matches!(result, Err(OrderBookError::InsufficientFunds)));
    }

    #[test]
    fn test_pre_process_command_insufficient_funds_ask() {
        let mut risk_engine = RiskEngine::new(HashMap::new(), 0, 1);
        let user_profile = create_test_user_profile(1, UserStatus::Active);
        risk_engine.user_profiles.insert(1, user_profile);
        
        let symbol_spec = create_test_symbol_spec(1);
        risk_engine.symbol_specs.insert(1, symbol_spec);
        
        // Try to sell with insufficient BTC (size = 2000, but only have 1000)
        let mut cmd = create_test_order_command(1, 1, 100, 2000, Side::Ask, OrderCommandType::PlaceLimitOrder);
        
        let result = risk_engine.pre_process_command(&mut cmd);
        assert!(matches!(result, Err(OrderBookError::InsufficientFunds)));
    }

    #[test]
    fn test_pre_process_command_successful_bid() {
        let mut risk_engine = RiskEngine::new(HashMap::new(), 0, 1);
        let user_profile = create_test_user_profile(1, UserStatus::Active);
        risk_engine.user_profiles.insert(1, user_profile);
        
        let symbol_spec = create_test_symbol_spec(1);
        risk_engine.symbol_specs.insert(1, symbol_spec);
        
        // Valid bid order
        let mut cmd = create_test_order_command(1, 1, 100, 10, Side::Bid, OrderCommandType::PlaceLimitOrder);
        

        
        let result = risk_engine.pre_process_command(&mut cmd);
        assert!(result.is_ok());
        
        // Check that funds were held
        let user_profile = risk_engine.user_profiles.get(&1).unwrap();
        let actual_balance = user_profile.accounts.get(&2).unwrap();
        assert_eq!(actual_balance, &49000); // 50000 - (100 * 10) = 50000 - 1000
    }

    #[test]
    fn test_pre_process_command_successful_ask() {
        let mut risk_engine = RiskEngine::new(HashMap::new(), 0, 1);
        let user_profile = create_test_user_profile(1, UserStatus::Active);
        risk_engine.user_profiles.insert(1, user_profile);
        
        let symbol_spec = create_test_symbol_spec(1);
        risk_engine.symbol_specs.insert(1, symbol_spec);
        
        // Valid ask order
        let mut cmd = create_test_order_command(1, 1, 100, 10, Side::Ask, OrderCommandType::PlaceLimitOrder);
        
        let result = risk_engine.pre_process_command(&mut cmd);
        assert!(result.is_ok());
        
        // Check that funds were held
        let user_profile = risk_engine.user_profiles.get(&1).unwrap();
        assert_eq!(user_profile.accounts.get(&1).unwrap(), &990); // 1000 - 10
    }

    #[test]
    fn test_pre_process_command_cancel_order() {
        let mut risk_engine = RiskEngine::new(HashMap::new(), 0, 1);
        let user_profile = create_test_user_profile(1, UserStatus::Active);
        risk_engine.user_profiles.insert(1, user_profile);
        
        // Cancel order should pass validation (no fund holding required)
        let mut cmd = create_test_order_command(1, 1, 0, 0, Side::Ask, OrderCommandType::CancelOrder);
        
        let result = risk_engine.pre_process_command(&mut cmd);
        assert!(result.is_ok());
    }

    #[test]
    fn test_pre_process_command_wrong_shard() {
        let mut risk_engine = RiskEngine::new(HashMap::new(), 1, 4);
        let user_profile = create_test_user_profile(2, UserStatus::Active); // User 2 goes to shard 2
        risk_engine.user_profiles.insert(2, user_profile);
        
        let mut cmd = create_test_order_command(2, 1, 100, 10, Side::Bid, OrderCommandType::PlaceLimitOrder);
        
        // Should be skipped (not for this shard)
        let result = risk_engine.pre_process_command(&mut cmd);
        assert!(result.is_ok());
    }

    #[test]
    fn test_handle_event_trade() {
        let mut risk_engine = RiskEngine::new(HashMap::new(), 0, 1);
        
        // Create user profiles for both maker and taker
        let maker_profile = create_test_user_profile(1, UserStatus::Active);
        let taker_profile = create_test_user_profile(2, UserStatus::Active);
        risk_engine.user_profiles.insert(1, maker_profile);
        risk_engine.user_profiles.insert(2, taker_profile);
        
        let symbol_spec = create_test_symbol_spec(1);
        risk_engine.symbol_specs.insert(1, symbol_spec);
        
        // Create a trade event
        let event = MatcherTradeEvent {
            event_type: MatcherEventType::Trade,
            section: 0,
            symbol_id: 1,
            active_order_user_id: 2, // taker
            taker_action: Side::Bid,
            active_order_completed: false,
            matched_order_id: 123,
            maker_user_id: 1, // maker
            matched_order_completed: false,
            price: 100,
            size: 10,
            bidder_hold_price: 0,
            taker_fee: 0,
            maker_fee: 0,
            next_event: None,
        };
        
        risk_engine.handle_event(&event);
        
        // Check that balances were updated
        let maker_profile = risk_engine.user_profiles.get(&1).unwrap();
        let taker_profile = risk_engine.user_profiles.get(&2).unwrap();
        
        // Maker (seller) should have received quote currency
        assert_eq!(maker_profile.accounts.get(&2).unwrap(), &51000); // 50000 + (100 * 10)
        // Taker (buyer) should have received base currency
        assert_eq!(taker_profile.accounts.get(&1).unwrap(), &1010); // 1000 + 10
    }

    #[test]
    fn test_handle_event_cancel() {
        let mut risk_engine = RiskEngine::new(HashMap::new(), 0, 1);
        let user_profile = create_test_user_profile(1, UserStatus::Active);
        risk_engine.user_profiles.insert(1, user_profile);
        
        let symbol_spec = create_test_symbol_spec(1);
        risk_engine.symbol_specs.insert(1, symbol_spec);
        
        // First place an order to hold funds
        let mut cmd = create_test_order_command(1, 1, 100, 10, Side::Bid, OrderCommandType::PlaceLimitOrder);
        let result = risk_engine.pre_process_command(&mut cmd);
        assert!(result.is_ok());
        
        // Verify funds were held
        let balance_after_hold = *risk_engine.user_profiles.get(&1).unwrap().accounts.get(&2).unwrap();
        assert_eq!(balance_after_hold, 49000); // 50000 - (100 * 10)
        
        // Create a cancel event to release the funds
        let event = MatcherTradeEvent {
            event_type: MatcherEventType::Cancel,
            section: 0,
            symbol_id: 1,
            active_order_user_id: 1,
            taker_action: Side::Bid,
            active_order_completed: true,
            matched_order_id: 0,
            maker_user_id: 0,
            matched_order_completed: false,
            price: 100,
            size: 10,
            bidder_hold_price: 0,
            taker_fee: 0,
            maker_fee: 0,
            next_event: None,
        };
        
        risk_engine.handle_event(&event);
        
        // Check that funds were released back to the account
        let balance_after_cancel = *risk_engine.user_profiles.get(&1).unwrap().accounts.get(&2).unwrap();
        assert_eq!(balance_after_cancel, 50000); // Funds should be restored
    }

    #[test]
    fn test_handle_event_reduce() {
        let mut risk_engine = RiskEngine::new(HashMap::new(), 0, 1);
        let user_profile = create_test_user_profile(1, UserStatus::Active);
        risk_engine.user_profiles.insert(1, user_profile);
        
        let symbol_spec = create_test_symbol_spec(1);
        risk_engine.symbol_specs.insert(1, symbol_spec);
        
        // First place an order to hold funds
        let mut cmd = create_test_order_command(1, 1, 100, 20, Side::Bid, OrderCommandType::PlaceLimitOrder);
        let result = risk_engine.pre_process_command(&mut cmd);
        assert!(result.is_ok());
        
        // Verify funds were held
        let balance_after_hold = *risk_engine.user_profiles.get(&1).unwrap().accounts.get(&2).unwrap();
        assert_eq!(balance_after_hold, 48000); // 50000 - (100 * 20)
        
        // Create a reduce event to partially release funds (reduce by 5)
        let event = MatcherTradeEvent {
            event_type: MatcherEventType::Reduce,
            section: 0,
            symbol_id: 1,
            active_order_user_id: 1,
            taker_action: Side::Bid,
            active_order_completed: false,
            matched_order_id: 0,
            maker_user_id: 0,
            matched_order_completed: false,
            price: 100,
            size: 5, // Reduce by 5
            bidder_hold_price: 0,
            taker_fee: 0,
            maker_fee: 0,
            next_event: None,
        };
        
        risk_engine.handle_event(&event);
        
        // Check that partial funds were released back to the account
        let balance_after_reduce = *risk_engine.user_profiles.get(&1).unwrap().accounts.get(&2).unwrap();
        assert_eq!(balance_after_reduce, 48500); // 48000 + (100 * 5)
    }

    #[test]
    fn test_handle_event_wrong_shard() {
        let mut risk_engine = RiskEngine::new(HashMap::new(), 1, 4);
        let user_profile = create_test_user_profile(2, UserStatus::Active); // User 2 goes to shard 2
        risk_engine.user_profiles.insert(2, user_profile);
        
        let event = MatcherTradeEvent {
            event_type: MatcherEventType::Trade,
            section: 0,
            symbol_id: 1,
            active_order_user_id: 2,
            taker_action: Side::Bid,
            active_order_completed: false,
            matched_order_id: 123,
            maker_user_id: 3, // User 3 also goes to shard 3
            matched_order_completed: false,
            price: 100,
            size: 10,
            bidder_hold_price: 0,
            taker_fee: 0,
            maker_fee: 0,
            next_event: None,
        };
        
        // Should be skipped (not for this shard)
        risk_engine.handle_event(&event);
        
        // Verify no changes were made
        let user_profile = risk_engine.user_profiles.get(&2).unwrap();
        assert_eq!(user_profile.accounts.get(&1).unwrap(), &1000); // Unchanged
        assert_eq!(user_profile.accounts.get(&2).unwrap(), &50000); // Unchanged
    }

    #[test]
    fn test_default_risk_engine() {
        let risk_engine = RiskEngine::default();
        assert_eq!(risk_engine.shard_id, 0);
        assert_eq!(risk_engine.shard_mask, 0);
        assert!(risk_engine.symbol_specs.is_empty());
        assert!(risk_engine.user_profiles.is_empty());
    }
}
