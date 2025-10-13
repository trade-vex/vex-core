use crate::error::{Result, RiskEngineError};
use common::BalanceError;
use common::BalanceStore;
use common::CoreMarketSpecification;
use common::MatcherTradeEvent;
use common::OrderCommand;
use common::OrderCommandType;
use common::PriceCache;
use common::Side;
use common::Status;
use common::UserBalance;
use common::{base_asset, quote_asset};
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
    pub fn pre_process_command(&self, cmd: &mut OrderCommand, price_cache: Arc<PriceCache>) {
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
        match cmd.command {
            OrderCommandType::PlaceOrder => {
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

                // Note: Fees are always in the receiving asset, hence are cut on post-processing (settlement)
                if let Err(err) = self.reserve_funds_for_order(cmd, price_cache) {
                    warn!(
                        "[RiskEngine] Insufficient funds for user {}: {:?}",
                        cmd.user_id, err
                    );
                    cmd.set_status(Status::Rejected);
                }
            }
            OrderCommandType::DepositFunds => {
                info!(
                    "[RiskEngine] Depositing {} units of asset {} for user {}",
                    cmd.size, cmd.market_id, cmd.user_id
                );
                match self.handle_deposit(cmd) {
                    Ok(_) => {
                        cmd.balance[0] = self.get_balance(cmd.user_id(), cmd.market_id as u16);
                        cmd.set_status(Status::Processed);
                    }
                    Err(err) => {
                        warn!(
                            "[RiskEngine] Failed to deposit funds for user {}: {:?}",
                            cmd.user_id, err
                        );
                        cmd.set_status(Status::Rejected);
                    }
                }
            }
            OrderCommandType::WithdrawFunds => {
                info!(
                    "[RiskEngine] Withdrawing {} units of asset {} for user {}",
                    cmd.size, cmd.market_id, cmd.user_id
                );
                match self.handle_withdrawal(cmd) {
                    Ok(_) => {
                        cmd.balance[0] = self.get_balance(cmd.user_id(), cmd.market_id as u16);
                        cmd.set_status(Status::Processed);
                    }
                    Err(err) => {
                        warn!(
                            "[RiskEngine] Failed to withdraw funds for user {}: {:?}",
                            cmd.user_id, err
                        );
                        cmd.set_status(Status::Rejected);
                    }
                }
            }
            _ => {} // no balance change happens in case of cancel
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
        user_side: Side,
        event: &mut MatcherTradeEvent,
        taker_cmd: Option<u64>,
    ) {
        info!(
            "[RiskEngine_{}] Processing settelement for user: {}, event: maker={:?}, price={}, size={}",
            self.shard_id, user_id, event.maker_user_id, event.price, event.size,
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

        if let Err(err) = self.settle_trade(user_id, market_id, user_side, event, spec, taker_cmd) {
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
        user_side: Side,
        event: &mut MatcherTradeEvent,
        spec: &CoreMarketSpecification,
        taker_price: Option<u64>,
    ) -> Result<()> {
        let is_maker = user_id == event.maker_user_id;
        let fee = if is_maker {
            spec.maker_fee
        } else {
            spec.taker_fee
        };

        // Acquire a lock on the store for all balance operations
        let mut store = self.balances.lock();

        // --- Price Improvement Refund Logic (for Taker only) ---
        // If the taker gets a better price than their limit, refund the difference.
        if !is_maker {
            if let Some(limit_price) = taker_price {
                // Price improvement only applies to BID orders where QUOTE currency was locked.
                if user_side == Side::Bid {
                    let execution_price = event.price;

                    if execution_price < limit_price {
                        let price_diff = limit_price - execution_price;
                        let refund_amount = price_diff * event.size;
                        // Move the saved amount from 'locked' back to 'available'.
                        store.unlock_funds(user_id, quote_asset(market_id), refund_amount)?;
                    }
                }
            }
        }

        let (asset_to_subtract, amount_to_subtract, asset_to_add, amount_to_add) = match user_side {
            // User is BUYING base asset with quote asset.
            // Spends quote, receives base. Fee is on base asset received.
            Side::Bid => {
                let quote_spent = event.price * event.size;
                // Fee is on the base asset received. Assuming fee is in basis points (e.g., 20bp = 0.2%)
                let fee_in_base = (event.size * fee) / 10000;
                let base_received_net = event.size - fee_in_base;
                (
                    quote_asset(market_id),
                    quote_spent,
                    base_asset(market_id),
                    base_received_net,
                )
            }
            // User is SELLING base asset for quote asset.
            // Spends base, receives quote. Fee is on quote asset received.
            Side::Ask => {
                let base_spent = event.size;
                let quote_received_gross = event.price * event.size;
                // Fee is on the quote asset received. Assuming fee is in basis points.
                let fee_in_quote = (quote_received_gross * fee) / 10000;
                let quote_received_net = quote_received_gross - fee_in_quote;
                (
                    base_asset(market_id),
                    base_spent,
                    quote_asset(market_id),
                    quote_received_net,
                )
            }
        };

        let balance_sub =
            store.subtract_locked_funds(user_id, asset_to_subtract, amount_to_subtract)?;
        let balance_add = store.add_funds(user_id, asset_to_add, amount_to_add)?;

        if is_maker {
            if asset_to_subtract == base_asset(market_id) {
                // Maker was on ASK side, selling base for quote.
                event.maker_balance[0] = balance_sub;
                event.maker_balance[1] = balance_add;
            } else {
                // Maker was on BID side, buying base with quote.
                event.maker_balance[0] = balance_add;
                event.maker_balance[1] = balance_sub;
            }
        }
        Ok(())
    }

    /// handle cancellations -> release funds
    pub fn handle_cancellation(&self, cmd: &mut OrderCommand) {
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
    fn reserve_funds_for_order(
        &self,
        cmd: &mut OrderCommand,
        price_cache: Arc<PriceCache>,
    ) -> Result<()> {
        let (asset_to_lock, amount_to_lock) = match cmd.side {
            // For a Bid (buy), we lock the quote currency. Amount = price * size.
            Side::Bid => {
                let amount = self.bid_amount(cmd, price_cache)?;
                (quote_asset(cmd.market_id), amount)
            }
            // For an Ask (sell), we lock the base currency. Amount = size.
            Side::Ask => (base_asset(cmd.market_id), cmd.size),
        };

        // Acquire a lock on the store and perform the operation
        let mut store = self.balances.lock();
        store.lock_funds(cmd.user_id, asset_to_lock, amount_to_lock)?;
        Ok(())
    }

    #[inline]
    fn bid_amount(&self, cmd: &mut OrderCommand, price_cache: Arc<PriceCache>) -> Result<u64> {
        if cmd.price == u64::MAX {
            // Market buy order
            let spec = self.symbol_specs.get(&cmd.market_id).ok_or(
                RiskEngineError::MarketSpecNotFound {
                    market_id: cmd.market_id,
                },
            )?;
            let slippage = spec.slippage;
            let best_ask = price_cache.get_best_ask(cmd.market_id);

            if best_ask == 0 {
                // No liquidity on ask side, cannot determine price for market order.
                return Err(RiskEngineError::InvalidArguments {
                    price: cmd.price,
                    size: cmd.size,
                });
            }

            // Assuming slippage is in basis points (e.g., 5bp = 0.05%)
            let slippage_adjustment = (best_ask as u128 * slippage as u128 / 10000) as u64;

            let conservative_price = best_ask
                .checked_add(slippage_adjustment)
                .ok_or(BalanceError::Overflow)?;

            // Persist the price used for locking on the command itself.
            // To ensure that if the order is cancelled (fully or partially),
            // the correct amount of funds can be released.
            cmd.price = conservative_price;
            cmd.size
                .checked_mul(conservative_price)
                .ok_or(BalanceError::Overflow.into())
        } else {
            // Limit order
            cmd.price
                .checked_mul(cmd.size)
                .ok_or(BalanceError::Overflow.into())
        }
    }

    /// Releases previously reserved funds from a canceled or filled order.
    /// This is called by the post-orderbook risk engine or order cancellation logic.
    fn release_funds_for_order(
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
            .map_err(RiskEngineError::BalanceError)
    }

    pub fn get_balance(&self, user_id: u64, asset_id: u16) -> UserBalance {
        let store = self.balances.lock();
        store.get_balance(user_id, asset_id)
    }

    pub fn try_get_balance(&self, user_id: u64, asset_id: u16) -> Result<UserBalance> {
        let store = self.balances.lock();
        Ok(store.try_get_balance(user_id, asset_id)?)
    }

    pub fn set_balance(&self, user_id: u64, asset_id: u16, balance: UserBalance) {
        let mut store = self.balances.lock();
        *store.get_balance_mut(user_id, asset_id) = balance;
    }

    /// Handles deposit funds command
    /// The market_id field is used to represent the asset_id for deposits
    fn handle_deposit(&self, cmd: &mut OrderCommand) -> Result<()> {
        let asset_id = cmd.market_id as u16;
        let amount = cmd.size;

        info!(
            "[RiskEngine] Processing deposit: user={}, asset={}, amount={}",
            cmd.user_id, asset_id, amount
        );

        let mut store = self.balances.lock();
        store.add_funds(cmd.user_id, asset_id, amount)?;

        info!(
            "[RiskEngine] Successfully deposited {} units of asset {} for user {}",
            amount, asset_id, cmd.user_id
        );

        Ok(())
    }

    /// Handles withdrawal funds command
    /// The market_id field is used to represent the asset_id for withdrawals
    fn handle_withdrawal(&self, cmd: &mut OrderCommand) -> Result<()> {
        let asset_id = cmd.market_id as u16;
        let amount = cmd.size;

        info!(
            "[RiskEngine] Processing withdrawal: user={}, asset={}, amount={}",
            cmd.user_id, asset_id, amount
        );

        let mut store = self.balances.lock();
        store.subtract_funds(cmd.user_id, asset_id, amount)?;

        info!(
            "[RiskEngine] Successfully withdrew {} units of asset {} for user {}",
            amount, asset_id, cmd.user_id
        );

        Ok(())
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
    use common::{MarketType, TimeInForce};

    fn get_spec(market_id: u32) -> CoreMarketSpecification {
        CoreMarketSpecification::builder()
            .market_id(market_id)
            .market_type(MarketType::Spot)
            .maker_fee(10) // 0.1%
            .taker_fee(20) // 0.2%
            .slippage(5)
            .build()
            .unwrap()
    }

    #[test]
    #[should_panic]
    fn test_new_risk_engine_panics_with_non_power_of_two_shards() {
        RiskEngine::new(HashMap::new(), 0, 3);
    }

    #[test]
    fn test_user_id_sharding() {
        let engine_shard0 = RiskEngine::new(HashMap::new(), 0, 4);
        let engine_shard1 = RiskEngine::new(HashMap::new(), 1, 4);

        assert!(engine_shard0.user_id_for_this_handler(0));
        assert!(engine_shard0.user_id_for_this_handler(4));
        assert!(!engine_shard0.user_id_for_this_handler(1));

        assert!(engine_shard1.user_id_for_this_handler(1));
        assert!(engine_shard1.user_id_for_this_handler(5));
        assert!(!engine_shard1.user_id_for_this_handler(0));
        let symbol_spec = HashMap::new();
        let price_cache = Arc::new(PriceCache::new(symbol_spec.keys()));
        let mut cmd = OrderCommand::new(TimeInForce::Gtc, 1, 1, 100, 10, Side::Bid, 1);

        // shard 0 should not process user 1's command, will be skipped
        engine_shard0.pre_process_command(&mut cmd, price_cache.clone());
        assert_eq!(cmd.status, Status::Processing);

        // shard 1 should process user 1's command
        // it will be rejected because no market spec
        engine_shard1.pre_process_command(&mut cmd, price_cache);
        assert_eq!(cmd.status, Status::Rejected);
    }

    #[test]
    fn test_balance_management() {
        let engine = RiskEngine::default();
        let user_id = 1;
        let asset_id = 1;
        let initial_balance = UserBalance::new(1000, 0);

        engine.set_balance(user_id, asset_id, initial_balance);

        let balance = engine.get_balance(user_id, asset_id);
        assert_eq!(balance.available(), 1000);
        assert_eq!(balance.locked(), 0);
        assert_eq!(balance.total(), 1000);

        let balance_res = engine.try_get_balance(user_id, asset_id).unwrap();
        assert_eq!(balance_res, initial_balance);

        let res = engine.try_get_balance(user_id, 2); // unknown asset
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err(),
            RiskEngineError::from(BalanceError::UserAssetNotFound(user_id, 2))
        );
    }

    #[test]
    fn test_reserve_and_cancel_bid() {
        let engine = RiskEngine::default();
        let user_id = 1;
        let market_id = (2_u32 << 16) | 1_u32; // base=1 (e.g. BTC), quote=2 (e.g. USD)
        let quote_asset = quote_asset(market_id);
        let price = 100;
        let size = 10;
        let required_quote = price * size;

        engine.set_balance(user_id, quote_asset, UserBalance::new(required_quote, 0));

        let mut cmd = OrderCommand::new(
            TimeInForce::Gtc,
            1,
            user_id,
            price,
            size,
            Side::Bid,
            market_id,
        );

        let spec = get_spec(market_id);
        let mut specs = HashMap::new();
        specs.insert(market_id, spec);

        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        engine
            .reserve_funds_for_order(&mut cmd, price_cache)
            .unwrap();

        let balance = engine.get_balance(user_id, quote_asset);
        assert_eq!(balance.available(), 0);
        assert_eq!(balance.locked(), required_quote);
        // Cancel the order and check if funds are released
        engine.handle_cancellation(&mut cmd);

        let balance = engine.get_balance(user_id, quote_asset);
        assert_eq!(balance.available(), required_quote);
        assert_eq!(balance.locked(), 0);
    }

    #[test]
    fn test_reserve_and_cancel_ask() {
        let engine = RiskEngine::default();
        let user_id = 1;
        let market_id = (2_u32 << 16) | 1_u32; // base=1 (e.g. BTC), quote=2 (e.g. USD)
        let base_asset = base_asset(market_id);
        let price = 100;
        let size = 10;

        engine.set_balance(user_id, base_asset, UserBalance::new(size, 0));

        let mut cmd = OrderCommand::new(
            TimeInForce::Gtc,
            1,
            user_id,
            price,
            size,
            Side::Ask,
            market_id,
        );

        let spec = get_spec(market_id);
        let mut specs = HashMap::new();
        specs.insert(market_id, spec);

        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        engine
            .reserve_funds_for_order(&mut cmd, price_cache)
            .unwrap();

        let balance = engine.get_balance(user_id, base_asset);
        assert_eq!(balance.available(), 0);
        assert_eq!(balance.locked(), size);
        // Cancel the order and check if funds are released
        engine.handle_cancellation(&mut cmd);

        let balance = engine.get_balance(user_id, base_asset);
        assert_eq!(balance.available(), size);
        assert_eq!(balance.locked(), 0);
    }

    #[test]
    fn test_insufficient_funds() {
        let engine = RiskEngine::default();
        let user_id = 1;
        let market_id = (2_u32 << 16) | 1_u32;
        let quote_asset = quote_asset(market_id);
        let price = 100;
        let size = 10;
        let required_quote = price * size;

        engine.set_balance(
            user_id,
            quote_asset,
            UserBalance::new(required_quote - 1, 0),
        );

        let mut cmd = OrderCommand::new(
            TimeInForce::Gtc,
            1,
            user_id,
            price,
            size,
            Side::Bid,
            market_id,
        );

        let spec = get_spec(market_id);
        let mut specs = HashMap::new();
        specs.insert(market_id, spec);
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let res = engine.reserve_funds_for_order(&mut cmd, price_cache);
        assert!(res.is_err());
        match res.unwrap_err() {
            RiskEngineError::BalanceError(BalanceError::InsufficientAvailableFunds { .. }) => (),
            e => panic!("Unexpected error: {e:?}"),
        }
    }

    #[test]
    fn test_trade_settlement() {
        // Market: BTC/USD -> base=BTC(2), quote=USD(1)
        let btc_asset_id = 2u16;
        let usd_asset_id = 1u16;
        let market_id = ((usd_asset_id as u32) << 16) | (btc_asset_id as u32);

        let mut specs = HashMap::new();
        specs.insert(market_id, get_spec(market_id));
        let price_cache = Arc::new(PriceCache::new(specs.keys()));

        let engine = RiskEngine::new(specs, 0, 1);

        let maker_id = 101; // Sells BTC for USD (ASK)
        let taker_id = 102; // Buys BTC with USD (BID)

        let price = 50_000;
        let size = 20_000; // amount of BTC

        // --- Initial Balances ---
        // Taker (buyer) needs USD (quote) to buy BTC (base).
        let taker_initial_quote = price * size;
        // Maker (seller) needs BTC (base) to sell for USD (quote).
        let maker_initial_base = size;

        let mut balances = engine.balances.lock();
        *balances.get_balance_mut(taker_id, usd_asset_id) =
            UserBalance::new(taker_initial_quote, 0);
        *balances.get_balance_mut(maker_id, btc_asset_id) = UserBalance::new(maker_initial_base, 0);
        drop(balances);

        // --- Reserve funds ---
        // Taker places BID order to buy BTC
        let mut taker_cmd = OrderCommand::new(
            TimeInForce::Gtc,
            1,
            taker_id,
            price,
            size,
            Side::Bid,
            market_id,
        );
        engine
            .reserve_funds_for_order(&mut taker_cmd, price_cache.clone())
            .unwrap();

        // Maker places ASK order to sell BTC
        let mut maker_cmd = OrderCommand::new(
            TimeInForce::Gtc,
            2,
            maker_id,
            price,
            size,
            Side::Ask,
            market_id,
        );
        engine
            .reserve_funds_for_order(&mut maker_cmd, price_cache.clone())
            .unwrap();

        // --- A trade occurs ---
        let mut trade_event = MatcherTradeEvent {
            price, // price in quote asset (USD) per base asset (BTC)
            size,  // size in base asset (BTC)
            maker_user_id: maker_id,
            active_order_completed: false,
            matched_order_id: 2,
            matched_order_completed: true,
            next_event: None,
            maker_balance: [UserBalance::default(); 2],
        };

        // Settle for Taker (Buyer, Bid side) - buys base (BTC) with quote (USD)
        engine.handle_trade_event(
            taker_id,
            market_id,
            Side::Bid,
            &mut trade_event,
            Some(price),
        );

        // Settle for Maker (Seller, Ask side) - sells base (BTC) for quote (USD)
        engine.handle_trade_event(maker_id, market_id, Side::Ask, &mut trade_event, None);

        // --- Final Balances ---
        // Taker (buyer): Spends `price * size` of USD (quote). Receives `size` of BTC (base), minus taker fee (20bp on base).
        let taker_fee_in_base = (size * 20) / 10000;
        let net_base_received = size - taker_fee_in_base;
        assert_eq!(engine.get_balance(taker_id, usd_asset_id).total(), 0);
        assert_eq!(
            engine.get_balance(taker_id, btc_asset_id).total(),
            net_base_received
        );

        // Maker (seller): Spends `size` of BTC (base). Receives `price * size` of USD (quote), minus maker fee (10bp on quote).
        let gross_quote_received = price * size;
        let maker_fee_in_quote = (gross_quote_received * 10) / 10000;
        let net_quote_received = gross_quote_received - maker_fee_in_quote;
        assert_eq!(
            engine.get_balance(maker_id, usd_asset_id).total(),
            net_quote_received
        );
        assert_eq!(engine.get_balance(maker_id, btc_asset_id).total(), 0);
    }

    #[test]
    fn test_market_order_reservation() {
        // --- Setup ---
        // Market: BTC/USD -> base=BTC(2), quote=USD(1)
        let btc_asset_id = 2u16;
        let usd_asset_id = 1u16;
        let market_id = ((usd_asset_id as u32) << 16) | (btc_asset_id as u32);

        let mut specs = HashMap::new();
        specs.insert(
            market_id,
            get_spec(market_id), // slippage is 5bp
        );
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 0, 1);

        let user_id = 1;
        let size = 10; // size in base asset (BTC)

        // --- Test 1: Market Buy with no liquidity ---
        let mut market_buy_cmd = OrderCommand::new(
            TimeInForce::Gtc,
            1,
            user_id,
            u64::MAX,
            size,
            Side::Bid,
            market_id,
        );

        let res = engine.reserve_funds_for_order(&mut market_buy_cmd, price_cache.clone());
        assert!(res.is_err());
        match res.unwrap_err() {
            RiskEngineError::InvalidArguments { .. } => (),
            e => panic!("Expected InvalidArguments, got {e:?}"),
        }

        // --- Test 2: Market Buy with liquidity ---
        let best_ask = 50000;
        price_cache.update_prices(market_id, 49990, best_ask);

        // Slippage is 5bp. Conservative price = 50000 + (50000 * 5 / 10000) = 50000 + 25 = 50025
        let conservative_price = 50025;
        let required_quote = conservative_price * size;
        engine.set_balance(user_id, usd_asset_id, UserBalance::new(required_quote, 0));

        engine
            .reserve_funds_for_order(&mut market_buy_cmd, price_cache.clone())
            .unwrap();

        let balance = engine.get_balance(user_id, usd_asset_id);
        assert_eq!(balance.available(), 0);
        assert_eq!(balance.locked(), required_quote);

        // --- Test 3: Market Buy with insufficient funds ---
        engine.set_balance(
            user_id,
            usd_asset_id,
            UserBalance::new(required_quote - 1, 0),
        );
        let res = engine.reserve_funds_for_order(&mut market_buy_cmd, price_cache.clone());
        assert!(res.is_err());
        match res.unwrap_err() {
            RiskEngineError::BalanceError(BalanceError::InsufficientAvailableFunds { .. }) => (),
            e => panic!("Expected InsufficientAvailableFunds, got {e:?}"),
        }

        // --- Test 4: Market Sell ---
        // Market sell doesn't depend on price cache, just locks `size` of base asset (BTC).
        engine.set_balance(user_id, btc_asset_id, UserBalance::new(size, 0));
        let mut market_sell_cmd =
            OrderCommand::new(TimeInForce::Gtc, 2, user_id, 0, size, Side::Ask, market_id);

        engine
            .reserve_funds_for_order(&mut market_sell_cmd, price_cache.clone())
            .unwrap();
        let balance = engine.get_balance(user_id, btc_asset_id);
        assert_eq!(balance.available(), 0);
        assert_eq!(balance.locked(), size);
    }

    #[test]
    fn test_parallel_markets_and_settlement() {
        // This test simulates a user trading on two different markets concurrently.
        // It verifies that funds are locked, settled, and cancelled correctly across markets.
        // Market 1: BTC/USD -> base=BTC(2), quote=USD(1)
        // Market 2: ETH/USD -> base=ETH(3), quote=USD(1)
        let usd_asset_id = 1u16;
        let btc_asset_id = 2u16;
        let eth_asset_id = 3u16;

        let market_id_btc_usd = ((usd_asset_id as u32) << 16) | (btc_asset_id as u32);
        let market_id_eth_usd = ((usd_asset_id as u32) << 16) | (eth_asset_id as u32);

        let mut specs = HashMap::new();
        specs.insert(market_id_btc_usd, get_spec(market_id_btc_usd));
        specs.insert(market_id_eth_usd, get_spec(market_id_eth_usd));
        let price_cache = Arc::new(PriceCache::new(specs.keys()));
        let engine = RiskEngine::new(specs, 0, 1);

        let user_id = 100;

        // --- Initial Balances ---
        // User has 100,000,000 USD, 10,000 BTC, 50,000 ETH
        let mut balances = engine.balances.lock();
        *balances.get_balance_mut(user_id, usd_asset_id) = UserBalance::new(100_000_000, 0);
        *balances.get_balance_mut(user_id, btc_asset_id) = UserBalance::new(10_000, 0);
        *balances.get_balance_mut(user_id, eth_asset_id) = UserBalance::new(50_000, 0);
        drop(balances);

        // --- Action 1: User places a BID order on BTC/USD market ---
        // Buy 1,000 BTC (base) for 50,000 USD (quote) each. Total cost: 50,000,000 USD
        let btc_price = 50_000;
        let btc_size = 1_000;
        let mut btc_buy_cmd = OrderCommand::new(
            TimeInForce::Gtc,
            1,
            user_id,
            btc_price,
            btc_size,
            Side::Bid,
            market_id_btc_usd,
        );
        engine
            .reserve_funds_for_order(&mut btc_buy_cmd, price_cache.clone())
            .unwrap();

        // --- Check balances after BTC order reservation ---
        assert_eq!(
            engine.get_balance(user_id, usd_asset_id).available(),
            50_000_000
        ); // 100M - 50M
        assert_eq!(
            engine.get_balance(user_id, usd_asset_id).locked(),
            50_000_000
        );
        assert_eq!(engine.get_balance(user_id, btc_asset_id).total(), 10_000); // unchanged
        assert_eq!(engine.get_balance(user_id, eth_asset_id).total(), 50_000); // unchanged

        // --- Action 2: User places an ASK order on ETH/USD market ---
        // Sell 2,000 ETH (base) for 3,000 USD (quote) each.
        let eth_price = 3_000;
        let eth_size = 2_000;
        let mut eth_sell_cmd = OrderCommand::new(
            TimeInForce::Gtc,
            2,
            user_id,
            eth_price,
            eth_size,
            Side::Ask,
            market_id_eth_usd,
        );
        engine
            .reserve_funds_for_order(&mut eth_sell_cmd, price_cache.clone())
            .unwrap();

        // --- Check balances after both reservations ---
        assert_eq!(
            engine.get_balance(user_id, usd_asset_id).total(),
            100_000_000
        );
        assert_eq!(
            engine.get_balance(user_id, usd_asset_id).locked(),
            50_000_000
        );
        assert_eq!(engine.get_balance(user_id, btc_asset_id).total(), 10_000);
        assert_eq!(
            engine.get_balance(user_id, eth_asset_id).available(),
            48_000
        ); // 50k - 2k
        assert_eq!(engine.get_balance(user_id, eth_asset_id).locked(), 2_000);

        // --- Action 3: The BTC buy order is filled (as taker) ---
        let mut btc_trade_event = MatcherTradeEvent {
            price: btc_price,
            size: btc_size,
            maker_user_id: 200, // some other user
            active_order_completed: true,
            matched_order_id: 99,
            matched_order_completed: true,
            next_event: None,
            maker_balance: [UserBalance::default(); 2],
        };
        engine.handle_trade_event(
            user_id,
            market_id_btc_usd,
            Side::Bid,
            &mut btc_trade_event,
            Some(btc_price),
        );

        // --- Check balances after BTC trade settlement ---
        // User (taker) spent 50,000,000 USD, received 1,000 BTC (minus 0.2% taker fee)
        let btc_taker_fee = (btc_size * 20) / 10000; // 0.2% of 1000 = 2
        let net_btc_received = btc_size - btc_taker_fee;

        assert_eq!(
            engine.get_balance(user_id, usd_asset_id).total(),
            50_000_000
        ); // 100M - 50M
        assert_eq!(engine.get_balance(user_id, usd_asset_id).locked(), 0);
        assert_eq!(
            engine.get_balance(user_id, btc_asset_id).total(),
            10_000 + net_btc_received
        ); // 10k + 998
        assert_eq!(engine.get_balance(user_id, eth_asset_id).total(), 50_000); // ETH balance is untouched by BTC trade
        assert_eq!(engine.get_balance(user_id, eth_asset_id).locked(), 2_000); // ETH lock is untouched

        // --- Action 4: The ETH sell order is cancelled ---
        engine.handle_cancellation(&mut eth_sell_cmd);

        // --- Final Balances ---
        assert_eq!(
            engine.get_balance(user_id, usd_asset_id).total(),
            50_000_000
        );
        assert_eq!(engine.get_balance(user_id, btc_asset_id).total(), 10_998);
        assert_eq!(engine.get_balance(user_id, eth_asset_id).total(), 50_000);
        assert_eq!(
            engine.get_balance(user_id, eth_asset_id).available(),
            50_000
        ); // Funds unlocked
        assert_eq!(engine.get_balance(user_id, eth_asset_id).locked(), 0);
    }
}
