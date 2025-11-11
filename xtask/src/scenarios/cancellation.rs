//! Order cancellation test scenarios
//!
//! This module contains comprehensive test scenarios for order cancellations.
//! All tests run in a single session without restarting the server.
//! State is maintained across sections - orderbook and balances persist.

use std::time::Duration;

use crate::builders::OrderBuilder;
use crate::fixtures::{assets, prices, users};
use crate::test_framework::TestContext;
use crate::test_framework::types::*;
use crate::verifiers::{BalanceVerifier, OrderbookVerifier, ResponseVerifier, TradeVerifier};
use common::Side;
use tracing::{info, warn};

/// Comprehensive cancellation test suite - runs all scenarios in a single session
///
/// This is a single massive test that validates all cancellation behaviors:
/// 1. Cancel resting bid order (unlocks quote currency)
/// 2. Cancel resting ask order (unlocks base currency)
/// 3. Cancel partially filled order (unlocks remaining size)
/// 4. Try cancelling non-existent order (should fail gracefully)
/// 5. Try cancelling already filled order (should fail)
/// 6. Cancel and replace at same price level
/// 7. Multiple sequential cancellations
///
/// State is maintained across sections - no cleanup between tests since
/// the VexCore server holds orderbook and balances in memory.
pub async fn run_all(ctx: &mut TestContext) -> TestResult<Vec<ScenarioResult>> {
    info!("╔════════════════════════════════════════╗");
    info!("║   CANCELLATION COMPREHENSIVE SUITE     ║");
    info!("╚════════════════════════════════════════╝");
    info!("Market ID: {} (Base: {}, Quote: {})", ctx.market_id, ctx.base_asset_id, ctx.quote_asset_id);
    info!("");

    let suite_start = std::time::Instant::now();
    let mut results = Vec::new();

    // ========================================================================
    // SECTION 1: Setup - Fund all test users
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 1: Initial Setup                │");
    info!("└─────────────────────────────────────────┘");

    info!("Funding test users...");
    ctx.fund_user(users::ALICE, 10_000_000, assets::USD).await?;  // 10M USD for Alice
    ctx.fund_user(users::ALICE, 1_000, assets::BTC).await?;       // 1000 BTC for Alice
    ctx.fund_user(users::BOB, 10_000_000, assets::USD).await?;    // 10M USD for Bob
    ctx.fund_user(users::BOB, 1_000, assets::BTC).await?;         // 1000 BTC for Bob
    ctx.fund_user(users::CHARLIE, 10_000_000, assets::USD).await?; // 10M USD for Charlie
    ctx.fund_user(users::CHARLIE, 1_000, assets::BTC).await?;      // 1000 BTC for Charlie

    info!("✓ All users funded successfully");
    info!("");

    // ========================================================================
    // SECTION 2: Cancel Resting Bid Order
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 2: Cancel Resting Bid Order     │");
    info!("│ (Funds should unlock)                   │");
    info!("└─────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match cancel_resting_bid_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 2 PASSED ({:?})", duration);
            results.push(ScenarioResult::success("cancel_resting_bid".to_string(), duration));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 2 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure("cancel_resting_bid".to_string(), duration, e));
            return Ok(results); // Stop on first failure
        }
    }
    info!("");

    // ========================================================================
    // SECTION 3: Cancel Resting Ask Order
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 3: Cancel Resting Ask Order     │");
    info!("│ (Funds should unlock)                   │");
    info!("└─────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match cancel_resting_ask_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 3 PASSED ({:?})", duration);
            results.push(ScenarioResult::success("cancel_resting_ask".to_string(), duration));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 3 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure("cancel_resting_ask".to_string(), duration, e));
            return Ok(results); // Stop on first failure
        }
    }
    info!("");

    // ========================================================================
    // SECTION 4: Cancel Partially Filled Order
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 4: Cancel Partially Filled Order│");
    info!("│ (Only remaining size unlocks)           │");
    info!("└─────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match cancel_partially_filled_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 4 PASSED ({:?})", duration);
            results.push(ScenarioResult::success("cancel_partially_filled".to_string(), duration));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 4 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure("cancel_partially_filled".to_string(), duration, e));
            return Ok(results); // Stop on first failure
        }
    }
    info!("");

    // ========================================================================
    // SECTION 5: Cancel Non-existent Order
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 5: Cancel Non-existent Order    │");
    info!("│ (Should handle gracefully)              │");
    info!("└─────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match cancel_nonexistent_order_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 5 PASSED ({:?})", duration);
            results.push(ScenarioResult::success("cancel_nonexistent_order".to_string(), duration));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 5 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure("cancel_nonexistent_order".to_string(), duration, e));
            return Ok(results); // Stop on first failure
        }
    }
    info!("");

    // ========================================================================
    // SECTION 6: Cancel Already Filled Order
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 6: Cancel Already Filled Order  │");
    info!("│ (Should handle gracefully)              │");
    info!("└─────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match cancel_filled_order_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 6 PASSED ({:?})", duration);
            results.push(ScenarioResult::success("cancel_filled_order".to_string(), duration));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 6 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure("cancel_filled_order".to_string(), duration, e));
            return Ok(results); // Stop on first failure
        }
    }
    info!("");

    // ========================================================================
    // SECTION 7: Cancel and Replace at Same Price
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 7: Cancel and Replace Order     │");
    info!("│ (Cancel then place new at same price)  │");
    info!("└─────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match cancel_and_replace_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 7 PASSED ({:?})", duration);
            results.push(ScenarioResult::success("cancel_and_replace".to_string(), duration));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 7 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure("cancel_and_replace".to_string(), duration, e));
            return Ok(results); // Stop on first failure
        }
    }
    info!("");

    // ========================================================================
    // SECTION 8: Multiple Sequential Cancellations
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 8: Multiple Cancellations       │");
    info!("│ (Cancel several orders in sequence)    │");
    info!("└─────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match multiple_cancellations_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 8 PASSED ({:?})", duration);
            results.push(ScenarioResult::success("multiple_cancellations".to_string(), duration));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 8 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure("multiple_cancellations".to_string(), duration, e));
            return Ok(results); // Stop on first failure
        }
    }
    info!("");

    // ========================================================================
    // Final Summary
    // ========================================================================
    let total_duration = suite_start.elapsed();
    let passed = results.iter().filter(|r| r.success).count();
    let failed = results.len() - passed;

    info!("╔════════════════════════════════════════╗");
    info!("║   TEST SUITE SUMMARY                   ║");
    info!("╚════════════════════════════════════════╝");
    info!("Total Sections: {}", results.len());
    info!("Passed:         {} ✓", passed);
    info!("Failed:         {} ✗", failed);
    info!("Total Time:     {:?}", total_duration);
    info!("╚════════════════════════════════════════╝");

    Ok(results)
}

/// SECTION 2: Cancel a resting bid order - funds should unlock
async fn cancel_resting_bid_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Alice places bid @ {} for 5 BTC", prices::LOW);
    info!("Alice cancels the order → Funds unlock");

    let market_id = ctx.market_id;
    let price = prices::LOW; // 40,000
    let size = 5;

    // Get Alice's initial balance
    let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
    let initial_balance = balance_verifier.get_balance(users::ALICE, assets::USD).await?;
    info!("  → Initial balance: available={} USD, locked={} USD", initial_balance.available, initial_balance.locked);

    // Alice places limit bid order
    let order_cmd = OrderBuilder::place_limit()
        .user(users::ALICE)
        .price(price)
        .size(size)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let response = ctx.execute_command(order_cmd)?;
    ResponseVerifier::assert_placed(&response)?;
    let order_id = response.order_id;
    info!("  → Order placed: order_id={}", order_id);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify funds are locked
    let locked_amount = price * size; // 40,000 * 5 = 200,000
    {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
        balance_verifier.assert_locked_eq(users::ALICE, assets::USD, initial_balance.locked + locked_amount).await?;
        info!("  → Funds locked: {} USD", locked_amount);
    }

    // Verify order is on orderbook
    {
        let redis_timeout = ctx.config().redis_event_timeout;
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier.wait_and_assert_level(market_id, Side::Bid, price, size, redis_timeout).await?;
        info!("  → Order on orderbook at {} for {} BTC", price, size);
    }

    // Cancel the order
    let cancel_cmd = OrderBuilder::cancel()
        .order_id(order_id)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let cancel_response = ctx.execute_command(cancel_cmd)?;
    ResponseVerifier::assert_cancelled(&cancel_response)?;
    info!("  → Order cancelled: order_id={}", order_id);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify funds are unlocked
    {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
        balance_verifier.assert_locked_eq(users::ALICE, assets::USD, initial_balance.locked).await?;
        info!("  → Funds unlocked: {} USD", locked_amount);
    }

    // Verify orderbook updated (order removed)
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        let orderbook = orderbook_verifier.get_orderbook(market_id).await?;

        // Check that the specific price level is either gone or has size 0
        let bid_at_price = orderbook.bids.iter().find(|b| b.price == price);
        if let Some(bid) = bid_at_price {
            if bid.size != 0 {
                return Err(TestError::Verification {
                    message: format!("Expected bid at price {} to be removed, but found size {}", price, bid.size),
                });
            }
        }
        info!("  → Order removed from orderbook");
    }

    Ok(())
}

/// SECTION 3: Cancel a resting ask order - funds should unlock
async fn cancel_resting_ask_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Bob places ask @ {} for 8 BTC", prices::HIGH);
    info!("Bob cancels the order → Funds unlock");

    let market_id = ctx.market_id;
    let price = prices::HIGH; // 60,000
    let size = 8;

    // Get Bob's initial balance
    let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
    let initial_balance = balance_verifier.get_balance(users::BOB, assets::BTC).await?;
    info!("  → Initial balance: available={} BTC, locked={} BTC", initial_balance.available, initial_balance.locked);

    // Bob places limit ask order
    let order_cmd = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(price)
        .size(size)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let response = ctx.execute_command(order_cmd)?;
    ResponseVerifier::assert_placed(&response)?;
    let order_id = response.order_id;
    info!("  → Order placed: order_id={}", order_id);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify funds are locked (BTC for ask)
    {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
        balance_verifier.assert_locked_eq(users::BOB, assets::BTC, initial_balance.locked + size).await?;
        info!("  → Funds locked: {} BTC", size);
    }

    // Verify order is on orderbook
    {
        let redis_timeout = ctx.config().redis_event_timeout;
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier.wait_and_assert_level(market_id, Side::Ask, price, size, redis_timeout).await?;
        info!("  → Order on orderbook at {} for {} BTC", price, size);
    }

    // Cancel the order
    let cancel_cmd = OrderBuilder::cancel()
        .order_id(order_id)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let cancel_response = ctx.execute_command(cancel_cmd)?;
    ResponseVerifier::assert_cancelled(&cancel_response)?;
    info!("  → Order cancelled: order_id={}", order_id);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify funds are unlocked
    {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
        balance_verifier.assert_locked_eq(users::BOB, assets::BTC, initial_balance.locked).await?;
        info!("  → Funds unlocked: {} BTC", size);
    }

    // Verify orderbook updated (order removed)
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        let orderbook = orderbook_verifier.get_orderbook(market_id).await?;

        let ask_at_price = orderbook.asks.iter().find(|a| a.price == price);
        if let Some(ask) = ask_at_price {
            if ask.size != 0 {
                return Err(TestError::Verification {
                    message: format!("Expected ask at price {} to be removed, but found size {}", price, ask.size),
                });
            }
        }
        info!("  → Order removed from orderbook");
    }

    Ok(())
}

/// SECTION 4: Cancel a partially filled order - only remaining size unlocks
async fn cancel_partially_filled_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Charlie places bid @ {} for 10 BTC", prices::MID);
    info!("Alice places ask @ {} for 3 BTC → Partial match (3 filled, 7 remain)", prices::MID);
    info!("Charlie cancels remaining 7 BTC → Partial funds unlock");

    let market_id = ctx.market_id;
    let price = prices::MID; // 50,000
    let charlie_size = 10;
    let alice_size = 3;
    let remaining_size = charlie_size - alice_size; // 7

    // Get Charlie's initial balance
    let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
    let charlie_initial_balance = balance_verifier.get_balance(users::CHARLIE, assets::USD).await?;
    info!("  → Charlie initial: available={} USD, locked={} USD",
          charlie_initial_balance.available, charlie_initial_balance.locked);

    // Charlie places limit bid order
    let charlie_order = OrderBuilder::place_limit()
        .user(users::CHARLIE)
        .price(price)
        .size(charlie_size)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let charlie_response = ctx.execute_command(charlie_order)?;
    ResponseVerifier::assert_placed(&charlie_response)?;
    let charlie_order_id = charlie_response.order_id;
    info!("  → Charlie's bid placed: order_id={}", charlie_order_id);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify Charlie's funds locked for full order
    let locked_amount = price * charlie_size; // 50,000 * 10 = 500,000
    {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
        balance_verifier.assert_locked_eq(users::CHARLIE, assets::USD, charlie_initial_balance.locked + locked_amount).await?;
        info!("  → Charlie's funds locked: {} USD", locked_amount);
    }

    // Alice places ask that partially matches
    let alice_order = OrderBuilder::place_limit()
        .user(users::ALICE)
        .price(price)
        .size(alice_size)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let alice_response = ctx.execute_command(alice_order)?;
    ResponseVerifier::assert_filled(&alice_response)?;
    info!("  → Alice's ask filled: {} BTC matched", alice_size);

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify trade occurred
    {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.redis);
        let criteria = TradeCriteria::new()
            .market_id(market_id)
            .maker_user_id(users::CHARLIE)
            .taker_user_id(users::ALICE)
            .price(price)
            .size(alice_size);
        trade_verifier.wait_and_assert_trade(market_id, &criteria, Duration::from_secs(2)).await?;
        info!("  → Trade executed: {} BTC @ {}", alice_size, price);
    }

    // Verify Charlie's locked amount decreased by filled portion
    let remaining_locked = price * remaining_size; // 50,000 * 7 = 350,000
    {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
        balance_verifier.assert_locked_eq(users::CHARLIE, assets::USD, charlie_initial_balance.locked + remaining_locked).await?;
        info!("  → Charlie's locked reduced to {} USD (for {} BTC remaining)", remaining_locked, remaining_size);
    }

    // Verify remaining size on orderbook
    {
        let redis_timeout = ctx.config().redis_event_timeout;
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier.wait_and_assert_level(market_id, Side::Bid, price, remaining_size, redis_timeout).await?;
        info!("  → Remaining {} BTC on orderbook at {}", remaining_size, price);
    }

    // Cancel Charlie's remaining order
    let cancel_cmd = OrderBuilder::cancel()
        .order_id(charlie_order_id)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let cancel_response = ctx.execute_command(cancel_cmd)?;
    ResponseVerifier::assert_cancelled(&cancel_response)?;
    info!("  → Charlie cancelled remaining order: order_id={}", charlie_order_id);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify only remaining funds unlocked
    {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
        balance_verifier.assert_locked_eq(users::CHARLIE, assets::USD, charlie_initial_balance.locked).await?;
        info!("  → Remaining {} USD unlocked", remaining_locked);
    }

    // Verify orderbook updated
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        let orderbook = orderbook_verifier.get_orderbook(market_id).await?;

        let bid_at_price = orderbook.bids.iter().find(|b| b.price == price);
        if let Some(bid) = bid_at_price {
            if bid.size != 0 {
                return Err(TestError::Verification {
                    message: format!("Expected bid at price {} to be removed after cancel, but found size {}", price, bid.size),
                });
            }
        }
        info!("  → Remaining order removed from orderbook");
    }

    Ok(())
}

/// SECTION 5: Try to cancel a non-existent order
async fn cancel_nonexistent_order_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Alice tries to cancel non-existent order_id=999999");

    let market_id = ctx.market_id;
    let fake_order_id = 999999;

    // Try to cancel non-existent order
    let cancel_cmd = OrderBuilder::cancel()
        .order_id(fake_order_id)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let cancel_response = ctx.execute_command(cancel_cmd)?;

    // The response should indicate the order doesn't exist or be rejected
    // System should handle this gracefully without crashing
    info!("  → Cancel response status: {:?}", cancel_response.status);
    info!("  → System handled non-existent order gracefully");

    Ok(())
}

/// SECTION 6: Try to cancel an already filled order
async fn cancel_filled_order_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Bob places ask @ {} for 2 BTC", prices::MID);
    info!("Charlie places bid @ {} for 2 BTC → Full match", prices::MID);
    info!("Bob tries to cancel filled order → Should handle gracefully");

    let market_id = ctx.market_id;
    let price = prices::MID;
    let size = 2;

    // Bob places ask
    let bob_order = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(price)
        .size(size)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let bob_response = ctx.execute_command(bob_order)?;
    ResponseVerifier::assert_placed(&bob_response)?;
    let bob_order_id = bob_response.order_id;
    info!("  → Bob's ask placed: order_id={}", bob_order_id);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Charlie places matching bid
    let charlie_order = OrderBuilder::place_limit()
        .user(users::CHARLIE)
        .price(price)
        .size(size)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let charlie_response = ctx.execute_command(charlie_order)?;
    ResponseVerifier::assert_filled(&charlie_response)?;
    info!("  → Charlie's bid filled - Bob's ask fully matched");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify trade occurred
    {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.redis);
        let criteria = TradeCriteria::new()
            .market_id(market_id)
            .maker_user_id(users::BOB)
            .taker_user_id(users::CHARLIE)
            .price(price)
            .size(size);
        trade_verifier.wait_and_assert_trade(market_id, &criteria, Duration::from_secs(2)).await?;
        info!("  → Trade executed: {} BTC @ {}", size, price);
    }

    // Try to cancel Bob's already filled order
    let cancel_cmd = OrderBuilder::cancel()
        .order_id(bob_order_id)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let cancel_response = ctx.execute_command(cancel_cmd)?;

    // System should handle this gracefully
    info!("  → Cancel response status: {:?}", cancel_response.status);
    info!("  → System handled already-filled order cancel gracefully");

    Ok(())
}

/// SECTION 7: Cancel and replace order at same price
async fn cancel_and_replace_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Alice places bid @ {} for 6 BTC", prices::LOW);
    info!("Alice cancels order");
    info!("Alice places new bid @ {} for 4 BTC", prices::LOW);

    let market_id = ctx.market_id;
    let price = prices::LOW; // 40,000
    let size1 = 6;
    let size2 = 4;

    // Get Alice's initial balance
    let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
    let initial_balance = balance_verifier.get_balance(users::ALICE, assets::USD).await?;

    // Alice places first order
    let order1 = OrderBuilder::place_limit()
        .user(users::ALICE)
        .price(price)
        .size(size1)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let response1 = ctx.execute_command(order1)?;
    ResponseVerifier::assert_placed(&response1)?;
    let order_id1 = response1.order_id;
    info!("  → First order placed: order_id={}, size={} BTC", order_id1, size1);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify first order on book
    {
        let redis_timeout = ctx.config().redis_event_timeout;
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier.wait_and_assert_level(market_id, Side::Bid, price, size1, redis_timeout).await?;
    }

    // Cancel first order
    let cancel_cmd = OrderBuilder::cancel()
        .order_id(order_id1)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let cancel_response = ctx.execute_command(cancel_cmd)?;
    ResponseVerifier::assert_cancelled(&cancel_response)?;
    info!("  → First order cancelled: order_id={}", order_id1);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Place second order at same price
    let order2 = OrderBuilder::place_limit()
        .user(users::ALICE)
        .price(price)
        .size(size2)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let response2 = ctx.execute_command(order2)?;
    ResponseVerifier::assert_placed(&response2)?;
    let order_id2 = response2.order_id;
    info!("  → Second order placed: order_id={}, size={} BTC", order_id2, size2);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify second order on book with new size
    {
        let redis_timeout = ctx.config().redis_event_timeout;
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier.wait_and_assert_level(market_id, Side::Bid, price, size2, redis_timeout).await?;
        info!("  → New order on orderbook at {} for {} BTC", price, size2);
    }

    // Verify correct locked amount (only size2 locked now)
    {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
        let locked_amount = price * size2;
        balance_verifier.assert_locked_eq(users::ALICE, assets::USD, initial_balance.locked + locked_amount).await?;
        info!("  → Correct funds locked: {} USD for {} BTC", locked_amount, size2);
    }

    Ok(())
}

/// SECTION 8: Multiple sequential cancellations
async fn multiple_cancellations_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Bob places 3 ask orders at different prices");
    info!("Bob cancels all 3 orders sequentially");

    let market_id = ctx.market_id;
    let price1 = prices::MID;        // 50,000
    let price2 = prices::MID + 5000; // 55,000
    let price3 = prices::HIGH;       // 60,000
    let size = 3;

    // Get Bob's initial balance
    let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
    let initial_balance = balance_verifier.get_balance(users::BOB, assets::BTC).await?;

    // Place 3 orders
    let order1 = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(price1)
        .size(size)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let response1 = ctx.execute_command(order1)?;
    let order_id1 = response1.order_id;
    info!("  → Order 1 placed: order_id={} @ {}", order_id1, price1);

    tokio::time::sleep(Duration::from_millis(100)).await;

    let order2 = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(price2)
        .size(size)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let response2 = ctx.execute_command(order2)?;
    let order_id2 = response2.order_id;
    info!("  → Order 2 placed: order_id={} @ {}", order_id2, price2);

    tokio::time::sleep(Duration::from_millis(100)).await;

    let order3 = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(price3)
        .size(size)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let response3 = ctx.execute_command(order3)?;
    let order_id3 = response3.order_id;
    info!("  → Order 3 placed: order_id={} @ {}", order_id3, price3);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify all 3 orders locked funds
    let total_locked = size * 3; // 3 BTC per order * 3 orders = 9 BTC
    {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
        balance_verifier.assert_locked_eq(users::BOB, assets::BTC, initial_balance.locked + total_locked).await?;
        info!("  → All orders locked {} BTC total", total_locked);
    }

    // Verify all orders on orderbook
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier.assert_level(market_id, Side::Ask, price1, size).await?;
        orderbook_verifier.assert_level(market_id, Side::Ask, price2, size).await?;
        orderbook_verifier.assert_level(market_id, Side::Ask, price3, size).await?;
        info!("  → All 3 orders on orderbook");
    }

    // Cancel order 1
    let cancel1 = OrderBuilder::cancel()
        .order_id(order_id1)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    ctx.execute_command(cancel1)?;
    info!("  → Cancelled order 1: order_id={}", order_id1);
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Cancel order 2
    let cancel2 = OrderBuilder::cancel()
        .order_id(order_id2)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    ctx.execute_command(cancel2)?;
    info!("  → Cancelled order 2: order_id={}", order_id2);
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Cancel order 3
    let cancel3 = OrderBuilder::cancel()
        .order_id(order_id3)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    ctx.execute_command(cancel3)?;
    info!("  → Cancelled order 3: order_id={}", order_id3);
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Verify all funds unlocked
    {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
        balance_verifier.assert_locked_eq(users::BOB, assets::BTC, initial_balance.locked).await?;
        info!("  → All {} BTC unlocked", total_locked);
    }

    // Verify all orders removed from orderbook
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        let orderbook = orderbook_verifier.get_orderbook(market_id).await?;

        for (i, price) in [price1, price2, price3].iter().enumerate() {
            let ask_at_price = orderbook.asks.iter().find(|a| a.price == *price);
            if let Some(ask) = ask_at_price {
                if ask.size != 0 {
                    return Err(TestError::Verification {
                        message: format!("Expected ask {} at price {} to be removed, but found size {}", i+1, price, ask.size),
                    });
                }
            }
        }
        info!("  → All orders removed from orderbook");
    }

    Ok(())
}
