//! FOK (Fill-Or-Kill) order test scenarios
//!
//! This module contains test scenarios for FOK limit orders.
//! FOK orders must be completely filled immediately or be fully rejected (killed).
//! All tests run in a single session without restarting the server.

use std::time::Duration;

use crate::builders::OrderBuilder;
use crate::fixtures::{assets, prices, users};
use crate::test_framework::TestContext;
use crate::test_framework::types::*;
use crate::verifiers::{BalanceVerifier, OrderbookVerifier, ResponseVerifier, TradeVerifier};
use common::Side;
use tracing::{info, warn};

/// Comprehensive FOK test suite - runs all scenarios in a single session
///
/// This validates all FOK order behaviors:
/// 1. FOK order with no liquidity (fully rejected)
/// 2. FOK order with insufficient liquidity (partial available, fully rejected)
/// 3. FOK order with exact liquidity (completely filled)
/// 4. FOK order with excess liquidity (completely filled, remainder stays on book)
/// 5. FOK order crossing multiple price levels (all-or-nothing across levels)
///
/// State is maintained across sections - no cleanup between tests.
pub async fn run_all(ctx: &mut TestContext) -> TestResult<Vec<ScenarioResult>> {
    info!("╔════════════════════════════════════════╗");
    info!("║   FOK COMPREHENSIVE TEST SUITE         ║");
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
    ctx.fund_user(users::ALICE, 10_000_000, assets::USD).await?;  // 10M USD
    ctx.fund_user(users::ALICE, 1_000, assets::BTC).await?;       // 1000 BTC
    ctx.fund_user(users::BOB, 10_000_000, assets::USD).await?;    // 10M USD
    ctx.fund_user(users::BOB, 1_000, assets::BTC).await?;         // 1000 BTC
    ctx.fund_user(users::CHARLIE, 10_000_000, assets::USD).await?; // 10M USD
    ctx.fund_user(users::CHARLIE, 1_000, assets::BTC).await?;      // 1000 BTC

    info!("✓ All users funded successfully");
    info!("");

    // ========================================================================
    // SECTION 2: FOK No Match - Order fully rejected
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 2: FOK No Match                 │");
    info!("│ (Order fully rejected)                  │");
    info!("└─────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match test_fok_no_match_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 2 PASSED ({:?})", duration);
            results.push(ScenarioResult::success("fok_no_match".to_string(), duration));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 2 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure("fok_no_match".to_string(), duration, e));
            return Ok(results);
        }
    }
    info!("");

    // ========================================================================
    // SECTION 3: FOK Insufficient Liquidity - Order rejected (partial available)
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 3: FOK Insufficient Liquidity   │");
    info!("│ (Order rejected, partial available)     │");
    info!("└─────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match test_fok_insufficient_liquidity_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 3 PASSED ({:?})", duration);
            results.push(ScenarioResult::success("fok_insufficient_liquidity".to_string(), duration));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 3 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure("fok_insufficient_liquidity".to_string(), duration, e));
            return Ok(results);
        }
    }
    info!("");

    // ========================================================================
    // SECTION 4: FOK Exact Match - Order completely filled
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 4: FOK Exact Match              │");
    info!("│ (Order completely filled)               │");
    info!("└─────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match test_fok_exact_match_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 4 PASSED ({:?})", duration);
            results.push(ScenarioResult::success("fok_exact_match".to_string(), duration));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 4 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure("fok_exact_match".to_string(), duration, e));
            return Ok(results);
        }
    }
    info!("");

    // ========================================================================
    // SECTION 5: FOK Excess Liquidity - Order filled, remainder stays
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 5: FOK Excess Liquidity         │");
    info!("│ (Order filled, excess remains on book)  │");
    info!("└─────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match test_fok_excess_liquidity_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 5 PASSED ({:?})", duration);
            results.push(ScenarioResult::success("fok_excess_liquidity".to_string(), duration));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 5 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure("fok_excess_liquidity".to_string(), duration, e));
            return Ok(results);
        }
    }
    info!("");

    // ========================================================================
    // SECTION 6: FOK Multiple Levels - All-or-nothing across levels
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 6: FOK Multiple Levels          │");
    info!("│ (All-or-nothing across price levels)    │");
    info!("└─────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match test_fok_multiple_levels_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 6 PASSED ({:?})", duration);
            results.push(ScenarioResult::success("fok_multiple_levels".to_string(), duration));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 6 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure("fok_multiple_levels".to_string(), duration, e));
            return Ok(results);
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

/// SECTION 2: Test FOK order with no matching liquidity (fully rejected)
async fn test_fok_no_match_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Alice places FOK bid @ {} for 5 BTC (no liquidity)", prices::LOW);

    let market_id = ctx.market_id;
    let price = prices::LOW; // 40,000 - no asks at this price
    let size = 5;

    // Record initial balance
    let initial_balance = {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
        balance_verifier.get_balance(users::ALICE, assets::USD).await?
    };

    // Alice places FOK bid order (should be fully rejected - no liquidity)
    let order_cmd = OrderBuilder::place_fok()
        .user(users::ALICE)
        .price(price)
        .size(size)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let response = ctx.execute_command(order_cmd)?;

    // Verify Response: FOK with no match should be rejected/cancelled
    ResponseVerifier::assert_cancelled(&response)?;
    info!("  → Order rejected: order_id={} (no liquidity)", response.order_id);

    // Verify Redis
    tokio::time::sleep(Duration::from_millis(100)).await;

    {
        // Verify no funds were locked (order was immediately rejected)
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
        balance_verifier.assert_locked_eq(users::ALICE, assets::USD, 0).await?;

        // Verify total balance unchanged
        let current_balance = balance_verifier.get_balance(users::ALICE, assets::USD).await?;
        if current_balance.total != initial_balance.total {
            return Err(TestError::Verification {
                message: format!(
                    "Balance changed after FOK rejection: expected={}, got={}",
                    initial_balance.total, current_balance.total
                ),
            });
        }
        info!("  → No funds locked (order immediately rejected)");
    }

    {
        // Verify orderbook is still empty
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier.assert_empty(market_id).await?;
        info!("  → Orderbook remains empty (FOK doesn't rest)");
    }

    Ok(())
}

/// SECTION 3: Test FOK order with insufficient liquidity (rejected)
async fn test_fok_insufficient_liquidity_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Bob places GTC ask @ {} for 3 BTC", prices::MID);
    info!("Alice places FOK bid @ {} for 5 BTC → Rejected (only 3 BTC available)", prices::MID);

    let market_id = ctx.market_id;
    let price = prices::MID; // 50,000
    let maker_size = 3;
    let fok_size = 5;

    // Bob (maker) places GTC ask order for 3 BTC
    let maker_order = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(price)
        .size(maker_size)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let maker_response = ctx.execute_command(maker_order)?;
    ResponseVerifier::assert_placed(&maker_response)?;
    info!("  → Bob's GTC ask placed: order_id={}", maker_response.order_id);

    // Wait longer for orderbook to update
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Record Bob's ask on orderbook
    {
        let redis_timeout = ctx.config().redis_event_timeout;
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier.wait_and_assert_depth(market_id, 0, 1, redis_timeout).await?;
        info!("  → Orderbook has 1 ask level: {} BTC @ {}", maker_size, price);
    }

    // Alice (taker) places FOK bid for 5 BTC (more than available)
    let fok_order = OrderBuilder::place_fok()
        .user(users::ALICE)
        .price(price)
        .size(fok_size)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let fok_response = ctx.execute_command(fok_order)?;

    // Verify Response: FOK should be rejected (insufficient liquidity)
    ResponseVerifier::assert_cancelled(&fok_response)?;
    info!("  → Alice's FOK bid rejected (insufficient liquidity: need {} BTC, only {} BTC available)", fok_size, maker_size);

    // Verify no trade occurred
    tokio::time::sleep(Duration::from_millis(200)).await;

    {
        // Verify Bob's ask is still on the orderbook (FOK was rejected, no trade)
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier.assert_level(market_id, Side::Ask, price, maker_size).await?;
        info!("  → Bob's ask still on book: {} BTC @ {} (FOK was rejected)", maker_size, price);
    }

    Ok(())
}

/// SECTION 4: Test FOK order with exact liquidity match
async fn test_fok_exact_match_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("First, consume Bob's leftover ask from Section 3");
    info!("Charlie places GTC ask @ {} for 6 BTC", prices::MID);
    info!("Alice places FOK bid @ {} for 6 BTC → Filled exactly", prices::MID);

    let market_id = ctx.market_id;
    let price = prices::MID; // 50,000
    let size = 6;

    // First, clear Bob's leftover 3 BTC ask from Section 3 by having Alice buy it with IOC
    let cleanup_order = OrderBuilder::place_ioc()
        .user(users::ALICE)
        .price(price)
        .size(3)
        .side(Side::Bid)
        .market_id(market_id)
        .build();
    ctx.execute_command(cleanup_order)?;
    info!("  → Consumed Bob's leftover 3 BTC ask from Section 3");

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Charlie (maker) places GTC ask order
    let maker_order = OrderBuilder::place_limit()
        .user(users::CHARLIE)
        .price(price)
        .size(size)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let maker_response = ctx.execute_command(maker_order)?;
    ResponseVerifier::assert_placed(&maker_response)?;
    info!("  → Charlie's GTC ask placed: order_id={}", maker_response.order_id);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Alice (taker) places FOK bid for exact amount available
    let fok_order = OrderBuilder::place_fok()
        .user(users::ALICE)
        .price(price)
        .size(size)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let fok_response = ctx.execute_command(fok_order)?;

    // Verify Response: FOK should be filled
    ResponseVerifier::assert_filled(&fok_response)?;
    info!("  → Alice's FOK bid filled: order_id={}", fok_response.order_id);

    // Verify trade occurred
    tokio::time::sleep(Duration::from_millis(200)).await;

    {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.redis);
        let criteria = TradeCriteria::new()
            .market_id(market_id)
            .maker_user_id(users::CHARLIE)
            .taker_user_id(users::ALICE)
            .price(price)
            .size(size)
            .maker_order_id(maker_response.order_id)
            .taker_order_id(fok_response.order_id);

        trade_verifier.wait_and_assert_trade(market_id, &criteria, Duration::from_secs(2)).await?;
        info!("  → Trade executed: {} BTC @ {}", size, price);
    }

    {
        // Verify orderbook is now empty (both orders filled exactly)
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier.assert_empty(market_id).await?;
        info!("  → Orderbook empty (exact match)");
    }

    Ok(())
}

/// SECTION 5: Test FOK order with excess liquidity
async fn test_fok_excess_liquidity_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Bob places GTC ask @ {} for 10 BTC", prices::MID);
    info!("Alice places FOK bid @ {} for 7 BTC → Filled, 3 BTC remains", prices::MID);

    let market_id = ctx.market_id;
    let price = prices::MID;
    let maker_size = 10;
    let fok_size = 7;
    let remaining = maker_size - fok_size;

    // Bob (maker) places larger GTC ask
    let maker_order = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(price)
        .size(maker_size)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let maker_response = ctx.execute_command(maker_order)?;
    ResponseVerifier::assert_placed(&maker_response)?;
    info!("  → Bob's GTC ask placed for {} BTC", maker_size);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Alice (taker) places FOK bid for less than available
    let fok_order = OrderBuilder::place_fok()
        .user(users::ALICE)
        .price(price)
        .size(fok_size)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let fok_response = ctx.execute_command(fok_order)?;

    // Verify Response: FOK should be filled
    ResponseVerifier::assert_filled(&fok_response)?;
    info!("  → Alice's FOK bid filled: {} BTC", fok_size);

    // Verify trade and remaining order
    tokio::time::sleep(Duration::from_millis(200)).await;

    {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.redis);
        let criteria = TradeCriteria::new()
            .market_id(market_id)
            .maker_user_id(users::BOB)
            .taker_user_id(users::ALICE)
            .size(fok_size);

        trade_verifier.assert_trade_exists(market_id, &criteria).await?;
        info!("  → Trade executed: {} BTC", fok_size);
    }

    {
        // Verify remaining ask stays on orderbook
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier.assert_level(market_id, Side::Ask, price, remaining).await?;
        info!("  → Remaining on orderbook: {} BTC @ {}", remaining, price);
    }

    Ok(())
}

/// SECTION 6: Test FOK order crossing multiple price levels (all-or-nothing)
async fn test_fok_multiple_levels_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("First, consume Bob's leftover 3 BTC ask @ {} from Section 5", prices::MID);
    info!("Setting up orderbook with multiple ask levels:");
    info!("  - Charlie: 2 BTC @ {}", prices::MID);
    info!("  - Bob: 3 BTC @ {}", prices::MID + 1000);
    info!("  - Charlie: 2 BTC @ {}", prices::MID + 2000);
    info!("Alice places FOK bid @ {} for 7 BTC → All-or-nothing across levels", prices::HIGH);

    let market_id = ctx.market_id;
    let level1_price = prices::MID;      // 50,000
    let level2_price = prices::MID + 1000; // 51,000
    let level3_price = prices::MID + 2000; // 52,000

    // First, clear Bob's leftover 3 BTC ask from Section 5
    let cleanup_order = OrderBuilder::place_ioc()
        .user(users::ALICE)
        .price(level1_price)
        .size(3)
        .side(Side::Bid)
        .market_id(market_id)
        .build();
    ctx.execute_command(cleanup_order)?;
    info!("  → Consumed Bob's leftover 3 BTC ask from Section 5");

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Setup: Create multi-level orderbook
    // Level 1: Charlie asks 2 BTC @ 50,000
    let order1 = OrderBuilder::place_limit()
        .user(users::CHARLIE)
        .price(level1_price)
        .size(2)
        .side(Side::Ask)
        .market_id(market_id)
        .build();
    let response1 = ctx.execute_command(order1)?;
    info!("  → Level 1 placed: order_id={}", response1.order_id);

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Level 2: Bob asks 3 BTC @ 51,000
    let order2 = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(level2_price)
        .size(3)
        .side(Side::Ask)
        .market_id(market_id)
        .build();
    let response2 = ctx.execute_command(order2)?;
    info!("  → Level 2 placed: order_id={}", response2.order_id);

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Level 3: Charlie asks 2 BTC @ 52,000
    let order3 = OrderBuilder::place_limit()
        .user(users::CHARLIE)
        .price(level3_price)
        .size(2)
        .side(Side::Ask)
        .market_id(market_id)
        .build();
    let response3 = ctx.execute_command(order3)?;
    info!("  → Level 3 placed: order_id={}", response3.order_id);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify orderbook has 3 ask levels
    {
        let redis_timeout = ctx.config().redis_event_timeout;
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier.wait_and_assert_depth(market_id, 0, 3, redis_timeout).await?;
        info!("  → Orderbook ready: 3 ask levels (2+3+2=7 BTC total)");
    }

    // Execute: Alice places FOK bid for exactly 7 BTC @ high price
    // Should match: 2 BTC @ 50,000 + 3 BTC @ 51,000 + 2 BTC @ 52,000 = 7 BTC filled
    let fok_order = OrderBuilder::place_fok()
        .user(users::ALICE)
        .price(prices::HIGH) // 60,000 - will cross all levels
        .size(7)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let fok_response = ctx.execute_command(fok_order)?;
    ResponseVerifier::assert_filled(&fok_response)?;
    info!("  → Alice's FOK bid filled: 7 BTC across 3 levels");

    // Verify trades
    tokio::time::sleep(Duration::from_millis(200)).await;

    {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.redis);

        // Verify trade at level 1 (2 BTC @ 50,000)
        let criteria1 = TradeCriteria::new()
            .market_id(market_id)
            .maker_order_id(response1.order_id)
            .taker_order_id(fok_response.order_id)
            .price(level1_price)
            .size(2);
        trade_verifier.assert_trade_exists(market_id, &criteria1).await?;
        info!("  → Trade 1: 2 BTC @ {}", level1_price);

        // Verify trade at level 2 (3 BTC @ 51,000)
        let criteria2 = TradeCriteria::new()
            .market_id(market_id)
            .maker_order_id(response2.order_id)
            .taker_order_id(fok_response.order_id)
            .price(level2_price)
            .size(3);
        trade_verifier.assert_trade_exists(market_id, &criteria2).await?;
        info!("  → Trade 2: 3 BTC @ {}", level2_price);

        // Verify trade at level 3 (2 BTC @ 52,000)
        let criteria3 = TradeCriteria::new()
            .market_id(market_id)
            .maker_order_id(response3.order_id)
            .taker_order_id(fok_response.order_id)
            .price(level3_price)
            .size(2);
        trade_verifier.assert_trade_exists(market_id, &criteria3).await?;
        info!("  → Trade 3: 2 BTC @ {}", level3_price);
    }

    {
        // Verify orderbook is now empty (all orders fully consumed)
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier.assert_empty(market_id).await?;
        info!("  → Orderbook empty (all liquidity consumed)");
    }

    Ok(())
}
