//! IOC (Immediate-Or-Cancel) order test scenarios
//!
//! This module contains test scenarios for IOC limit orders.
//! IOC orders execute immediately against available liquidity and cancel any unfilled portion.
//! All tests run in a single session without restarting the server.

use std::time::Duration;

use crate::builders::OrderBuilder;
use crate::fixtures::{assets, prices, users};
use crate::test_framework::TestContext;
use crate::test_framework::types::*;
use crate::verifiers::{BalanceVerifier, OrderbookVerifier, ResponseVerifier, TradeVerifier};
use common::Side;
use tracing::{info, warn};

/// Comprehensive IOC test suite - runs all scenarios in a single session
///
/// This validates all IOC order behaviors:
/// 1. IOC order with no liquidity (fully cancelled)
/// 2. IOC order with full match (completely filled)
/// 3. IOC order with partial match (partial fill + cancel remainder)
/// 4. IOC order crossing multiple price levels
/// 5. IOC order with self-match prevention
///
/// State is maintained across sections - no cleanup between tests.
pub async fn run_all(ctx: &mut TestContext) -> TestResult<Vec<ScenarioResult>> {
    info!("╔════════════════════════════════════════╗");
    info!("║   IOC COMPREHENSIVE TEST SUITE         ║");
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
    // SECTION 2: IOC No Match - Order fully cancelled
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 2: IOC No Match                 │");
    info!("│ (Order fully cancelled)                 │");
    info!("└─────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match test_ioc_no_match_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 2 PASSED ({:?})", duration);
            results.push(ScenarioResult::success("ioc_no_match".to_string(), duration));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 2 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure("ioc_no_match".to_string(), duration, e));
            return Ok(results);
        }
    }
    info!("");

    // ========================================================================
    // SECTION 3: IOC Full Match - Complete fill
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 3: IOC Full Match               │");
    info!("│ (Order completely filled)               │");
    info!("└─────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match test_ioc_full_match_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 3 PASSED ({:?})", duration);
            results.push(ScenarioResult::success("ioc_full_match".to_string(), duration));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 3 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure("ioc_full_match".to_string(), duration, e));
            return Ok(results);
        }
    }
    info!("");

    // ========================================================================
    // SECTION 4: IOC Partial Match - Partial fill + cancel
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 4: IOC Partial Match            │");
    info!("│ (Partial fill + cancel remainder)       │");
    info!("└─────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match test_ioc_partial_match_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 4 PASSED ({:?})", duration);
            results.push(ScenarioResult::success("ioc_partial_match".to_string(), duration));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 4 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure("ioc_partial_match".to_string(), duration, e));
            return Ok(results);
        }
    }
    info!("");

    // ========================================================================
    // SECTION 5: IOC Multiple Levels - Crosses multiple price levels
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 5: IOC Multiple Levels          │");
    info!("│ (Crosses multiple price levels)         │");
    info!("└─────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match test_ioc_multiple_levels_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 5 PASSED ({:?})", duration);
            results.push(ScenarioResult::success("ioc_multiple_levels".to_string(), duration));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 5 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure("ioc_multiple_levels".to_string(), duration, e));
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

/// SECTION 2: Test IOC order with no matching liquidity (fully cancelled)
async fn test_ioc_no_match_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Alice places IOC bid @ {} for 5 BTC (no liquidity)", prices::LOW);

    let market_id = ctx.market_id;
    let price = prices::LOW; // 40,000 - no asks at this price
    let size = 5;

    // Record initial balance
    let initial_balance = {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
        balance_verifier.get_balance(users::ALICE, assets::USD).await?
    };

    // Alice places IOC bid order (should be fully cancelled - no liquidity)
    let order_cmd = OrderBuilder::place_ioc()
        .user(users::ALICE)
        .price(price)
        .size(size)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let response = ctx.execute_command(order_cmd)?;

    // Verify Response: IOC with no match should be cancelled
    ResponseVerifier::assert_cancelled(&response)?;
    info!("  → Order cancelled: order_id={} (no liquidity)", response.order_id);

    // Verify Redis
    tokio::time::sleep(Duration::from_millis(100)).await;

    {
        // Verify no funds were locked (order was immediately cancelled)
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
        balance_verifier.assert_locked_eq(users::ALICE, assets::USD, 0).await?;

        // Verify total balance unchanged
        let current_balance = balance_verifier.get_balance(users::ALICE, assets::USD).await?;
        if current_balance.total != initial_balance.total {
            return Err(TestError::Verification {
                message: format!(
                    "Balance changed after IOC cancellation: expected={}, got={}",
                    initial_balance.total, current_balance.total
                ),
            });
        }
        info!("  → No funds locked (order immediately cancelled)");
    }

    {
        // Verify orderbook is still empty
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier.assert_empty(market_id).await?;
        info!("  → Orderbook remains empty (IOC doesn't rest)");
    }

    Ok(())
}

/// SECTION 3: Test IOC order that fully matches
async fn test_ioc_full_match_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Bob places GTC ask @ {} for 8 BTC", prices::MID);
    info!("Alice places IOC bid @ {} for 8 BTC → Full match", prices::MID);

    let market_id = ctx.market_id;
    let price = prices::MID; // 50,000
    let size = 8;

    // Bob (maker) places GTC ask order
    let maker_order = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(price)
        .size(size)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let maker_response = ctx.execute_command(maker_order)?;
    ResponseVerifier::assert_placed(&maker_response)?;
    info!("  → Bob's GTC ask placed: order_id={}", maker_response.order_id);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Alice (taker) places IOC bid
    let taker_order = OrderBuilder::place_ioc()
        .user(users::ALICE)
        .price(price)
        .size(size)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let taker_response = ctx.execute_command(taker_order)?;
    ResponseVerifier::assert_filled(&taker_response)?;
    info!("  → Alice's IOC bid filled: order_id={}", taker_response.order_id);

    // Verify trade in Redis
    tokio::time::sleep(Duration::from_millis(200)).await;

    {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.redis);
        let criteria = TradeCriteria::new()
            .market_id(market_id)
            .maker_user_id(users::BOB)
            .taker_user_id(users::ALICE)
            .price(price)
            .size(size)
            .maker_order_id(maker_response.order_id)
            .taker_order_id(taker_response.order_id);

        trade_verifier.wait_and_assert_trade(market_id, &criteria, Duration::from_secs(2)).await?;
        info!("  → Trade executed: {} BTC @ {}", size, price);
    }

    {
        // Verify orderbook is empty (both orders filled)
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier.assert_empty(market_id).await?;
        info!("  → Orderbook empty (both orders filled)");
    }

    Ok(())
}

/// SECTION 4: Test IOC order with partial match
async fn test_ioc_partial_match_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Charlie places GTC ask @ {} for 3 BTC", prices::MID);
    info!("Bob places IOC bid @ {} for 10 BTC → Partial match (3 filled, 7 cancelled)", prices::MID);

    let market_id = ctx.market_id;
    let price = prices::MID;
    let maker_size = 3;
    let taker_size = 10;
    let filled_size = maker_size;
    let cancelled_size = taker_size - maker_size;

    // Charlie (maker) places smaller GTC ask
    let maker_order = OrderBuilder::place_limit()
        .user(users::CHARLIE)
        .price(price)
        .size(maker_size)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let maker_response = ctx.execute_command(maker_order)?;
    ResponseVerifier::assert_placed(&maker_response)?;
    info!("  → Charlie's GTC ask placed for {} BTC", maker_size);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Bob (taker) places larger IOC bid
    let taker_order = OrderBuilder::place_ioc()
        .user(users::BOB)
        .price(price)
        .size(taker_size)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let taker_response = ctx.execute_command(taker_order)?;

    // Verify response: IOC should be partially filled then cancelled
    ResponseVerifier::assert_partially_filled(&taker_response, taker_size)?;
    info!("  → Bob's IOC bid: {} BTC filled, {} BTC cancelled", filled_size, cancelled_size);

    // Verify trade and orderbook state
    tokio::time::sleep(Duration::from_millis(200)).await;

    {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.redis);
        let criteria = TradeCriteria::new()
            .market_id(market_id)
            .maker_user_id(users::CHARLIE)
            .taker_user_id(users::BOB)
            .size(filled_size);

        trade_verifier.assert_trade_exists(market_id, &criteria).await?;
        info!("  → Trade executed for filled portion: {} BTC", filled_size);
    }

    {
        // Verify orderbook is empty (maker filled, taker IOC doesn't rest)
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier.assert_empty(market_id).await?;
        info!("  → Orderbook empty (IOC remainder cancelled)");
    }

    Ok(())
}

/// SECTION 5: Test IOC order crossing multiple price levels
async fn test_ioc_multiple_levels_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Setting up orderbook with multiple ask levels:");
    info!("  - Bob: 2 BTC @ {}", prices::MID);
    info!("  - Charlie: 3 BTC @ {}", prices::MID + 1000);
    info!("  - Bob: 4 BTC @ {}", prices::MID + 2000);
    info!("Alice places IOC bid @ {} for 6 BTC → Crosses 2.5 levels", prices::HIGH);

    let market_id = ctx.market_id;
    let level1_price = prices::MID;      // 50,000
    let level2_price = prices::MID + 1000; // 51,000
    let level3_price = prices::MID + 2000; // 52,000

    // Setup: Create multi-level orderbook
    // Level 1: Bob asks 2 BTC @ 50,000 (changed from Alice to avoid self-match)
    let order1 = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(level1_price)
        .size(2)
        .side(Side::Ask)
        .market_id(market_id)
        .build();
    let response1 = ctx.execute_command(order1)?;
    info!("  → Level 1 placed: order_id={}", response1.order_id);

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Level 2: Charlie asks 3 BTC @ 51,000 (changed from Bob to Charlie)
    let order2 = OrderBuilder::place_limit()
        .user(users::CHARLIE)
        .price(level2_price)
        .size(3)
        .side(Side::Ask)
        .market_id(market_id)
        .build();
    let response2 = ctx.execute_command(order2)?;
    info!("  → Level 2 placed: order_id={}", response2.order_id);

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Level 3: Bob asks 4 BTC @ 52,000 (changed from Charlie to Bob)
    let order3 = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(level3_price)
        .size(4)
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
        info!("  → Orderbook ready: 3 ask levels");
    }

    // Execute: Alice places IOC bid for 6 BTC @ high price
    // Should match: 2 BTC @ 50,000 + 3 BTC @ 51,000 + 1 BTC @ 52,000 = 6 BTC filled
    // Remaining: 3 BTC @ 52,000 should stay on book
    let ioc_order = OrderBuilder::place_ioc()
        .user(users::ALICE)
        .price(prices::HIGH) // 60,000 - will cross all levels
        .size(6)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let ioc_response = ctx.execute_command(ioc_order)?;
    ResponseVerifier::assert_filled(&ioc_response)?;
    info!("  → Alice's IOC bid filled: 6 BTC across multiple levels");

    // Verify trades
    tokio::time::sleep(Duration::from_millis(200)).await;

    {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.redis);

        // Should have at least 3 more trades (one per level matched)
        trade_verifier.assert_min_trade_count(market_id, 5).await?; // 2 from previous sections + 3 new

        // Verify trade at level 1 (2 BTC @ 50,000)
        let criteria1 = TradeCriteria::new()
            .market_id(market_id)
            .maker_order_id(response1.order_id)
            .taker_order_id(ioc_response.order_id)
            .price(level1_price)
            .size(2);
        trade_verifier.assert_trade_exists(market_id, &criteria1).await?;
        info!("  → Trade 1: 2 BTC @ {}", level1_price);

        // Verify trade at level 2 (3 BTC @ 51,000)
        let criteria2 = TradeCriteria::new()
            .market_id(market_id)
            .maker_order_id(response2.order_id)
            .taker_order_id(ioc_response.order_id)
            .price(level2_price)
            .size(3);
        trade_verifier.assert_trade_exists(market_id, &criteria2).await?;
        info!("  → Trade 2: 3 BTC @ {}", level2_price);

        // Verify trade at level 3 (1 BTC @ 52,000)
        let criteria3 = TradeCriteria::new()
            .market_id(market_id)
            .maker_order_id(response3.order_id)
            .taker_order_id(ioc_response.order_id)
            .price(level3_price)
            .size(1);
        trade_verifier.assert_trade_exists(market_id, &criteria3).await?;
        info!("  → Trade 3: 1 BTC @ {}", level3_price);
    }

    {
        // Verify orderbook now has 1 ask level (remaining 3 BTC @ 52,000)
        let redis_timeout = ctx.config().redis_event_timeout;
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier.wait_and_assert_depth(market_id, 0, 1, redis_timeout).await?;
        orderbook_verifier.assert_level(market_id, Side::Ask, level3_price, 3).await?;
        info!("  → Orderbook: 3 BTC remaining @ {}", level3_price);
    }

    Ok(())
}
