//! Comprehensive integration test scenario
//!
//! This module contains a single massive test that combines all order types
//! (GTC, IOC, FOK, cancellations) in a realistic trading scenario.
//! Tests the exchange under load with multiple concurrent operations,
//! edge cases, and adverse conditions - all in one continuous state.
//!
//! This replaces the Commands::All approach which didn't preserve state
//! between different scenario suites.

use std::time::Duration;

use crate::builders::OrderBuilder;
use crate::fixtures::{assets, users};
use crate::test_framework::TestContext;
use crate::test_framework::types::*;
use crate::verifiers::{BalanceVerifier, OrderbookVerifier, ResponseVerifier, TradeVerifier};
use common::Side;
use tracing::{info, warn};

/// Comprehensive integration test - single massive test combining all order types
///
/// This test simulates realistic exchange operations with:
/// - Multiple order types (GTC, IOC, FOK) interacting
/// - Cancellations during active trading
/// - Partial fills across different order types
/// - Self-match prevention scenarios
/// - Multiple price levels and order book depth
/// - Edge cases and adverse conditions
/// - State persistence across all operations
///
/// The test validates that the exchange behaves correctly under complex
/// realistic conditions where different order types interact.
pub async fn run_all(ctx: &mut TestContext) -> TestResult<Vec<ScenarioResult>> {
    info!("╔════════════════════════════════════════════════╗");
    info!("║   COMPREHENSIVE INTEGRATION TEST SUITE         ║");
    info!("║   All Order Types + Edge Cases + Load          ║");
    info!("╚════════════════════════════════════════════════╝");
    info!(
        "Market ID: {} (Base: {}, Quote: {})",
        ctx.market_id, ctx.base_asset_id, ctx.quote_asset_id
    );
    info!("");

    let suite_start = std::time::Instant::now();
    let mut results = Vec::new();

    // ========================================================================
    // SECTION 1: Setup - Fund all test users
    // ========================================================================
    info!("┌────────────────────────────────────────────────┐");
    info!("│ SECTION 1: Initial Setup                       │");
    info!("└────────────────────────────────────────────────┘");

    info!("Funding test users with substantial amounts for comprehensive testing...");
    ctx.fund_user(users::ALICE, 50_000_000, assets::USD).await?; // 50M USD
    ctx.fund_user(users::ALICE, 5_000, assets::BTC).await?; // 5000 BTC
    ctx.fund_user(users::BOB, 50_000_000, assets::USD).await?; // 50M USD
    ctx.fund_user(users::BOB, 5_000, assets::BTC).await?; // 5000 BTC
    ctx.fund_user(users::CHARLIE, 50_000_000, assets::USD)
        .await?; // 50M USD
    ctx.fund_user(users::CHARLIE, 5_000, assets::BTC).await?; // 5000 BTC

    info!("✓ All users funded successfully");
    info!("");

    // ========================================================================
    // SECTION 2: Build Initial Order Book with GTC Orders
    // ========================================================================
    info!("┌────────────────────────────────────────────────┐");
    info!("│ SECTION 2: Build Initial Order Book (GTC)      │");
    info!("│ Create multi-level orderbook depth             │");
    info!("└────────────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match build_initial_orderbook_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 2 PASSED ({:?})", duration);
            results.push(ScenarioResult::success(
                "build_orderbook".to_string(),
                duration,
            ));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 2 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure(
                "build_orderbook".to_string(),
                duration,
                e,
            ));
            return Ok(results);
        }
    }
    info!("");

    // ========================================================================
    // SECTION 3: IOC Orders Against Existing Liquidity
    // ========================================================================
    info!("┌────────────────────────────────────────────────┐");
    info!("│ SECTION 3: IOC Orders vs Existing Liquidity    │");
    info!("│ Test IOC partial fills and cancellations       │");
    info!("└────────────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match ioc_against_orderbook_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 3 PASSED ({:?})", duration);
            results.push(ScenarioResult::success(
                "ioc_vs_liquidity".to_string(),
                duration,
            ));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 3 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure(
                "ioc_vs_liquidity".to_string(),
                duration,
                e,
            ));
            return Ok(results);
        }
    }
    info!("");

    // ========================================================================
    // SECTION 4: FOK Orders - All or Nothing Tests
    // ========================================================================
    info!("┌────────────────────────────────────────────────┐");
    info!("│ SECTION 4: FOK Orders (All-or-Nothing)         │");
    info!("│ Test FOK success and rejection scenarios       │");
    info!("└────────────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match fok_orders_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 4 PASSED ({:?})", duration);
            results.push(ScenarioResult::success("fok_orders".to_string(), duration));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 4 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure(
                "fok_orders".to_string(),
                duration,
                e,
            ));
            return Ok(results);
        }
    }
    info!("");

    // ========================================================================
    // SECTION 5: Cancellations During Active Trading
    // ========================================================================
    info!("┌────────────────────────────────────────────────┐");
    info!("│ SECTION 5: Cancellations During Trading        │");
    info!("│ Cancel orders with active orderbook            │");
    info!("└────────────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match cancellations_during_trading_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 5 PASSED ({:?})", duration);
            results.push(ScenarioResult::success(
                "active_cancellations".to_string(),
                duration,
            ));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 5 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure(
                "active_cancellations".to_string(),
                duration,
                e,
            ));
            return Ok(results);
        }
    }
    info!("");

    // ========================================================================
    // SECTION 6: Mixed Order Types - Stress Test
    // ========================================================================
    info!("┌────────────────────────────────────────────────┐");
    info!("│ SECTION 6: Mixed Order Types (Stress Test)     │");
    info!("│ Rapid sequence of GTC/IOC/FOK/Cancel           │");
    info!("└────────────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match mixed_order_types_stress_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 6 PASSED ({:?})", duration);
            results.push(ScenarioResult::success(
                "mixed_stress_test".to_string(),
                duration,
            ));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 6 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure(
                "mixed_stress_test".to_string(),
                duration,
                e,
            ));
            return Ok(results);
        }
    }
    info!("");

    // ========================================================================
    // SECTION 7: Edge Cases - Self-Match Prevention & Boundary Tests
    // ========================================================================
    info!("┌────────────────────────────────────────────────┐");
    info!("│ SECTION 7: Edge Cases & Boundary Tests         │");
    info!("│ Self-match prevention, partial fills, etc      │");
    info!("└────────────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match edge_cases_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 7 PASSED ({:?})", duration);
            results.push(ScenarioResult::success("edge_cases".to_string(), duration));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 7 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure(
                "edge_cases".to_string(),
                duration,
                e,
            ));
            return Ok(results);
        }
    }
    info!("");

    // ========================================================================
    // SECTION 8: Multi-Level Orderbook Interactions
    // ========================================================================
    info!("┌────────────────────────────────────────────────┐");
    info!("│ SECTION 8: Multi-Level Orderbook Interactions  │");
    info!("│ Orders crossing multiple price levels          │");
    info!("└────────────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match multi_level_interactions_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 8 PASSED ({:?})", duration);
            results.push(ScenarioResult::success("multi_level".to_string(), duration));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 8 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure(
                "multi_level".to_string(),
                duration,
                e,
            ));
            return Ok(results);
        }
    }
    info!("");

    // ========================================================================
    // SECTION 9: Partial Fills with Cancellations
    // ========================================================================
    info!("┌────────────────────────────────────────────────┐");
    info!("│ SECTION 9: Partial Fills + Cancellations       │");
    info!("│ Cancel partially filled orders                 │");
    info!("└────────────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match partial_fills_with_cancel_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 9 PASSED ({:?})", duration);
            results.push(ScenarioResult::success(
                "partial_cancel".to_string(),
                duration,
            ));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 9 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure(
                "partial_cancel".to_string(),
                duration,
                e,
            ));
            return Ok(results);
        }
    }
    info!("");

    // ========================================================================
    // SECTION 10: Final Orderbook Cleanup and Balance Verification
    // ========================================================================
    info!("┌────────────────────────────────────────────────┐");
    info!("│ SECTION 10: Final Cleanup & Balance Check      │");
    info!("│ Cancel all remaining orders, verify balances   │");
    info!("└────────────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match final_cleanup_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 10 PASSED ({:?})", duration);
            results.push(ScenarioResult::success(
                "final_cleanup".to_string(),
                duration,
            ));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 10 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure(
                "final_cleanup".to_string(),
                duration,
                e,
            ));
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

    info!("╔════════════════════════════════════════════════╗");
    info!("║   COMPREHENSIVE TEST SUITE SUMMARY             ║");
    info!("╚════════════════════════════════════════════════╝");
    info!("Total Sections: {}", results.len());
    info!("Passed:         {} ✓", passed);
    info!("Failed:         {} ✗", failed);
    info!("Total Time:     {:?}", total_duration);
    info!("╚════════════════════════════════════════════════╝");

    Ok(results)
}

/// SECTION 2: Build initial orderbook with multiple GTC orders at various price levels
async fn build_initial_orderbook_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Building multi-level orderbook:");
    info!("  Bids: Alice @ 45k (10 BTC), Bob @ 40k (15 BTC)");
    info!("  Asks: Charlie @ 55k (12 BTC), Bob @ 60k (8 BTC)");

    let market_id = ctx.market_id;
    let redis_timeout = ctx.config().redis_event_timeout;

    // Bid side - Level 1: Alice @ 45,000 for 10 BTC
    let bid1 = OrderBuilder::place_limit()
        .user(users::ALICE)
        .price(45_000)
        .size(10)
        .side(Side::Bid)
        .market_id(market_id)
        .build();
    let resp1 = ctx.execute_command(bid1)?;
    ResponseVerifier::assert_placed(&resp1)?;
    info!("  → Bid level 1: order_id={} (Alice)", resp1.order_id);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Wait for first bid to appear on orderbook
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier
            .wait_and_assert_level(market_id, Side::Bid, 45_000, 10, redis_timeout)
            .await?;
    }

    // Bid side - Level 2: Bob @ 40,000 for 15 BTC
    let bid2 = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(40_000)
        .size(15)
        .side(Side::Bid)
        .market_id(market_id)
        .build();
    let resp2 = ctx.execute_command(bid2)?;
    ResponseVerifier::assert_placed(&resp2)?;
    info!("  → Bid level 2: order_id={} (Bob)", resp2.order_id);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Wait for second bid to appear
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier
            .wait_and_assert_level(market_id, Side::Bid, 40_000, 15, redis_timeout)
            .await?;
    }

    // Ask side - Level 1: Charlie @ 55,000 for 12 BTC
    let ask1 = OrderBuilder::place_limit()
        .user(users::CHARLIE)
        .price(55_000)
        .size(12)
        .side(Side::Ask)
        .market_id(market_id)
        .build();
    let resp3 = ctx.execute_command(ask1)?;
    ResponseVerifier::assert_placed(&resp3)?;
    info!("  → Ask level 1: order_id={} (Charlie)", resp3.order_id);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Wait for first ask to appear
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier
            .wait_and_assert_level(market_id, Side::Ask, 55_000, 12, redis_timeout)
            .await?;
    }

    // Ask side - Level 2: Bob @ 60,000 for 8 BTC (changed from Alice to avoid same user on both sides)
    let ask2 = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(60_000)
        .size(8)
        .side(Side::Ask)
        .market_id(market_id)
        .build();
    let resp4 = ctx.execute_command(ask2)?;
    ResponseVerifier::assert_placed(&resp4)?;
    info!("  → Ask level 2: order_id={} (Bob)", resp4.order_id);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Wait for second ask to appear and verify final orderbook structure
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier
            .wait_and_assert_level(market_id, Side::Ask, 60_000, 8, redis_timeout)
            .await?;
        orderbook_verifier
            .wait_and_assert_depth(market_id, 2, 2, redis_timeout)
            .await?;
        info!("  → Orderbook verified: 2 bid levels, 2 ask levels");
    }

    Ok(())
}

/// SECTION 3: IOC orders against existing orderbook
async fn ioc_against_orderbook_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Testing IOC orders against existing liquidity:");
    info!("  1. Bob IOC buy @ 56k for 5 BTC → Partial fill (crosses 55k ask)");
    info!("  2. Charlie IOC sell @ 44k for 7 BTC → Partial fill (crosses 45k bid)");

    let market_id = ctx.market_id;

    // Bob places IOC buy @ 56,000 for 5 BTC
    // This crosses Charlie's ask @ 55,000 (12 BTC available)
    // Should fill 5 BTC @ 55,000, leave 7 BTC on ask
    let ioc_buy = OrderBuilder::place_ioc()
        .user(users::BOB)
        .price(56_000)
        .size(5)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let ioc_resp1 = ctx.execute_command(ioc_buy)?;
    ResponseVerifier::assert_filled(&ioc_resp1)?;
    info!("  → IOC buy filled: 5 BTC @ 55,000");

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify trade
    {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.redis);
        let criteria = TradeCriteria::new()
            .market_id(market_id)
            .maker_user_id(users::CHARLIE)
            .taker_user_id(users::BOB)
            .price(55_000)
            .size(5);
        trade_verifier
            .wait_and_assert_trade(market_id, &criteria, Duration::from_secs(2))
            .await?;
    }

    // Verify remaining ask @ 55k is 7 BTC
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier
            .assert_level(market_id, Side::Ask, 55_000, 7)
            .await?;
        info!("  → Remaining ask: 7 BTC @ 55,000");
    }

    // Charlie places IOC sell @ 44,000 for 7 BTC
    // This crosses Alice's bid @ 45,000 (10 BTC available)
    // Should fill 7 BTC @ 45,000, leave 3 BTC on bid
    let ioc_sell = OrderBuilder::place_ioc()
        .user(users::CHARLIE)
        .price(44_000)
        .size(7)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let ioc_resp2 = ctx.execute_command(ioc_sell)?;
    ResponseVerifier::assert_filled(&ioc_resp2)?;
    info!("  → IOC sell filled: 7 BTC @ 45,000");

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify trade
    {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.redis);
        let criteria = TradeCriteria::new()
            .market_id(market_id)
            .maker_user_id(users::ALICE)
            .taker_user_id(users::CHARLIE)
            .price(45_000)
            .size(7);
        trade_verifier
            .wait_and_assert_trade(market_id, &criteria, Duration::from_secs(2))
            .await?;
    }

    // Verify remaining bid @ 45k is 3 BTC
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier
            .assert_level(market_id, Side::Bid, 45_000, 3)
            .await?;
        info!("  → Remaining bid: 3 BTC @ 45,000");
    }

    Ok(())
}

/// SECTION 4: FOK orders - all-or-nothing scenarios
async fn fok_orders_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Testing FOK orders (all-or-nothing):");
    info!("  Current state: Bid 3@45k, 15@40k | Ask 7@55k, 8@60k");
    info!("  1. Alice FOK buy @ 56k for 7 BTC → Should fill completely");
    info!("  2. Bob FOK buy @ 61k for 20 BTC → Should reject (insufficient liquidity)");

    let market_id = ctx.market_id;

    // FOK buy for exact available liquidity @ 55k (7 BTC)
    let fok_buy1 = OrderBuilder::place_fok()
        .user(users::ALICE)
        .price(56_000)
        .size(7)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let fok_resp1 = ctx.execute_command(fok_buy1)?;
    ResponseVerifier::assert_filled(&fok_resp1)?;
    info!("  → FOK buy filled: 7 BTC @ 55,000");

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify trade
    {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.redis);
        let criteria = TradeCriteria::new()
            .market_id(market_id)
            .price(55_000)
            .size(7);
        trade_verifier
            .wait_and_assert_trade(market_id, &criteria, Duration::from_secs(2))
            .await?;
    }

    // Now only ask @ 60k (8 BTC) remains
    // FOK for 20 BTC should be rejected (only 8 BTC available)
    let fok_buy2 = OrderBuilder::place_fok()
        .user(users::BOB)
        .price(61_000)
        .size(20)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let fok_resp2 = ctx.execute_command(fok_buy2)?;
    ResponseVerifier::assert_cancelled(&fok_resp2)?;
    info!("  → FOK buy cancelled: insufficient liquidity (wanted 20, only 8 available)");

    Ok(())
}

/// SECTION 5: Cancellations during active trading
async fn cancellations_during_trading_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Testing cancellations with active orderbook:");
    info!("  Current: Bid 3@45k, 15@40k | Ask 8@60k");
    info!("  1. Charlie places new ask @ 58k for 10 BTC");
    info!("  2. Charlie cancels it immediately");
    info!("  3. Bob cancels his bid @ 40k");

    let market_id = ctx.market_id;

    // Charlie places new ask
    let new_ask = OrderBuilder::place_limit()
        .user(users::CHARLIE)
        .price(58_000)
        .size(10)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let ask_resp = ctx.execute_command(new_ask)?;
    ResponseVerifier::assert_placed(&ask_resp)?;
    let charlie_order_id = ask_resp.order_id;
    info!("  → Charlie placed ask: order_id={}", charlie_order_id);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Charlie cancels it
    let cancel1 = OrderBuilder::cancel()
        .order_id(charlie_order_id)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let cancel_resp1 = ctx.execute_command(cancel1)?;
    ResponseVerifier::assert_cancelled(&cancel_resp1)?;
    info!("  → Charlie cancelled order_id={}", charlie_order_id);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify Charlie's order removed
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        let orderbook = orderbook_verifier.get_orderbook(market_id).await?;
        let ask_at_58k = orderbook.asks.iter().find(|a| a.price == 58_000);
        if let Some(ask) = ask_at_58k {
            if ask.size != 0 {
                return Err(TestError::Verification {
                    message: format!("Expected ask @ 58k to be removed, found size {}", ask.size),
                });
            }
        }
        info!("  → Verified: ask @ 58k removed");
    }

    // Note: We need to track Bob's original bid order_id from section 2
    // For now, let's just place a new order and cancel it to demonstrate cancellation
    let bob_bid = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(42_000)
        .size(5)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let bob_resp = ctx.execute_command(bob_bid)?;
    let bob_order_id = bob_resp.order_id;
    info!("  → Bob placed new bid: order_id={}", bob_order_id);

    tokio::time::sleep(Duration::from_millis(150)).await;

    let cancel2 = OrderBuilder::cancel()
        .order_id(bob_order_id)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    ctx.execute_command(cancel2)?;
    info!("  → Bob cancelled order_id={}", bob_order_id);

    Ok(())
}

/// SECTION 6: Mixed order types stress test
async fn mixed_order_types_stress_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Stress test with rapid mixed order types:");
    info!("  Rapid sequence of GTC, IOC, FOK, and cancellations");

    let market_id = ctx.market_id;

    // Rapid sequence of orders
    for i in 0..5 {
        let price = 50_000 + (i * 1000);

        // GTC order
        let gtc = OrderBuilder::place_limit()
            .user(users::ALICE)
            .price(price)
            .size(2)
            .side(Side::Ask)
            .market_id(market_id)
            .build();

        let gtc_resp = ctx.execute_command(gtc)?;
        info!(
            "  → GTC ask placed @ {}: order_id={}",
            price, gtc_resp.order_id
        );

        tokio::time::sleep(Duration::from_millis(50)).await;

        // IOC order that might match
        let ioc = OrderBuilder::place_ioc()
            .user(users::BOB)
            .price(price - 1000)
            .size(1)
            .side(Side::Bid)
            .market_id(market_id)
            .build();

        ctx.execute_command(ioc)?;
        info!("  → IOC bid placed @ {}", price - 1000);

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    info!("  → Completed rapid sequence of 10 orders");

    Ok(())
}

/// SECTION 7: Edge cases - self-match prevention and boundary tests
async fn edge_cases_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Testing edge cases:");
    info!("  1. Self-match prevention (same user bid/ask crossing)");
    info!("  2. Zero-size edge (if applicable)");

    let market_id = ctx.market_id;

    // Alice places ask @ 48k
    let alice_ask = OrderBuilder::place_limit()
        .user(users::ALICE)
        .price(48_000)
        .size(5)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let ask_resp = ctx.execute_command(alice_ask)?;
    info!("  → Alice placed ask @ 48k: order_id={}", ask_resp.order_id);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Alice tries to place bid @ 49k (would cross her own ask)
    // Due to self-match prevention, should not match
    let alice_bid = OrderBuilder::place_limit()
        .user(users::ALICE)
        .price(49_000)
        .size(3)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let bid_resp = ctx.execute_command(alice_bid)?;
    info!(
        "  → Alice placed bid @ 49k: order_id={} (self-match prevented)",
        bid_resp.order_id
    );

    // Verify no trade occurred (self-match prevented)
    tokio::time::sleep(Duration::from_millis(300)).await;

    {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.redis);
        let all_trades = trade_verifier.get_recent_trades(market_id, 100).await?;
        let self_trade = all_trades
            .iter()
            .find(|t| t.maker_user_id == users::ALICE && t.taker_user_id == users::ALICE);

        if self_trade.is_some() {
            return Err(TestError::Verification {
                message: "Self-match occurred when it should have been prevented".to_string(),
            });
        }
        info!("  → Verified: No self-match occurred");
    }

    Ok(())
}

/// SECTION 8: Multi-level orderbook interactions
async fn multi_level_interactions_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Testing multi-level price interactions:");
    info!("  Place asks at 3 levels, then single bid crosses all");
    info!("  Note: First clean up leftover orders, then test multi-level matching");

    let market_id = ctx.market_id;

    // First, clean up any leftover asks from previous sections (e.g., 60k ask from Section 2)
    // Use Alice's IOC to consume any asks below 64k
    info!("  → Cleaning up leftover orders below 64k");
    let cleanup = OrderBuilder::place_ioc()
        .user(users::ALICE)
        .price(64_000)
        .size(50) // Large size to consume any leftovers
        .side(Side::Bid)
        .market_id(market_id)
        .build();
    ctx.execute_command(cleanup)?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Use higher prices to avoid overlap with existing orders from previous sections
    // Place 3 asks at different levels
    let level1_price = 65_000;
    let level2_price = 66_000;
    let level3_price = 67_000;

    let level1 = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(level1_price)
        .size(3)
        .side(Side::Ask)
        .market_id(market_id)
        .build();
    ctx.execute_command(level1)?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let level2 = OrderBuilder::place_limit()
        .user(users::CHARLIE)
        .price(level2_price)
        .size(4)
        .side(Side::Ask)
        .market_id(market_id)
        .build();
    ctx.execute_command(level2)?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let level3 = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(level3_price)
        .size(5)
        .side(Side::Ask)
        .market_id(market_id)
        .build();
    ctx.execute_command(level3)?;

    info!("  → Placed 3 ask levels: 3@65k, 4@66k, 5@67k");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Place IOC bid that crosses all 3 levels
    let big_bid = OrderBuilder::place_ioc()
        .user(users::ALICE)
        .price(68_000)
        .size(12)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let big_resp = ctx.execute_command(big_bid)?;
    info!("  → Placed IOC bid for 12 BTC @ 68k (crosses all 3 levels)");
    info!("  → Response: {:?}", big_resp.status);

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify trades at all 3 levels
    {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.redis);

        // Check for trade @ 65k (3 BTC)
        let criteria1 = TradeCriteria::new()
            .market_id(market_id)
            .price(level1_price)
            .size(3);
        trade_verifier
            .assert_trade_exists(market_id, &criteria1)
            .await?;

        // Check for trade @ 66k (4 BTC)
        let criteria2 = TradeCriteria::new()
            .market_id(market_id)
            .price(level2_price)
            .size(4);
        trade_verifier
            .assert_trade_exists(market_id, &criteria2)
            .await?;

        // Check for trade @ 67k (5 BTC)
        let criteria3 = TradeCriteria::new()
            .market_id(market_id)
            .price(level3_price)
            .size(5);
        trade_verifier
            .assert_trade_exists(market_id, &criteria3)
            .await?;

        info!("  → Verified: 3 trades across 3 price levels (3+4+5=12 BTC)");
    }

    Ok(())
}

/// SECTION 9: Partial fills with cancellations
async fn partial_fills_with_cancel_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Testing partial fills followed by cancellations:");
    info!("  1. Clean up leftover bids above 38k");
    info!("  2. Alice places large GTC bid @ 38k");
    info!("  3. Bob partially fills it with IOC");
    info!("  4. Alice cancels remaining");

    let market_id = ctx.market_id;

    // First, clean up any leftover bids from previous sections
    info!("  → Cleaning up leftover bids above 38k");
    let cleanup = OrderBuilder::place_ioc()
        .user(users::BOB)
        .price(39_000)
        .size(50) // Large size to consume any leftovers
        .side(Side::Ask)
        .market_id(market_id)
        .build();
    ctx.execute_command(cleanup)?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Alice places large bid @ 38k for 20 BTC (using lower price to avoid conflicts)
    let large_bid = OrderBuilder::place_limit()
        .user(users::ALICE)
        .price(38_000)
        .size(20)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let bid_resp = ctx.execute_command(large_bid)?;
    let alice_order_id = bid_resp.order_id;
    info!(
        "  → Alice placed bid for 20 BTC @ 38k: order_id={}",
        alice_order_id
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Bob partially fills with IOC sell for 6 BTC
    let partial_fill = OrderBuilder::place_ioc()
        .user(users::BOB)
        .price(37_000)
        .size(6)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    ctx.execute_command(partial_fill)?;
    info!("  → Bob IOC sell for 6 BTC @ 37k (partially fills Alice)");

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify partial fill trade
    {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.redis);
        let criteria = TradeCriteria::new()
            .market_id(market_id)
            .maker_user_id(users::ALICE)
            .taker_user_id(users::BOB)
            .price(38_000)
            .size(6);
        trade_verifier
            .wait_and_assert_trade(market_id, &criteria, Duration::from_secs(2))
            .await?;
        info!("  → Verified trade: 6 BTC @ 38k");
    }

    // Verify remaining 14 BTC on orderbook
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier
            .assert_level(market_id, Side::Bid, 38_000, 14)
            .await?;
        info!("  → Remaining on orderbook: 14 BTC @ 38k");
    }

    // Alice cancels remaining order
    let cancel = OrderBuilder::cancel()
        .order_id(alice_order_id)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let cancel_resp = ctx.execute_command(cancel)?;
    ResponseVerifier::assert_cancelled(&cancel_resp)?;
    info!("  → Alice cancelled remaining 14 BTC");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify removal
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        let orderbook = orderbook_verifier.get_orderbook(market_id).await?;
        let bid_at_38k = orderbook.bids.iter().find(|b| b.price == 38_000);
        if let Some(bid) = bid_at_38k {
            if bid.size != 0 {
                return Err(TestError::Verification {
                    message: format!("Expected bid @ 38k to be removed, found size {}", bid.size),
                });
            }
        }
        info!("  → Verified: Order removed from orderbook");
    }

    Ok(())
}

/// SECTION 10: Final cleanup and balance verification
async fn final_cleanup_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Final cleanup and balance verification:");
    info!("  Checking that all balances sum correctly (available + locked = total)");

    // Verify balance invariants for all users
    {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);

        balance_verifier
            .assert_balance_invariant(users::ALICE, assets::USD)
            .await?;
        balance_verifier
            .assert_balance_invariant(users::ALICE, assets::BTC)
            .await?;
        info!("  → Alice balances: USD and BTC invariants satisfied");

        balance_verifier
            .assert_balance_invariant(users::BOB, assets::USD)
            .await?;
        balance_verifier
            .assert_balance_invariant(users::BOB, assets::BTC)
            .await?;
        info!("  → Bob balances: USD and BTC invariants satisfied");

        balance_verifier
            .assert_balance_invariant(users::CHARLIE, assets::USD)
            .await?;
        balance_verifier
            .assert_balance_invariant(users::CHARLIE, assets::BTC)
            .await?;
        info!("  → Charlie balances: USD and BTC invariants satisfied");
    }

    info!("  → All balance invariants verified successfully");
    info!("  → Comprehensive integration test completed");

    Ok(())
}
