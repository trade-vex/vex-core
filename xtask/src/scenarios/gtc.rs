//! GTC (Good-Till-Canceled) order test scenarios
//!
//! This module contains test scenarios for GTC limit orders.
//! All tests run in a single session without restarting the server.

use std::time::Duration;

use crate::builders::OrderBuilder;
use crate::fixtures::{assets, prices, users};
use crate::test_framework::TestContext;
use crate::test_framework::types::*;
use crate::verifiers::{BalanceVerifier, OrderbookVerifier, ResponseVerifier, TradeVerifier};
use common::Side;
use tracing::{info, warn};

/// Test GTC limit order that doesn't match (rests on book)
///
/// Setup: Empty orderbook
/// Execute: Place bid below market or ask above market
/// Verify:
/// - Response: status=Placed, order_id assigned, size unchanged
/// - Redis: order exists, orderbook updated, funds locked
pub async fn test_gtc_no_match(ctx: &mut TestContext) -> TestResult<()> {
    info!("Running test: gtc_no_match");

    let user_id = users::ALICE;
    let market_id = ctx.market_id;
    let price = prices::LOW; // 40,000
    let size = 10;

    // Fund user with quote currency (USD)
    ctx.fund_user(user_id, 1_000_000, assets::USD).await?;

    // Place limit bid order
    let order_cmd = OrderBuilder::place_limit()
        .user(user_id)
        .price(price)
        .size(size)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let original_size = order_cmd.size;
    let response = ctx.execute_command(order_cmd)?;

    // Phase 1: Verify Response
    ResponseVerifier::assert_gtc_placed_no_match(
        &response,
        user_id,
        market_id,
        price,
        original_size,
    )?;

    // Phase 2: Verify Redis
    let locked_amount = price * size;
    let redis_timeout = ctx.config().redis_event_timeout;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Verify funds locked (price * size for bid)
    {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
        balance_verifier
            .assert_locked_eq(user_id, assets::USD, locked_amount)
            .await?;
        balance_verifier
            .assert_balance_invariant(user_id, assets::USD)
            .await?;
    }

    // Verify orderbook updated
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier
            .wait_and_assert_depth(market_id, 1, 0, redis_timeout)
            .await?;
        orderbook_verifier
            .assert_level(market_id, Side::Bid, price, size)
            .await?;
    }

    info!("Test passed: gtc_no_match");
    Ok(())
}

/// Test GTC limit order that fully matches
///
/// Setup: Pre-place opposing order
/// Execute: Place order that crosses
/// Verify:
/// - Response: status=Filled, size=0
/// - Redis: trade exists, balances updated, no resting order
pub async fn test_gtc_full_match(ctx: &mut TestContext) -> TestResult<()> {
    info!("Running test: gtc_full_match");

    let maker_id = users::ALICE;
    let taker_id = users::BOB;
    let market_id = ctx.market_id;
    let price = prices::MID; // 50,000
    let size = 5;

    // Fund both users
    // Maker places Ask (selling BTC) -> needs BTC
    // Taker places Bid (buying BTC) -> needs USD
    ctx.fund_user(maker_id, 1_000, assets::BTC).await?; // Base for ask
    ctx.fund_user(taker_id, 1_000_000, assets::USD).await?; // Quote for bid

    // Setup: Maker places ask order
    let maker_order = OrderBuilder::place_limit()
        .user(maker_id)
        .price(price)
        .size(size)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let maker_response = ctx.execute_command(maker_order)?;
    ResponseVerifier::assert_placed(&maker_response)?;

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Execute: Taker places matching bid
    let taker_order = OrderBuilder::place_limit()
        .user(taker_id)
        .price(price)
        .size(size)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let taker_response = ctx.execute_command(taker_order)?;

    // Phase 1: Verify Response
    ResponseVerifier::assert_filled(&taker_response)?;

    // Phase 2: Verify Redis
    let mut trade_verifier = TradeVerifier::new(&mut ctx.redis);

    // Wait for trade event
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let criteria = TradeCriteria::new()
        .market_id(market_id)
        .maker_user_id(maker_id)
        .taker_user_id(taker_id)
        .price(price)
        .size(size);

    trade_verifier
        .assert_trade_exists(market_id, &criteria)
        .await?;

    // Verify orderbook is now empty
    let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
    orderbook_verifier.assert_empty(market_id).await?;

    info!("Test passed: gtc_full_match");
    Ok(())
}

/// Test GTC limit order that partially matches
///
/// Setup: Pre-place smaller opposing order
/// Execute: Place larger order
/// Verify:
/// - Response: status=PartiallyFilled, size=remaining
/// - Redis: trade exists, partial amount rests on book
pub async fn test_gtc_partial_match(ctx: &mut TestContext) -> TestResult<()> {
    info!("Running test: gtc_partial_match");

    let maker_id = users::ALICE;
    let taker_id = users::BOB;
    let market_id = ctx.market_id;
    let price = prices::MID;
    let maker_size = 3;
    let taker_size = 10;
    let expected_remaining = taker_size - maker_size;

    // Fund both users
    // Maker places Ask (selling BTC) -> needs BTC
    // Taker places Bid (buying BTC) -> needs USD
    ctx.fund_user(maker_id, 1_000, assets::BTC).await?; // Base for ask
    ctx.fund_user(taker_id, 1_000_000, assets::USD).await?; // Quote for bid

    // Setup: Maker places smaller ask
    let maker_order = OrderBuilder::place_limit()
        .user(maker_id)
        .price(price)
        .size(maker_size)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    ctx.execute_command(maker_order)?;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Execute: Taker places larger bid
    let taker_order = OrderBuilder::place_limit()
        .user(taker_id)
        .price(price)
        .size(taker_size)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let taker_response = ctx.execute_command(taker_order)?;

    // Phase 1: Verify Response
    ResponseVerifier::assert_partially_filled(&taker_response, taker_size)?;
    ResponseVerifier::assert_size(&taker_response, expected_remaining)?;

    // Phase 2: Verify Redis
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Verify trade occurred for matched portion
    let mut trade_verifier = TradeVerifier::new(&mut ctx.redis);
    let criteria = TradeCriteria::new().market_id(market_id).size(maker_size); // Only maker_size was matched

    trade_verifier
        .assert_trade_exists(market_id, &criteria)
        .await?;

    // Verify remaining size rests on book
    let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
    orderbook_verifier
        .assert_level(market_id, Side::Bid, price, expected_remaining)
        .await?;

    info!("Test passed: gtc_partial_match");
    Ok(())
}

/// Comprehensive GTC test suite - runs all scenarios in a single session
///
/// This is a single massive test that validates all GTC order behaviors:
/// 1. Orders that rest on the book (no match)
/// 2. Orders that fully match
/// 3. Orders that partially match
///
/// State is maintained across sections - no cleanup between tests since
/// the VexCore server holds orderbook and balances in memory.
pub async fn run_all(ctx: &mut TestContext) -> TestResult<Vec<ScenarioResult>> {
    info!("╔════════════════════════════════════════╗");
    info!("║   GTC COMPREHENSIVE TEST SUITE         ║");
    info!("╚════════════════════════════════════════╝");
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
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 1: Initial Setup                │");
    info!("└─────────────────────────────────────────┘");

    info!("Funding test users...");
    ctx.fund_user(users::ALICE, 10_000_000, assets::USD).await?; // 10M USD for Alice
    ctx.fund_user(users::ALICE, 1_000, assets::BTC).await?; // 1000 BTC for Alice
    ctx.fund_user(users::BOB, 10_000_000, assets::USD).await?; // 10M USD for Bob
    ctx.fund_user(users::BOB, 1_000, assets::BTC).await?; // 1000 BTC for Bob
    ctx.fund_user(users::CHARLIE, 10_000_000, assets::USD)
        .await?; // 10M USD for Charlie
    ctx.fund_user(users::CHARLIE, 1_000, assets::BTC).await?; // 1000 BTC for Charlie

    info!("✓ All users funded successfully");
    info!("");

    // ========================================================================
    // SECTION 2: GTC No Match - Orders rest on book
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 2: GTC No Match                 │");
    info!("│ (Orders rest on book)                   │");
    info!("└─────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match test_gtc_no_match_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 2 PASSED ({:?})", duration);
            results.push(ScenarioResult::success(
                "gtc_no_match".to_string(),
                duration,
            ));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 2 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure(
                "gtc_no_match".to_string(),
                duration,
                e,
            ));
            return Ok(results); // Stop on first failure
        }
    }
    info!("");

    // ========================================================================
    // SECTION 3: GTC Full Match - Complete fills
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 3: GTC Full Match               │");
    info!("│ (Orders fully matched)                  │");
    info!("└─────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match test_gtc_full_match_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 3 PASSED ({:?})", duration);
            results.push(ScenarioResult::success(
                "gtc_full_match".to_string(),
                duration,
            ));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 3 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure(
                "gtc_full_match".to_string(),
                duration,
                e,
            ));
            return Ok(results); // Stop on first failure
        }
    }
    info!("");

    // ========================================================================
    // SECTION 4: GTC Partial Match - Partial fills
    // ========================================================================
    info!("┌─────────────────────────────────────────┐");
    info!("│ SECTION 4: GTC Partial Match           │");
    info!("│ (Partial fill + rest on book)          │");
    info!("└─────────────────────────────────────────┘");

    let section_start = std::time::Instant::now();

    match test_gtc_partial_match_section(ctx).await {
        Ok(_) => {
            let duration = section_start.elapsed();
            info!("✓ SECTION 4 PASSED ({:?})", duration);
            results.push(ScenarioResult::success(
                "gtc_partial_match".to_string(),
                duration,
            ));
        }
        Err(e) => {
            let duration = section_start.elapsed();
            warn!("✗ SECTION 4 FAILED ({:?}): {}", duration, e);
            results.push(ScenarioResult::failure(
                "gtc_partial_match".to_string(),
                duration,
                e,
            ));
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

/// SECTION 2: Test GTC orders that don't match (rest on book)
async fn test_gtc_no_match_section(ctx: &mut TestContext) -> TestResult<()> {
    info!(
        "Alice places bid @ {} for 10 BTC (below market)",
        prices::LOW
    );

    let market_id = ctx.market_id;
    let price = prices::LOW; // 40,000
    let size = 10;

    // Alice places limit bid order
    let order_cmd = OrderBuilder::place_limit()
        .user(users::ALICE)
        .price(price)
        .size(size)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let original_size = order_cmd.size;
    let response = ctx.execute_command(order_cmd)?;

    // Verify Response
    ResponseVerifier::assert_gtc_placed_no_match(
        &response,
        users::ALICE,
        market_id,
        price,
        original_size,
    )?;
    info!("  → Order placed: order_id={}", response.order_id);

    // Verify Redis
    let locked_amount = price * size;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.redis);
        balance_verifier
            .assert_locked_eq(users::ALICE, assets::USD, locked_amount)
            .await?;
        info!("  → Funds locked: {} USD", locked_amount);
    }

    let redis_timeout = ctx.config().redis_event_timeout;
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier
            .wait_and_assert_depth(market_id, 1, 0, redis_timeout)
            .await?;
        orderbook_verifier
            .assert_level(market_id, Side::Bid, price, size)
            .await?;
        info!("  → Orderbook updated: 1 bid @ {}", price);
    }

    Ok(())
}

/// SECTION 3: Test GTC orders that fully match
async fn test_gtc_full_match_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Bob places ask @ {} for 5 BTC", prices::MID);
    info!(
        "Charlie places bid @ {} for 5 BTC → Full match",
        prices::MID
    );

    let market_id = ctx.market_id;
    let price = prices::MID; // 50,000
    let size = 5;

    // Bob (maker) places ask order
    let maker_order = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(price)
        .size(size)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let maker_response = ctx.execute_command(maker_order)?;
    ResponseVerifier::assert_placed(&maker_response)?;
    info!("  → Bob's ask placed: order_id={}", maker_response.order_id);

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Charlie (taker) places matching bid
    let taker_order = OrderBuilder::place_limit()
        .user(users::CHARLIE)
        .price(price)
        .size(size)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let taker_response = ctx.execute_command(taker_order)?;
    ResponseVerifier::assert_filled(&taker_response)?;
    info!(
        "  → Charlie's bid filled: order_id={}",
        taker_response.order_id
    );

    // Verify trade in Redis
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let criteria = TradeCriteria::new()
        .market_id(market_id)
        .maker_user_id(users::BOB)
        .taker_user_id(users::CHARLIE)
        .price(price)
        .size(size)
        .maker_order_id(maker_response.order_id)
        .taker_order_id(taker_response.order_id);

    let mut trade_verifier = TradeVerifier::new(&mut ctx.redis);
    trade_verifier
        .wait_and_assert_trade(market_id, &criteria, Duration::from_secs(2))
        .await?;
    info!("  → Trade executed: {} BTC @ {}", size, price);

    Ok(())
}

/// SECTION 4: Test GTC orders that partially match
async fn test_gtc_partial_match_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Alice places ask @ {} for 3 BTC", prices::MID);
    info!(
        "Bob places bid @ {} for 10 BTC → Partial match (3 filled, 7 rest)",
        prices::MID
    );

    let market_id = ctx.market_id;
    let price = prices::MID;
    let maker_size = 3;
    let taker_size = 10;
    let expected_remaining = taker_size - maker_size;

    // Alice (maker) places smaller ask
    let maker_order = OrderBuilder::place_limit()
        .user(users::ALICE)
        .price(price)
        .size(maker_size)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    ctx.execute_command(maker_order)?;
    info!("  → Alice's ask placed for {} BTC", maker_size);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Bob (taker) places larger bid
    let taker_order = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(price)
        .size(taker_size)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let taker_response = ctx.execute_command(taker_order)?;
    ResponseVerifier::assert_partially_filled(&taker_response, taker_size)?;
    ResponseVerifier::assert_size(&taker_response, expected_remaining)?;
    info!(
        "  → Bob's bid partially filled: {} BTC matched, {} BTC resting",
        maker_size, expected_remaining
    );

    // Verify trade and remaining order
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.redis);
        let criteria = TradeCriteria::new().market_id(market_id).size(maker_size);
        trade_verifier
            .assert_trade_exists(market_id, &criteria)
            .await?;
        info!("  → Trade executed for matched portion: {} BTC", maker_size);
    }

    {
        // Wait for orderbook to update with remaining order
        let redis_timeout = ctx.config().redis_event_timeout;
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.redis);
        orderbook_verifier
            .wait_and_assert_level(
                market_id,
                Side::Bid,
                price,
                expected_remaining,
                redis_timeout,
            )
            .await?;
        info!(
            "  → Remaining {} BTC rests on orderbook @ {}",
            expected_remaining, price
        );
    }

    Ok(())
}
