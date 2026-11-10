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

    // Verify Kafka events: a bid locks its quote cost at the market's native scales
    let locked_amount = ctx
        .market_spec()
        .calculate_quote_cost(price, size)
        .expect("scenario notional fits in u64");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.kafka);
        balance_verifier
            .assert_locked_eq(users::ALICE, assets::USD, locked_amount)
            .await?;
        info!("  → Funds locked: {} USD", locked_amount);
    }

    let event_timeout = ctx.config().event_timeout;
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.kafka);
        orderbook_verifier
            .wait_and_assert_depth(market_id, 1, 0, event_timeout)
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

    // Verify trade on Kafka
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let criteria = TradeCriteria::new()
        .market_id(market_id)
        .maker_user_id(users::BOB)
        .taker_user_id(users::CHARLIE)
        .price(price)
        .size(size)
        .maker_order_id(maker_response.order_id)
        .taker_order_id(taker_response.order_id);

    let mut trade_verifier = TradeVerifier::new(&mut ctx.kafka);
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
        let mut trade_verifier = TradeVerifier::new(&mut ctx.kafka);
        let criteria = TradeCriteria::new().market_id(market_id).size(maker_size);
        trade_verifier
            .assert_trade_exists(market_id, &criteria)
            .await?;
        info!("  → Trade executed for matched portion: {} BTC", maker_size);
    }

    {
        // Wait for orderbook to update with remaining order
        let event_timeout = ctx.config().event_timeout;
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.kafka);
        orderbook_verifier
            .wait_and_assert_level(
                market_id,
                Side::Bid,
                price,
                expected_remaining,
                event_timeout,
            )
            .await?;
        info!(
            "  → Remaining {} BTC rests on orderbook @ {}",
            expected_remaining, price
        );
    }

    Ok(())
}
