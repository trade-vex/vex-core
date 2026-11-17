//! Balance management test scenarios
//!
//! This module contains test scenarios for deposit and withdrawal operations.

use crate::builders::OrderBuilder;
use crate::fixtures::{assets, users};
use crate::test_framework::types::*;
use crate::test_framework::{TestContext, scenario};
use crate::verifiers::{BalanceVerifier, ResponseVerifier};
use common::Status;
use tracing::info;

/// Test single deposit operation
///
/// Verifies:
/// - Response status is Processed
/// - Balance is published to Kafka with correct values
/// - Balance invariant holds (available + locked = total)
pub async fn test_single_deposit(ctx: &mut TestContext) -> TestResult<()> {
    info!("Running test: single_deposit");

    let user_id = users::ALICE;
    let asset_id = assets::USD;
    let amount = 1_000_000u64; // 1M USD

    // Execute deposit
    let deposit_cmd = OrderBuilder::deposit()
        .user(user_id)
        .amount(amount)
        .asset(asset_id)
        .build();

    let response = ctx.execute_command(deposit_cmd)?;

    // Phase 1: Verify Response
    ResponseVerifier::assert_status(&response, Status::Processed)?;

    // Phase 2: Verify Kafka events
    let event_timeout = ctx.config().event_timeout;
    ctx.kafka
        .wait_for_balance_after(user_id, asset_id, response.timestamp, event_timeout)
        .await?;
    let mut balance_verifier = BalanceVerifier::new(&mut ctx.kafka);

    // Verify balance values
    balance_verifier
        .assert_total_eq(user_id, asset_id, amount)
        .await?;
    balance_verifier
        .assert_available_eq(user_id, asset_id, amount)
        .await?;
    balance_verifier
        .assert_locked_eq(user_id, asset_id, 0)
        .await?;

    // Verify invariant
    balance_verifier
        .assert_balance_invariant(user_id, asset_id)
        .await?;

    info!("Test passed: single_deposit");
    Ok(())
}

/// Test multiple deposits to the same asset
///
/// Verifies:
/// - Balances accumulate correctly
/// - Each deposit is processed independently
pub async fn test_multiple_deposits_same_asset(ctx: &mut TestContext) -> TestResult<()> {
    info!("Running test: multiple_deposits_same_asset");

    let user_id = users::BOB;
    let asset_id = assets::USD;
    let deposit1 = 500_000u64;
    let deposit2 = 300_000u64;
    let deposit3 = 200_000u64;
    let expected_total = deposit1 + deposit2 + deposit3;

    // First deposit
    let cmd1 = OrderBuilder::deposit()
        .user(user_id)
        .amount(deposit1)
        .asset(asset_id)
        .build();

    let response1 = ctx.execute_command(cmd1)?;
    ResponseVerifier::assert_status(&response1, Status::Processed)?;

    // Wait for first deposit
    let event_timeout = ctx.config().event_timeout;
    {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.kafka);
        balance_verifier
            .wait_and_assert_balance(
                user_id,
                asset_id,
                Balance::from_total(deposit1),
                response1.timestamp,
                event_timeout,
            )
            .await?;
    }

    // Second deposit
    let cmd2 = OrderBuilder::deposit()
        .user(user_id)
        .amount(deposit2)
        .asset(asset_id)
        .build();

    let response2 = ctx.execute_command(cmd2)?;
    ResponseVerifier::assert_status(&response2, Status::Processed)?;

    // Third deposit
    let cmd3 = OrderBuilder::deposit()
        .user(user_id)
        .amount(deposit3)
        .asset(asset_id)
        .build();

    let response3 = ctx.execute_command(cmd3)?;
    ResponseVerifier::assert_status(&response3, Status::Processed)?;

    // Wait and verify final total
    ctx.kafka
        .wait_for_balance_after(user_id, asset_id, response3.timestamp, event_timeout)
        .await?;
    {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.kafka);
        balance_verifier
            .assert_total_eq(user_id, asset_id, expected_total)
            .await?;
        balance_verifier
            .assert_balance_invariant(user_id, asset_id)
            .await?;
    }

    info!("Test passed: multiple_deposits_same_asset");
    Ok(())
}

/// Test deposits to different assets
///
/// Verifies:
/// - Assets are tracked independently
/// - No cross-asset balance contamination
pub async fn test_deposits_different_assets(ctx: &mut TestContext) -> TestResult<()> {
    info!("Running test: deposits_different_assets");
    let timeout = ctx.config().event_timeout;
    let user_id = users::CHARLIE;
    let usd_amount = 1_000_000u64;
    let btc_amount = 100u64;

    // Deposit USD
    let usd_cmd = OrderBuilder::deposit()
        .user(user_id)
        .amount(usd_amount)
        .asset(assets::USD)
        .build();

    let usd_response = ctx.execute_command(usd_cmd)?;
    ResponseVerifier::assert_status(&usd_response, Status::Processed)?;

    // Deposit BTC
    let btc_cmd = OrderBuilder::deposit()
        .user(user_id)
        .amount(btc_amount)
        .asset(assets::BTC)
        .build();

    let btc_response = ctx.execute_command(btc_cmd)?;
    ResponseVerifier::assert_status(&btc_response, Status::Processed)?;

    // Verify both balances
    ctx.kafka
        .wait_for_balance_after(user_id, assets::USD, usd_response.timestamp, timeout)
        .await?;
    ctx.kafka
        .wait_for_balance_after(user_id, assets::BTC, btc_response.timestamp, timeout)
        .await?;
    let mut balance_verifier = BalanceVerifier::new(&mut ctx.kafka);
    balance_verifier
        .assert_total_eq(user_id, assets::USD, usd_amount)
        .await?;
    balance_verifier
        .assert_total_eq(user_id, assets::BTC, btc_amount)
        .await?;

    // Verify both invariants
    balance_verifier
        .assert_balance_invariant(user_id, assets::USD)
        .await?;
    balance_verifier
        .assert_balance_invariant(user_id, assets::BTC)
        .await?;

    info!("Test passed: deposits_different_assets");
    Ok(())
}

/// Run all balance test scenarios
pub async fn run_all(ctx: &mut TestContext) -> TestResult<Vec<ScenarioResult>> {
    let mut results = Vec::new();
    scenario!(results, "single_deposit", test_single_deposit(ctx));
    scenario!(
        results,
        "multiple_deposits_same_asset",
        test_multiple_deposits_same_asset(ctx)
    );
    scenario!(
        results,
        "deposits_different_assets",
        test_deposits_different_assets(ctx)
    );
    Ok(results)
}
