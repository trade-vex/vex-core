//! IOC (Immediate-Or-Cancel) order test scenarios
//!
//! This module contains test scenarios for IOC limit orders.
//! IOC orders execute immediately against available liquidity and cancel any unfilled portion.
//! All tests run in a single session without restarting the server.

use crate::builders::OrderBuilder;
use crate::fixtures::{assets, prices, users};
use crate::test_framework::types::*;
use crate::test_framework::{TestContext, scenario};
use crate::verifiers::{BalanceVerifier, OrderbookVerifier, ResponseVerifier, TradeVerifier};
use common::Side;
use tracing::info;

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
    info!("Funding test users...");
    ctx.fund_user(users::ALICE, 10_000_000, assets::USD).await?; // 10M USD
    ctx.fund_user(users::ALICE, 1_000, assets::BTC).await?; // 1000 BTC
    ctx.fund_user(users::BOB, 10_000_000, assets::USD).await?; // 10M USD
    ctx.fund_user(users::BOB, 1_000, assets::BTC).await?; // 1000 BTC
    ctx.fund_user(users::CHARLIE, 10_000_000, assets::USD)
        .await?; // 10M USD
    ctx.fund_user(users::CHARLIE, 1_000, assets::BTC).await?; // 1000 BTC

    info!("✓ All users funded successfully");

    let mut results = Vec::new();
    scenario!(results, "ioc_no_match", test_ioc_no_match_section(ctx));
    scenario!(results, "ioc_full_match", test_ioc_full_match_section(ctx));
    scenario!(
        results,
        "ioc_partial_match",
        test_ioc_partial_match_section(ctx)
    );
    scenario!(
        results,
        "ioc_multiple_levels",
        test_ioc_multiple_levels_section(ctx)
    );
    scenario!(results, "market_buy", test_market_buy_section(ctx));
    scenario!(results, "market_sell", test_market_sell_section(ctx));
    Ok(results)
}

/// SECTION 2: Test IOC order with no matching liquidity (fully cancelled)
async fn test_ioc_no_match_section(ctx: &mut TestContext) -> TestResult<()> {
    info!(
        "Alice places IOC bid @ {} for 5 BTC (no liquidity)",
        prices::LOW
    );

    let market_id = ctx.market_id;
    let event_timeout = ctx.config().event_timeout;
    let price = prices::LOW; // 40,000 - no asks at this price
    let size = 5;

    // Record initial balance
    let initial_balance = {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.kafka);
        balance_verifier
            .get_balance(users::ALICE, assets::USD)
            .await?
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
    info!(
        "  → Order cancelled: order_id={} (no liquidity)",
        response.order_id
    );

    // Verify Kafka events
    ctx.kafka
        .wait_for_balance_after(users::ALICE, assets::USD, response.timestamp, event_timeout)
        .await?;
    ctx.kafka
        .wait_for_orderbook_after(market_id, response.timestamp, event_timeout)
        .await?;

    {
        // Verify no funds were locked (order was immediately cancelled)
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.kafka);
        balance_verifier
            .assert_locked_eq(users::ALICE, assets::USD, 0)
            .await?;

        // Verify total balance unchanged
        let current_balance = balance_verifier
            .get_balance(users::ALICE, assets::USD)
            .await?;
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
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.kafka);
        orderbook_verifier.assert_empty(market_id).await?;
        info!("  → Orderbook remains empty (IOC doesn't rest)");
    }

    Ok(())
}

/// SECTION 3: Test IOC order that fully matches
async fn test_ioc_full_match_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Bob places GTC ask @ {} for 8 BTC", prices::MID);
    info!(
        "Alice places IOC bid @ {} for 8 BTC → Full match",
        prices::MID
    );

    let market_id = ctx.market_id;
    let event_timeout = ctx.config().event_timeout;
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
    info!(
        "  → Bob's GTC ask placed: order_id={}",
        maker_response.order_id
    );

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
    info!(
        "  → Alice's IOC bid filled: order_id={}",
        taker_response.order_id
    );

    // Verify trade on Kafka
    {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.kafka);
        let criteria = TradeCriteria::new()
            .market_id(market_id)
            .maker_user_id(users::BOB)
            .taker_user_id(users::ALICE)
            .price(price)
            .size(size)
            .maker_order_id(maker_response.order_id)
            .taker_order_id(taker_response.order_id);

        trade_verifier
            .wait_and_assert_trade(market_id, &criteria, event_timeout)
            .await?;
        info!("  → Trade executed: {} BTC @ {}", size, price);
    }

    ctx.kafka
        .wait_for_orderbook_after(market_id, taker_response.timestamp, event_timeout)
        .await?;
    {
        // Verify orderbook is empty (both orders filled)
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.kafka);
        orderbook_verifier.assert_empty(market_id).await?;
        info!("  → Orderbook empty (both orders filled)");
    }

    Ok(())
}

/// SECTION 4: Test IOC order with partial match
async fn test_ioc_partial_match_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Charlie places GTC ask @ {} for 3 BTC", prices::MID);
    info!(
        "Bob places IOC bid @ {} for 10 BTC → Partial match (3 filled, 7 cancelled)",
        prices::MID
    );

    let market_id = ctx.market_id;
    let event_timeout = ctx.config().event_timeout;
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
    info!(
        "  → Bob's IOC bid: {} BTC filled, {} BTC cancelled",
        filled_size, cancelled_size
    );

    // Verify trade and orderbook state
    {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.kafka);
        let criteria = TradeCriteria::new()
            .market_id(market_id)
            .maker_user_id(users::CHARLIE)
            .taker_user_id(users::BOB)
            .size(filled_size)
            .taker_order_id(taker_response.order_id);

        trade_verifier
            .wait_and_assert_trade(market_id, &criteria, event_timeout)
            .await?;
        info!("  → Trade executed for filled portion: {} BTC", filled_size);
    }

    ctx.kafka
        .wait_for_orderbook_after(market_id, taker_response.timestamp, event_timeout)
        .await?;
    {
        // Verify orderbook is empty (maker filled, taker IOC doesn't rest)
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.kafka);
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
    info!(
        "Alice places IOC bid @ {} for 6 BTC → Crosses 2.5 levels",
        prices::HIGH
    );

    let market_id = ctx.market_id;
    let event_timeout = ctx.config().event_timeout;
    let level1_price = prices::MID; // 50,000
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

    // Verify orderbook has 3 ask levels
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.kafka);
        orderbook_verifier
            .wait_and_assert_depth(market_id, 0, 3, response3.timestamp, event_timeout)
            .await?;
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
    {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.kafka);

        // Verify trade at level 1 (2 BTC @ 50,000)
        let criteria1 = TradeCriteria::new()
            .market_id(market_id)
            .maker_order_id(response1.order_id)
            .taker_order_id(ioc_response.order_id)
            .price(level1_price)
            .size(2);
        trade_verifier
            .wait_and_assert_trade(market_id, &criteria1, event_timeout)
            .await?;
        info!("  → Trade 1: 2 BTC @ {}", level1_price);

        // Verify trade at level 2 (3 BTC @ 51,000)
        let criteria2 = TradeCriteria::new()
            .market_id(market_id)
            .maker_order_id(response2.order_id)
            .taker_order_id(ioc_response.order_id)
            .price(level2_price)
            .size(3);
        trade_verifier
            .wait_and_assert_trade(market_id, &criteria2, event_timeout)
            .await?;
        info!("  → Trade 2: 3 BTC @ {}", level2_price);

        // Verify trade at level 3 (1 BTC @ 52,000)
        let criteria3 = TradeCriteria::new()
            .market_id(market_id)
            .maker_order_id(response3.order_id)
            .taker_order_id(ioc_response.order_id)
            .price(level3_price)
            .size(1);
        trade_verifier
            .wait_and_assert_trade(market_id, &criteria3, event_timeout)
            .await?;
        info!("  → Trade 3: 1 BTC @ {}", level3_price);
    }

    {
        // Verify orderbook now has 1 ask level (remaining 3 BTC @ 52,000)
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.kafka);
        orderbook_verifier
            .wait_and_assert_depth(market_id, 0, 1, ioc_response.timestamp, event_timeout)
            .await?;
        orderbook_verifier
            .assert_level(market_id, Side::Ask, level3_price, 3)
            .await?;
        info!("  → Orderbook: 3 BTC remaining @ {}", level3_price);
    }

    Ok(())
}

/// SECTION 6: Test market buy order with PriceCache and slippage
///
/// Market buy orders:
/// - Set price = u64::MAX (sentinel value)
/// - Risk R1 calculates conservative_price = best_ask + slippage
/// - Matching engine uses conservative_price as ceiling
/// - Price improvement triggers refund in R2
async fn test_market_buy_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Setting up orderbook for market buy test:");
    info!("  - First: Clear leftover ask @ 52k from Section 5");
    info!("  - Charlie: 5 BTC @ 53,000 (best ask)");
    info!("  - Bob: 5 BTC @ 54,000");
    info!("Alice places market buy for 4 BTC");

    let market_id = ctx.market_id;
    let event_timeout = ctx.config().event_timeout;
    let best_ask_price = 53_000;
    let second_ask_price = 54_000;
    let buy_size = 4;
    let market_spec = ctx.market_spec().clone();

    // Clear the leftover 3 BTC @ 52k from Section 5 to get clean orderbook
    let clear_order = OrderBuilder::place_limit()
        .user(users::ALICE)
        .price(52_000)
        .size(3)
        .side(Side::Bid)
        .market_id(market_id)
        .build();
    let clear_response = ctx.execute_command(clear_order)?;
    info!("  → Cleared leftover 3 BTC @ 52k");

    // Setup: Create orderbook with asks
    // Charlie places ask @ 53,000 (best ask)
    let ask1 = OrderBuilder::place_limit()
        .user(users::CHARLIE)
        .price(best_ask_price)
        .size(5)
        .side(Side::Ask)
        .market_id(market_id)
        .build();
    let ask1_response = ctx.execute_command(ask1)?;
    info!("  → Ask 1 placed: order_id={}", ask1_response.order_id);

    // Bob places ask @ 54,000
    let ask2 = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(second_ask_price)
        .size(5)
        .side(Side::Ask)
        .market_id(market_id)
        .build();
    let ask2_response = ctx.execute_command(ask2)?;
    info!("  → Ask 2 placed: order_id={}", ask2_response.order_id);

    // Verify orderbook setup (cleared leftover, now just our 2 asks)
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.kafka);
        orderbook_verifier
            .wait_and_assert_depth(market_id, 0, 2, ask2_response.timestamp, event_timeout)
            .await?;
        info!("  → Orderbook ready: 0 bids, 2 asks (53k, 54k)");
    }

    // Record Alice's initial balance, as the clearing order left it
    let initial_usd_balance = ctx
        .kafka
        .wait_for_balance_after(
            users::ALICE,
            assets::USD,
            clear_response.timestamp,
            event_timeout,
        )
        .await?;
    let initial_btc_balance = ctx
        .kafka
        .wait_for_balance_after(
            users::ALICE,
            assets::BTC,
            clear_response.timestamp,
            event_timeout,
        )
        .await?;
    info!(
        "  → Alice initial: {} USD, {} BTC",
        initial_usd_balance.available, initial_btc_balance.available
    );

    // R1 caps a market buy at best_ask + best_ask * slippage_bps / 10_000, with the market's
    // configured slippage, and returns that cap as the response price.
    let slippage_adjustment = best_ask_price * u64::from(market_spec.slippage) / 10_000;
    let expected_conservative_price = best_ask_price + slippage_adjustment;
    info!(
        "  → Expected conservative price: {} (best_ask={}, slippage={})",
        expected_conservative_price, best_ask_price, slippage_adjustment
    );

    // Alice places market buy order (price = u64::MAX)
    let market_buy = OrderBuilder::place_market()
        .user(users::ALICE)
        .size(buy_size)
        .side(Side::Bid)
        .market_id(market_id)
        .build();

    let market_response = ctx.execute_command(market_buy)?;
    ResponseVerifier::assert_filled(&market_response)?;
    ResponseVerifier::assert_price(&market_response, expected_conservative_price)?;
    info!(
        "  → Alice's market buy filled: order_id={}",
        market_response.order_id
    );

    // Verify trade execution
    let trade = {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.kafka);

        // Should execute against best ask (53,000)
        let criteria = TradeCriteria::new()
            .market_id(market_id)
            .maker_order_id(ask1_response.order_id)
            .taker_order_id(market_response.order_id)
            .price(best_ask_price) // Should execute at best ask, NOT conservative price
            .size(buy_size);

        let trade = trade_verifier
            .wait_and_assert_trade(market_id, &criteria, event_timeout)
            .await?;
        info!(
            "  → Trade executed: {} BTC @ {} (at best ask, below conservative ceiling)",
            buy_size, best_ask_price
        );
        trade
    };

    // Verify balances reflect trade + price improvement refund
    {
        // Calculate expected costs
        let actual_cost = market_spec
            .calculate_quote_cost(best_ask_price, buy_size)
            .expect("scenario notional fits in u64");
        let conservative_cost = market_spec
            .calculate_quote_cost(expected_conservative_price, buy_size)
            .expect("scenario notional fits in u64");
        let expected_refund = conservative_cost - actual_cost; // Price improvement refund

        // Bid taker fee is charged on the received base asset.
        let taker_fee_bps = market_spec.taker_fee;
        let taker_fee_btc = (buy_size * taker_fee_bps).div_ceil(10_000);

        let final_usd_balance = ctx
            .kafka
            .wait_for_balance_after(
                users::ALICE,
                assets::USD,
                market_response.timestamp,
                event_timeout,
            )
            .await?;
        let final_btc_balance = ctx
            .kafka
            .wait_for_balance_after(
                users::ALICE,
                assets::BTC,
                market_response.timestamp,
                event_timeout,
            )
            .await?;
        // Section 7 reads Charlie's balance, which this fill changed last: wait for both assets
        ctx.kafka
            .wait_for_balance_after(
                users::CHARLIE,
                assets::BTC,
                market_response.timestamp,
                event_timeout,
            )
            .await?;
        ctx.kafka
            .wait_for_balance_after(
                users::CHARLIE,
                assets::USD,
                market_response.timestamp,
                event_timeout,
            )
            .await?;

        // Verify BTC received (size - fee)
        let expected_btc_received = buy_size - taker_fee_btc;
        assert_settled(
            "BTC received (size - fee)",
            &initial_btc_balance,
            &final_btc_balance,
            i128::from(expected_btc_received),
        )?;

        // Verify USD spent: the cost at the best ask, with the rest of the cap's lock refunded
        assert_settled(
            "USD spent at the best ask",
            &initial_usd_balance,
            &final_usd_balance,
            -i128::from(actual_cost),
        )?;

        // The trade reports both fees: the taker's in the BTC it bought, the maker's in the USD
        // it sold for
        let maker_fee_usd = (actual_cost * market_spec.maker_fee).div_ceil(10_000);
        assert_fees(&trade, taker_fee_btc, maker_fee_usd)?;

        info!(
            "  → Price improvement: locked={}, spent={}, refund={}",
            conservative_cost, actual_cost, expected_refund
        );
        info!(
            "  → Alice final: {} USD (spent {}), {} BTC (received {}, fee {})",
            final_usd_balance.available,
            actual_cost,
            final_btc_balance.available,
            expected_btc_received,
            taker_fee_btc
        );
    }

    // Verify orderbook state (ask1 partially filled, ask2 untouched)
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.kafka);
        orderbook_verifier
            .wait_and_assert_depth(market_id, 0, 2, market_response.timestamp, event_timeout)
            .await?;
        orderbook_verifier
            .assert_level(market_id, Side::Ask, best_ask_price, 1)
            .await?;
        orderbook_verifier
            .assert_level(market_id, Side::Ask, second_ask_price, 5)
            .await?;
        info!(
            "  → Orderbook: 1 BTC @ {}, 5 BTC @ {}",
            best_ask_price, second_ask_price
        );
    }

    Ok(())
}

/// SECTION 7: Test market sell order
///
/// Market sell orders:
/// - Set price = 0 (sentinel value)
/// - Execute against best bid
/// - Simpler than buy (no slippage locking needed, only lock base asset)
async fn test_market_sell_section(ctx: &mut TestContext) -> TestResult<()> {
    info!("Setting up orderbook for market sell test:");
    info!("  - Alice: 100 BTC bid @ 52,000 (best bid)");
    info!("  - Bob: 100 BTC bid @ 51,000");
    info!("Charlie places market sell for 10 BTC");

    let market_id = ctx.market_id;
    let event_timeout = ctx.config().event_timeout;
    let best_bid_price = 52_000;
    let second_bid_price = 51_000;
    let sell_size = 10;
    let market_spec = ctx.market_spec().clone();

    // Setup: Create orderbook with bids
    // Alice places bid @ 52,000 (best bid)
    let bid1 = OrderBuilder::place_limit()
        .user(users::ALICE)
        .price(best_bid_price)
        .size(100)
        .side(Side::Bid)
        .market_id(market_id)
        .build();
    let bid1_response = ctx.execute_command(bid1)?;
    info!("  → Bid 1 placed: order_id={}", bid1_response.order_id);

    // Bob places bid @ 51,000
    let bid2 = OrderBuilder::place_limit()
        .user(users::BOB)
        .price(second_bid_price)
        .size(100)
        .side(Side::Bid)
        .market_id(market_id)
        .build();
    let bid2_response = ctx.execute_command(bid2)?;
    info!("  → Bid 2 placed: order_id={}", bid2_response.order_id);

    // Verify orderbook setup (Section 6 left 2 asks, we added 2 bids)
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.kafka);
        orderbook_verifier
            .wait_and_assert_depth(market_id, 2, 2, bid2_response.timestamp, event_timeout)
            .await?;
        info!("  → Orderbook ready: 2 bids (new), 2 asks (from Section 6)");
    }

    // Record Charlie's initial balance
    let initial_usd_balance = {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.kafka);
        balance_verifier
            .get_balance(users::CHARLIE, assets::USD)
            .await?
    };
    let initial_btc_balance = {
        let mut balance_verifier = BalanceVerifier::new(&mut ctx.kafka);
        balance_verifier
            .get_balance(users::CHARLIE, assets::BTC)
            .await?
    };
    info!(
        "  → Charlie initial: {} USD, {} BTC",
        initial_usd_balance.available, initial_btc_balance.available
    );

    // Charlie places market sell order (price = 0)
    let market_sell = OrderBuilder::place_market()
        .user(users::CHARLIE)
        .size(sell_size)
        .side(Side::Ask)
        .market_id(market_id)
        .build();

    let market_response = ctx.execute_command(market_sell)?;
    ResponseVerifier::assert_filled(&market_response)?;
    info!(
        "  → Charlie's market sell filled: order_id={}",
        market_response.order_id
    );

    // Verify trade execution
    let trade = {
        let mut trade_verifier = TradeVerifier::new(&mut ctx.kafka);

        // Should execute against best bid (52,000)
        let criteria = TradeCriteria::new()
            .market_id(market_id)
            .maker_order_id(bid1_response.order_id)
            .taker_order_id(market_response.order_id)
            .price(best_bid_price) // Should execute at best bid
            .size(sell_size);

        let trade = trade_verifier
            .wait_and_assert_trade(market_id, &criteria, event_timeout)
            .await?;
        info!(
            "  → Trade executed: {} BTC @ {} (at best bid)",
            sell_size, best_bid_price
        );
        trade
    };

    // Verify balances reflect trade
    {
        // Calculate expected revenue
        let gross_revenue = market_spec
            .calculate_quote_cost(best_bid_price, sell_size)
            .expect("scenario notional fits in u64");
        let taker_fee_bps = market_spec.taker_fee;
        let taker_fee_usd = (gross_revenue * taker_fee_bps).div_ceil(10_000);
        let expected_usd = gross_revenue.saturating_sub(taker_fee_usd);

        let final_usd_balance = ctx
            .kafka
            .wait_for_balance_after(
                users::CHARLIE,
                assets::USD,
                market_response.timestamp,
                event_timeout,
            )
            .await?;
        let final_btc_balance = ctx
            .kafka
            .wait_for_balance_after(
                users::CHARLIE,
                assets::BTC,
                market_response.timestamp,
                event_timeout,
            )
            .await?;

        // Ask taker fee is charged on the received quote asset, not the base asset.
        assert_settled(
            "BTC sold (no base-side fee)",
            &initial_btc_balance,
            &final_btc_balance,
            -i128::from(sell_size),
        )?;

        // Verify USD received (gross quote revenue minus quote-side taker fee)
        assert_settled(
            &format!("USD received (gross {gross_revenue} - fee {taker_fee_usd})"),
            &initial_usd_balance,
            &final_usd_balance,
            i128::from(expected_usd),
        )?;

        // The trade reports both fees: the taker's in the USD it sold for, the maker's in the
        // BTC it bought
        let maker_fee_btc = (sell_size * market_spec.maker_fee).div_ceil(10_000);
        assert_fees(&trade, taker_fee_usd, maker_fee_btc)?;

        info!(
            "  → Charlie final: {} USD (received {}, fee {}), {} BTC (sold {})",
            final_usd_balance.available,
            expected_usd,
            taker_fee_usd,
            final_btc_balance.available,
            sell_size
        );
    }

    // Verify orderbook state (bid1 partially filled, bid2 untouched, 2 asks remain)
    {
        let mut orderbook_verifier = OrderbookVerifier::new(&mut ctx.kafka);
        orderbook_verifier
            .wait_and_assert_depth(market_id, 2, 2, market_response.timestamp, event_timeout)
            .await?;
        orderbook_verifier
            .assert_level(market_id, Side::Bid, best_bid_price, 90)
            .await?;
        orderbook_verifier
            .assert_level(market_id, Side::Bid, second_bid_price, 100)
            .await?;
        info!(
            "  → Orderbook: Bids: 90 BTC @ {}, 100 BTC @ {} | 2 asks remain",
            best_bid_price, second_bid_price
        );
    }

    Ok(())
}

/// A settled taker's balance in one asset: its total moved by `change`, and its locked amount did
/// not (the taker's own resting orders keep theirs).
fn assert_settled(
    what: &str,
    before: &BalanceEvent,
    after: &BalanceEvent,
    change: i128,
) -> TestResult<()> {
    let moved = i128::from(after.total) - i128::from(before.total);
    if moved != change || after.locked != before.locked {
        return Err(TestError::Verification {
            message: format!(
                "{what}: total {} -> {} ({moved:+}), locked {} -> {}; expected {change:+}, \
                 locked unchanged",
                before.total, after.total, before.locked, after.locked
            ),
        });
    }
    Ok(())
}

/// The fees a trade reports, each in the asset that side received.
fn assert_fees(trade: &TradeEvent, taker_fee: u64, maker_fee: u64) -> TestResult<()> {
    if (trade.taker_fee, trade.maker_fee) != (taker_fee, maker_fee) {
        return Err(TestError::Verification {
            message: format!(
                "trade {}:{} reports (taker, maker) fees ({}, {}), expected ({taker_fee}, \
                 {maker_fee})",
                trade.taker_order_id, trade.maker_order_id, trade.taker_fee, trade.maker_fee
            ),
        });
    }
    Ok(())
}
