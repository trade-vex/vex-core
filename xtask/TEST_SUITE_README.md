# VEX-CORE Integration Test Suite

## Overview

Production-grade test framework for VEX-CORE trading system. Validates end-to-end order processing: submission → journaling → matching → event publishing to Kafka.

## Architecture

```
Test Framework → VexCore → [Journaling, Risk, Orderbook] → Events Handler → Kafka → Test Framework
```

## Two-Phase Verification

1. **Response Verification** - Immediate validation of OrderCommand response fields
2. **Kafka Verification** - Eventual consistency validation of the events the core publishes

## Running Tests

### Prerequisites

1. VexCore server: `VEX_ENV=development cargo run --bin test_server` (`VEX_ENV` is required, as for the server)
1. VexCore server: `cargo run --bin test_server`
2. Kafka with a Schema Registry: the server publishes to its configured broker and registers its
   schemas at `schema_registry_url` (default `http://localhost:8081`, override with `VEX__SCHEMA_REGISTRY_URL`); the suite reads the events
   back from `TestConfig::kafka_brokers` (default `localhost:19092`, Redpanda's host listener in
   `tests/docker-compose.yml`)

### Execute

```bash
# Comprehensive integration test (RECOMMENDED - all order types in single test)
cargo run --bin run_test_suite all

# Individual test suites (state-aware, single continuous test per suite)
cargo run --bin run_test_suite gtc          # GTC orders (3 sections)
cargo run --bin run_test_suite ioc          # IOC orders (5 sections)
cargo run --bin run_test_suite fok          # FOK orders (6 sections)
cargo run --bin run_test_suite cancellation # Cancellations (8 sections)
cargo run --bin run_test_suite balance      # Balance operations

# Options
cargo run --bin run_test_suite all --verbose    # Detailed logging
cargo run --bin run_test_suite all --fail-fast  # Stop on first failure
```

## Test Philosophy: State-Aware Continuous Testing

**CRITICAL**: All tests maintain state throughout execution - **NO cleanup between sections**.

- Each section builds on previous state
- Orderbook persists across sections
- Balances carry forward
- Mirrors real exchange operation

Example: Comprehensive test Section 3 uses orderbook from Section 2, Section 4 uses state from Section 3, etc.

## Test Suites

### Comprehensive Integration (`scenarios/comprehensive.rs`)

**Single massive test combining all order types in realistic trading scenario.**

**10 Sections:**
1. Setup - Fund users (50M USD, 5000 BTC each)
2. Build Orderbook - Multi-level GTC orders
3. IOC vs Liquidity - Partial fills against existing book
4. FOK Orders - All-or-nothing scenarios
5. Active Cancellations - Cancel during trading
6. Mixed Stress Test - Rapid GTC/IOC/FOK sequence
7. Edge Cases - Self-match prevention
8. Multi-Level Matching - Orders crossing multiple levels
9. Partial + Cancel - Cancel partially filled orders
10. Final Verification - Balance invariants

**Runtime**: ~10 seconds

### GTC Orders (`scenarios/gtc.rs`)

**3 Sections:**
1. Setup - Fund users
2. No Match - Orders rest on book
3. Full Match - Complete fill
4. Partial Match - Partial fill + rest on book

### IOC Orders (`scenarios/ioc.rs`)

**5 Sections:**
1. Setup
2. No Match - Fully cancelled
3. Full Match - Complete fill
4. Partial Match - Partial fill + cancel remainder
5. Multiple Levels - Cross 3 price levels

### FOK Orders (`scenarios/fok.rs`)

**6 Sections:**
1. Setup
2. No Match - Rejected
3. Insufficient Liquidity - Rejected despite partial availability
4. Exact Match - Complete fill
5. Excess Liquidity - Fill with remainder staying
6. Multiple Levels - All-or-nothing across levels

### Cancellations (`scenarios/cancellation.rs`)

**8 Sections:**
1. Setup
2. Cancel Resting Bid - Funds unlock
3. Cancel Resting Ask - Funds unlock
4. Cancel Partially Filled - Only remaining unlocks
5. Cancel Non-existent - Handled gracefully
6. Cancel Already Filled - Handled gracefully
7. Cancel & Replace - Cancel then new order at same price
8. Multiple Cancellations - Sequential cancels

### Balance Management (`scenarios/balance.rs`)

- Single deposit
- Multiple deposits same asset
- Deposits different assets

## Key Components

### Builders (`builders/order_builder.rs`)
Type-safe fluent API for OrderCommands with compile-time guarantees.

### Verifiers
- `ResponseVerifier` - Response field validation
- `BalanceVerifier` - Balance state assertions
- `TradeVerifier` - Trade matching with TradeCriteria
- `OrderbookVerifier` - Orderbook validation with wait APIs

### Kafka Verification

`KafkaVerifier` (`test_framework/kafka.rs`) consumes the topics the core publishes to, decoding
each value with the `processors` crate's protobuf types after stripping its Schema Registry
framing. When the test context is created it reads each topic from its current end, so a run sees
only its own events, whatever the topics already hold. It keeps what a downstream reader keeps:

| Topic | Key | Message | Kept |
|-------|-----|---------|------|
| `balances` | `{user_id}:{asset_id}` | `BalanceEvent` | Latest per key |
| `trades` | `{taker_order_id}:{maker_order_id}` | `TradeEvent` | Every trade, in publication order |
| `orderbook` | `{market_id}` | `OrderbookEvent` (levels as JSON) | Latest per key |

Every read first takes in all the events the broker holds at that moment. The core publishes
asynchronously, after it has answered the command, so assertions made right after a command
either wait first or use a wait-based API.

### Wait-Based APIs

Wait-based APIs handle eventual consistency:
- `wait_for_trade()` - Poll until trade matching criteria appears
- `wait_for_orderbook_update()` - Wait for orderbook publication
- `wait_and_assert_level()` - Wait for specific price level
- `wait_and_assert_depth()` - Wait for orderbook depth

## System Invariants

1. Balance Consistency: `available + locked = total`
2. No Negative Balances
3. Orderbook Ordering: Bids descending, asks ascending
4. No Crossed Book: `best_ask ≥ best_bid`
5. Trade Validity: Size > 0, valid prices

## Market Configuration

The suites use market 65538 as `config.dev.yaml` specifies it, loaded by `TestContext::market_spec`:

- **Market ID**: 65538 (0x00010002)
- **Base Asset**: 2 (BTC), native scale 10^8
- **Quote Asset**: 1 (USD), native scale 10^6, `quote_scale_k` 1
- **Maker Fee**: 0bp, **Taker Fee**: 0bp
- **Slippage**: 150bp (the market-buy price cap above the best ask)

Locked and settled quote amounts are `calculate_quote_cost(price, size)`, not `price * size`: a bid of
size 10 at 40,000 locks 4,000 USD units (ARCHITECTURE.md, Balance Locking Logic).

## Example Usage

```rust
use xtask::test_framework::TestContext;
use xtask::builders::OrderBuilder;
use xtask::verifiers::{ResponseVerifier, BalanceVerifier};
use common::Side;

#[tokio::test]
async fn test_example() -> TestResult<()> {
    let mut ctx = TestContext::new().await?;
    ctx.fund_user(1, 1_000_000, 1).await?;

    let order = OrderBuilder::place_limit()
        .user(1)
        .price(50_000)
        .size(10)
        .side(Side::Bid)
        .market_id(ctx.market_id)
        .build();

    let response = ctx.execute_command(order)?;

    // Phase 1: Response verification
    ResponseVerifier::assert_placed(&response)?;

    // Phase 2: Kafka verification: the bid locks its quote cost (5,000 on market 65538)
    let locked = ctx.market_spec().calculate_quote_cost(50_000, 10).expect("fits in u64");
    let mut verifier = BalanceVerifier::new(&mut ctx.kafka);
    verifier.assert_locked_eq(1, 1, locked).await?;

    Ok(())
}
```

## Contributing

When adding tests:
1. Follow state-aware continuous testing model
2. Use wait-based APIs for Kafka verification
3. Handle leftover state from previous sections
4. Document expected state transitions
5. Add proper sleep delays between operations, perfomance optmization is not the goal for the test suite, they must be handled separately

## Security
This test suite is the **security backbone** of VEX-CORE. All changes must maintain type safety, test isolation principles, and comprehensive error handling.
