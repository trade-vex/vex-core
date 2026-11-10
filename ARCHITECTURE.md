# VEX-CORE: High-Performance Trading Engine Architecture

## Table of Contents

1. [System Overview](#system-overview)
2. [Core Design Principles](#core-design-principles)
3. [Architecture Components](#architecture-components)
4. [Processing Pipeline](#processing-pipeline)
5. [Concurrency and Sharding](#concurrency-and-sharding)
6. [Transport Layer](#transport-layer)
7. [Data Structures and Algorithms](#data-structures-and-algorithms)
8. [Performance Characteristics](#performance-characteristics)
9. [Build and Deployment](#build-and-deployment)

## System Overview

VEX-CORE is a high-performance, low-latency order matching engine designed for cryptocurrency spot trading. The system achieves microsecond-scale latency through mechanical sympathy, careful memory management, and the Disruptor pattern.

### Design Goals

1. **Deterministic Latency**: Achieve predictable p99 latencies under 50-70 microseconds for order processing
2. **High Throughput**: Process sustained high-volume order flow
3. **Correctness**: Maintain strict balance invariants and prevent race conditions in concurrent settlement
4. **Horizontal Scalability**: Scale across multiple markets and user shards

### Non-Goals

This system explicitly does not:
- Provide distributed consensus (single-node design for maximum performance)
- Support margin trading or derivatives (spot markets only)
- Guarantee fairness across gateways (deterministic sequencing based on arrival)

## Core Design Principles

### 1. Sequential Processing with Parallel Execution

The architecture uses a single sequential pipeline per order (Disruptor pattern) with parallelization at the processor level through sharding. This design minimizes lock contention while achieving parallelism.

**Rationale**: The Disruptor pattern provides:
- Memory barriers instead of widespread locking
- Predictable cache line behavior
- Minimal allocation in the hot path
- Natural backpressure through ring buffer saturation

### 2. Mechanical Sympathy

The system is designed with explicit awareness of modern CPU architecture:
- Sequential memory access patterns where possible
- Core pinning to prevent context switching

### 3. Fail-Fast Validation

Validation occurs as early as possible in the pipeline:
- Gateway performs syntactic validation before submission
- Risk Engine R1 validates funds before matching
- Matching engine performs semantic validation during order placement

**Rationale**: Early rejection minimizes wasted work. Orders rejected at Risk R1 are consumed by the pipeline but skipped by downstream processors, avoiding matching engine and settlement overhead.

### 4. Event Sourcing

The system maintains an append-only journal via Aeron Archive. State reconstruction requires replaying all prior events in sequence. Snapshots are not currently implemented.

**Consistency Model**:
- Linearizability within a single market (total order on all operations)
- Eventual consistency across markets (updates published asynchronously)

## Architecture Components

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    Gateway Network (Geo-Distributed)            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │ Gateway 1   │  │ Gateway 2   │  │ Gateway N   │              │
│  │ (Aeron)     │  │ (Aeron)     │  │ (Aeron)     │              │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘              │
└─────────┼─────────────────┼─────────────────┼───────────────────┘
          │ (UDP)           │ (UDP)           │ (UDP)
          └─────────────────┴─────────────────┘
                            │
                    ┌───────▼────────┐
                    │  Aeron Media   │
                    │    Driver      │
                    │ (UDP Transport)│
                    └───────┬────────┘
                            │
          ┌─────────────────┴─────────────────┐
          │     VEX-CORE Processing Engine    │
          │                                   │
          │  ┌──────────────────────────────┐ │
          │  │   Disruptor Ring Buffer      │ │
          │  └──────────────┬───────────────┘ │
          │                 │                 │
          │     ┌───────────▼───────────┐     │             ┌───────────────┐
          │     │  Journaling Processor │     │             | Aeron Archive |
          │     │(timestamp,orderid_gen)│------------------>| (Journalling) |
          │     └───────────┬───────────┘     │             └───────────────┘
          │                 │                 │             
          │  ┌──────────────▼─────────────┐   │
          │  │      Risk Engine R1        │   │
          │  │ (Pre-Validate, lock funds) │   │
          │  └──────────────┬─────────────┘   │
          │                 │                 │
          │  ┌──────────────▼─────────────┐   │
          │  │   Matching Engines         │   │
          │  └──────────────┬─────────────┘   │
          │                 │                 │
          │  ┌──────────────▼─────────────┐   │
          │  │  Risk Engine R2 (Post)     │   │
          │  │                            │   │
          │  └──────────────┬─────────────┘   │
          │                 │                 │
          │     ┌───────────▼───────────┐     │
          │     │   Events Handler      │     │
          │     │ (Kafka + Gateway Pub) │     │
          │     └───────────┬───────────┘     │
          └─────────────────┼─────────────────┘
                            │
              ┌─────────────┴─────────────┐
              │                           │
      ┌───────▼────────┐         ┌───────▼────────┐
      │  Kafka Cluster │         │   Gateway      │
      │  (Events)      │         │  (Response)    │
      └────────────────┘         └────────────────┘
```

### Component Responsibilities

#### 1. Gateway Network

**Purpose**: Ingest orders from external clients and route responses back.

**Implementation**: Separate gateway instances deployed at different geo-locations. Each gateway communicates with VEX-CORE via UDP transport using Aeron.

**Key Design Decisions**:
- Gateway ID encoded in Snowflake order IDs for response routing
- SBE (Simple Binary Encoding) for zero-copy serialization
- Fixed frame size for predictable memory access patterns

#### 2. Disruptor Ring Buffer

**Purpose**: Provide lock-free, wait-free queue for order commands between publishers and processors.

**Implementation**: Custom Rust disruptor based on LMAX architecture. Ring buffer with power-of-2 size for bitwise modulo operations.

**Memory Layout**:
- Each slot contains a full OrderCommand
- Total ring buffer size determined by: `buffer_slots * sizeof(OrderCommand)`

**Backpressure**: Gateway ingress never waits for a slot. A command that finds the ring buffer full is not enqueued; the originating gateway gets it back with `Status::Busy` (wire value 7): nothing was executed, retry. `Rejected` is reserved for commands the engine processed and refused.

#### 3. Journaling Processor

**Purpose**: Assign globally unique, monotonically increasing order IDs and persist commands to Aeron Archive.

**Field Transformations**:

Ingress (`FragmentHandler`, networking/src/server/cmd_handler.rs) sets `status = Processing` and records the sending gateway in `route_gateway_id`, which the events handler uses to route the response. A cancel's `order_id` is the client's id of the order to cancel.

The journaling processor then stamps every command and assigns an order id to every command but a cancel:

```rust
// In JournalingProcessor (processors/src/journaling.rs)
cmd.timestamp = self.next_timestamp();
if cmd.command != OrderCommandType::CancelOrder {
    cmd.order_id = self
        .snowflake
        .generate(cmd.timestamp, cmd.route_gateway_id as u64)
        .unwrap();
}
```

**Snowflake ID Structure** (64 bits):
```
┌─────────────┬────────────┬─────────┐
│  Timestamp  │  Sequence  │ Gateway │
│   48 bits   │  12 bits   │ 4 bits  │
└─────────────┴────────────┴─────────┘
```

An id is the command's timestamp plus its gateway (common/src/snowflake.rs):
- **Timestamp** (48 bits): ~1.024 µs ticks since 2025-01-01 (the timestamp's nanoseconds >> 10), in the MSB for natural time ordering
- **Sequence** (12 bits): the timestamp's nanoseconds within its tick (0-1023), which sequences the commands of one tick
- **Gateway** (4 bits): Gateway identifier (0-15)

Ids therefore order exactly as their timestamps do, and are as unique: journaling stamps every command strictly above the previous one and above the last command in the journal (see Timestamp Field), so an id is never issued twice, within a run or across a restart. A tick holds 1024 nanoseconds, so its sequence cannot overflow.

**Design Decision: Monotonically Increasing Order IDs**:

Order IDs are designed to be strictly monotonically increasing. This design choice enables efficient orderbook operations:

**Binary Search for Order Cancellation** (orderbook/src/lib.rs:72-74):

```rust
fn remove_order(&mut self, order_id: u64, cmd: &mut OrderCommand) {
    if let Ok(pos) = self.orders.binary_search_by_key(&order_id, |order| order.order_id)
    {
        // Cancel order at position 'pos'
    }
}
```

The `orders` VecDeque stores orders in insertion order. Since orders are assigned monotonically increasing IDs by the single-threaded journaling processor, this insertion order is also ascending order_id order. Binary search on this naturally-sorted collection provides O(log n) lookup complexity for cancellations, versus O(n) for linear scan.

**Additional Benefits**:

- **Time-Ordered Processing**: Timestamp in MSB ensures IDs naturally sort by time
- **Deterministic Replay**: Archive replay processes commands in order_id sequence, preserving original execution order
- **No Sorting Overhead**: VecDeque remains sorted without explicit sorting operations

**Timestamp Field**:

The `cmd.timestamp` field is the command's journaled time: wall-clock nanoseconds, forced strictly above the previous command's and above the journal tail (`JournalingProcessor::next_timestamp`). The journal tail is the timestamp of the last command in the archive, which the server reads on every start by replaying only the term that holds it (`VexCoreServer::read_journal_tail`). With journaling disabled there is no tail, and timestamps, and the ids derived from them, follow the wall clock alone. Every Kafka event a command produces, `orderbook` included, carries it.

**Replay Mode**:

Every start replays the journal, with the replay flag set (`ReplayControl::enabled()` in `init_internal`) until the replay has drained (`finish_replay`). While it is set, the journaling processor skips ID generation and archive publishing:

```rust
if self.replay_enabled.is_enabled() {
    return;  // Skip journaling during replay
}
```

During replay, OrderCommands already have assigned order_ids and timestamps from the original execution. Re-assigning would break deterministic replay. Balance preload (development and test only) runs before the replay, with the flag set, so it is never stamped, journaled or published.

#### 4. Risk Engine (R1 - Pre-Processing)

**Purpose**: Validate sufficient balance and lock funds before order enters the matching engine.

**Sharding Strategy**: User-based sharding using bitwise AND on user_id.

```rust
// Shard assignment for user_id with N shards
// shard_mask = num_shards - 1 (e.g., 4 shards → mask = 3)
let shard_id = user_id & shard_mask;
```

**Balance Locking Logic** (`RiskEngine::reserve_funds_for_order` and `bid_amount`, `processors/src/risk_engine.rs`):

A quote amount is not simply `price * size`: it goes through the market's native scales in `CoreMarketSpecification::calculate_quote_cost` (`common/src/market_specification.rs`):

```
quote_cost(price, size) = price * size * quote_native_scale / (base_native_scale * quote_scale_k)
```

computed in `u128` and rounded down. An overflow, a result above `u64::MAX` or a zero denominator rejects the order. On the development market 65538 (`base_native_scale` 10^8, `quote_native_scale` 10^6, `quote_scale_k` 1) a bid of size 10 at 40,000 locks `quote_cost = 4,000`, not 400,000.

For a BID order (buying base asset with quote asset):
```
locked_amount = quote_cost(price, size)   // rejected when it is 0
asset_to_lock = quote_asset(market_id)
```

For an ASK order (selling base asset for quote asset):
```
locked_amount = size                      // a limit ask whose quote_cost(price, size) is 0 is rejected
asset_to_lock = base_asset(market_id)
```

R1 also records each bid's locked amount as that order's collateral. Every fill spends `quote_cost(fill_price, fill_size)` of it, plus any price-improvement refund, and R2 releases what is left when the order completes or is cancelled. After a partial fill at its own price a bid therefore still locks `quote_cost(price, size) - quote_cost(price, filled)`, which can exceed `quote_cost(price, size - filled)` by one atomic unit of rounding.

**Market Orders** (price = u64::MAX for bids, price = 0 for asks):

Market orders require special handling since the execution price is unknown at fund reservation time. Risk R1 uses the PriceCache to obtain conservative price estimates:

```rust
// For market buy (BID): lock enough quote currency. `spec.slippage` is in basis points.
let best_ask = price_cache.get_prices(market_id).map_or(0, |(_, best_ask)| best_ask);
let slippage_adjustment = best_ask * spec.slippage / 10_000;  // in u128, rounded down
let conservative_price = best_ask + slippage_adjustment;
cmd.price = conservative_price;  // Mutate price field; the response returns it
locked_amount = spec.calculate_quote_cost(conservative_price, size);
```

For market sell (ASK): Lock base asset only (price doesn't affect locked amount).

**PriceCache Importance**:

The PriceCache is a shared, read-only snapshot of top-of-book prices across all markets, critical for risk management of market orders:

1. **Lock-Free Reads**: Risk R1 shards query prices without contending for locks with the matching engines
2. **Conservative Pricing**: Adds slippage buffer (basis points) to protect against price movement between R1 and Matching
3. **Memory Ordering**: AtomicU64 with Release-Acquire semantics ensures visibility of price updates from matching engines
4. **Rejection Logic**: If `best_ask = 0` (no liquidity), market buy is rejected immediately at R1

**Price Ceiling Enforcement**:

The matching engine respects the conservative price as a ceiling. For a market buy with `conservative_price = 50,350`:

```rust
// Matching engine price check for BID
if taker_price >= maker_price { /* match */ }
// With conservative_price: if 50,350 >= maker_ask { /* match */ }
```

The order will ONLY match against asks priced at or below the conservative price. If all available asks are above 50,350 (e.g., best ask moved to 50,400), the order matches nothing. Since market orders are typically IOC/FOK, the unfilled order is cancelled rather than resting on book. This guarantees the matching engine cannot spend more than the locked funds.

**Price Improvement Refund**: If actual execution is better than conservative estimate, R2 refunds the difference to available balance.

**Locking**: Risk engines use `Mutex` locks on the balance store within each shard. Each shard processes events sequentially, acquiring locks as needed.

**Critical Invariant**:
```
available + locked = total  (always maintained)
```

**Rejection Reasons**:
- Insufficient available balance
- Market specification not found
- Invalid market order (no opposite side liquidity)

#### 5. Matching Engine

**Purpose**: Execute order matching using price-time priority and maintain orderbook state.

**Sharding Strategy**: Market-based sharding using bitwise AND on market_id.

```rust
// Shard assignment for market_id with N shards
let shard_id = market_id & shard_mask;
```

**Orderbook Data Structure**: Generic `OrderBook` implementation supporting different side types. Currently uses BTreeMap-based sides.

**BTreeAskSide** (asks sorted ascending):
```rust
pub struct BTreeAskSide {
    tree: BTreeMap<u64, VecDeque<Order>>
}
```

**BTreeBidSide** (bids sorted descending):
```rust
pub struct BTreeBidSide {
    tree: BTreeMap<Reverse<u64>, VecDeque<Order>>
}
```

**Generic OrderBook**:
```rust
pub struct OrderBook<A: OrderBookSide, B: OrderBookSide> {
    pub asks: A,
    pub bids: B,
    pub market_id: u32,
}
```

The sides are generic, allowing flexibility in the underlying data structure implementation. Current implementation uses BTreeMap for sorted price levels.

**Order Matching Algorithm**:

```
1. Determine aggressor side (bid or ask)
2. While order has remaining size AND opposite side has liquidity:
   a. Get best opposite price level
   b. Check price compatibility
   c. Match against orders in FIFO order (VecDeque front)
   d. Generate MatcherTradeEvent for each fill
   e. If maker order fully filled, remove from orderbook
   f. If price level empty, remove from tree
3. Handle remaining quantity based on TimeInForce:
   - GTC: Place remainder on orderbook
   - IOC: Cancel remainder
   - FOK: If any remainder, cancel entire order
```

**TimeInForce Semantics**:

- **GTC (Good-Till-Cancel)**: Order rests on book until filled or explicitly cancelled
- **IOC (Immediate-Or-Cancel)**: Fill available liquidity, cancel remainder
- **FOK (Fill-Or-Kill)**: All-or-nothing, reject if full size cannot be filled immediately

**MatcherTradeEvent Linked List**:

When an order matches multiple resting orders, events are stored as a linked list:

```rust
pub struct MatcherTradeEvent {
    pub price: u64,
    pub size: u64,
    pub maker_user_id: u64,
    pub matched_order_id: u64,
    pub active_order_completed: bool,
    pub matched_order_completed: bool,
    pub next_event: Option<Box<MatcherTradeEvent>>,
    pub maker_balance: [UserBalance; 2],
}
```

#### 6. Risk Engine (R2 - Post-Processing)

**Purpose**: Settle trades by moving funds from locked to available/other asset based on MatcherTradeEvents.

**Settlement Logic**:

For each MatcherTradeEvent in the linked list:

```
Taker (BID - buying base for quote):
  1. Subtract locked quote: locked_quote -= quote_cost(price, size)
  2. Calculate taker fee: fee = ceil(size * taker_fee_bps / 10000)
  3. Add base asset: available_base += (size - fee)

Maker (ASK - selling base for quote):
  1. Subtract locked base: locked_base -= size
  2. Calculate maker fee: fee = ceil(quote_cost(price, size) * maker_fee_bps / 10000)
  3. Add quote asset: available_quote += (quote_cost(price, size) - fee)
```

`quote_cost` is `CoreMarketSpecification::calculate_quote_cost`, as in R1 (`RiskEngine::settle_trade`).

**Fee Structure**: Maker and taker fees configured per market in basis points. Each fee is rounded up per fill, so a fill pays its exact share plus less than one atomic unit: slicing an order into many fills can never pay less than one fill would, and over-pays by at most one unit per fill.

**Price Improvement Refund**:

If a taker BID order executes at a better price than the limit:
```
execution_price < limit_price  =>  refund = quote_cost(limit_price - execution_price, size)
```

The refund is moved from locked back to available quote currency.

**Locking**: R2 engines use `Mutex` locks on the balance store within each shard, acquired during settlement operations.

**Critical Invariant** (maintained across settlement):
```
SUM(all user balances in asset X) = constant  (conservation of assets)
```

**Shard Coordination**:

R2 engines process trade events for both maker and taker:
- Each R2 shard handles users where `user_id & shard_mask == shard_id`
- When maker and taker are in different shards, both shards process the same event
- Balance stores are independent per shard (disjoint user sets)

#### 7. Events Handler

**Purpose**: Publish order lifecycle events to Kafka for external consumption AND send responses back to the originating gateway via Aeron.

**Kafka Topics**: values are `trading.proto` messages framed for the Schema Registry (subject `<topic>-value`); keys are UTF-8 strings; every event carries its command's timestamp. `deploy/kafka/topics.toml` declares the topics.

| Topic | Message | Key | Emitted for |
|---|---|---|---|
| `balances` | `BalanceEvent` | `user_id:asset_id` | the taker after a placement, cancel or fills; each maker once per command; a deposit or withdrawal |
| `orders` | `OrderEvent` | `order_id` | the taker; each maker per fill |
| `orderbook` | `OrderbookEvent` (L2 snapshot) | `market_id` | a placement, cancel or fill |
| `cancels` | `CancelOrderEvent` | `order_id` | a cancellation |
| `trades` | `TradeEvent` | `taker_order_id:maker_order_id` | each fill |
| `deposits` | `DepositEvent` | `user_id` | a processed deposit |
| `withdrawals` | `WithdrawEvent` | `user_id` | a processed withdrawal |

The ordering and delivery contract is on `KafkaEventsHandler` (`processors/src/events.rs`). In short: per key, a run publishes in engine order through one ordered sender and an idempotent producer; at most one event per key per command, holding the key's state after it; `(key, timestamp)` identifies an event, and a reader takes a key's latest state from the greatest timestamp. No event is dropped while the process runs: a Kafka or registry outage back-pressures intake (`Busy`).

**Gateway Response**: The events handler also publishes the processed OrderCommand back to the originating gateway via Aeron for client response, after its events.

**Delivered watermark and replay**: the sender tracks W, the greatest command timestamp up to which the brokers have acknowledged every event, and writes it to the compacted topic `vex-core.delivered` (at most every 100 ms while it advances, and at the end of a shutdown). Every start reads W before the pipeline starts, after checking the topics, then replays the journal: the events handler re-publishes the events of every replayed command above W, which a death may have left undelivered, and sends no gateway response for any replayed command. So a crash, a kill or a shutdown past its delivery grace loses no event; Kafka gets duplicates of the commands after the last W written.

## Processing Pipeline

### Pipeline Stages and Dependencies

```
Seq: 0     1     2     3     4     5     6     7     8     9     ...
     │     │     │     │     │     │     │     │     │     │
     ▼     ▼     ▼     ▼     ▼     ▼     ▼     ▼     ▼     ▼
┌─────────────────────────────────────────────────────────────┐
│                 Journaling (Sequential)                      │
└───────┬───┬───────┬───────┬───────┬───────┬───────┬─────────┘
        │   │       │       │       │       │       │
        ▼   ▼       ▼       ▼       ▼       ▼       ▼
   ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
   │ Risk R1 │ │ Risk R1 │ │ Risk R1 │ │ Risk R1 │  (Parallel)
   │ Shard 0 │ │ Shard 1 │ │ Shard 2 │ │ Shard 3 │
   └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘
        └──────┬────┴──────┬────┴──────┘
               │           │
        ┌──────▼──────┐    │        (Barrier: All Risk R1 complete)
        │             │    │
        ▼             ▼    ▼
   ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
   │Matching │ │Matching │ │Matching │ │Matching │  (Parallel)
   │ Shard 0 │ │ Shard 1 │ │ Shard 2 │ │ Shard 3 │
   └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘
        └──────┬────┴──────┬────┴──────┘
               │           │
               │           │        (Barrier: All Matching complete)
               ▼           ▼
   ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
   │ Risk R2 │ │ Risk R2 │ │ Risk R2 │ │ Risk R2 │  (Parallel)
   │ Shard 0 │ │ Shard 1 │ │ Shard 2 │ │ Shard 3 │
   └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘
        └──────┬────┴──────┬────┴──────┘
               │           │
               │           │        (Barrier: All Risk R2 complete)
               ▼           ▼
        ┌──────────────────────┐
        │   Events Handler     │       (Sequential)
        │  (Kafka + Gateway)   │
        └──────────────────────┘
```

### Dependency Barriers

**Barrier 1** (After Journaling → Before Risk R1):
- All events must be journaled before risk validation
- Ensures audit trail completeness

**Barrier 2** (After Risk R1 → Before Matching):
- All risk shards must complete fund locking
- Prevents race conditions in balance validation

**Barrier 3** (After Matching → Before Risk R2):
- All matching must complete before settlement
- Ensures trade events are fully generated

**Barrier 4** (After Risk R2 → Before Events):
- All settlement must complete before publishing
- Guarantees external consumers see consistent state

### Wait Strategy: BusySpin

**Implementation**: Processors continuously poll their barrier without yielding the CPU.

**Rationale**:
- Eliminates kernel scheduler latency
- Guarantees deterministic wakeup latency
- Sacrifices CPU utilization for latency predictability

**Trade-offs**:
- High CPU usage even under low load
- Requires dedicated cores (core pinning)

### End-to-End Processing Examples

The following examples trace order processing through all pipeline stages with concrete values.

**Assumptions**:
- Market ID: 1 (BTC/USD)
- Base asset: BTC (ID: 1), Quote asset: USD (ID: 2)
- 4 shards for both Risk and Matching engines
- Taker fee: 10 bps (0.1%), Maker fee: 5 bps (0.05%)
- Market slippage: 50 bps (0.5%)
- Amounts in whole units with no native scaling, so `quote_cost(price, size) = price * size` (real markets scale; see Balance Locking Logic)

#### Example 1: GTC Limit Order (Maker - Rests on Book)

**Scenario**: User 100 places GTC BID to buy 5 BTC at $50,000 via Gateway 3.

**Stage 0: Gateway Fragment Handler**
- Gateway 3 receives order from client
- `FragmentHandler` decodes OrderCommand:
  ```
  command: PlaceOrder
  client_order_id: 12345 (from client)
  user_id: 100
  market_id: 1
  price: 50000
  size: 5
  side: Bid
  time_in_force: GTC
  ```
- Field transformations:
  ```
  status = Processing
  order_id = 3 (gateway_id stored temporarily)
  ```
- Publish to disruptor ring buffer

**Stage 1: Journaling Processor**
- Sequence claimed: 1000
- Timestamp assignment: `cmd.timestamp = 1735696793249403904`
- Snowflake generation:
  ```rust
  order_id = snowflake.generate(cmd.timestamp, 3)  // Input: timestamp, gateway_id
  // Output: 0x0001A2B3C4D50003
  // Breakdown: timestamp=0x0001A2B3C4D5, sequence=0, gateway=3
  ```
- Archive publication: Publishes to `aeron:ipc` stream 2001
- Result:
  ```
  order_id: 0x0001A2B3C4D50003 (assigned)
  timestamp: 1735696793249403904
  status: Processing
  ```

**Stage 2: Risk Engine R1 (Pre-Processing)**
- Shard selection: `user_id & 3 = 100 & 3 = 0` → **Shard 0**
- Balance check (user 100, asset USD):
  ```
  required = price * size = 50000 * 5 = 250,000 USD
  available_usd = 1,000,000 USD
  locked_usd = 0 USD
  ```
- Sufficient funds: Yes
- Fund locking:
  ```
  available_usd: 1,000,000 → 750,000
  locked_usd: 0 → 250,000
  ```
- Result:
  ```
  status: Processing (unchanged)
  Proceeds to matching
  ```

**Stage 3: Matching Engine**
- Shard selection: `market_id & 3 = 1 & 3 = 1` → **Shard 1**
- Check orderbook asks for crossing orders
- Best ask: 50,500 (no cross with bid 50,000)
- GTC order rests on book:
  ```rust
  Order {
      order_id: 0x0001A2B3C4D50003,
      user_id: 100,
      price: 50000,
      size: 5,
      side: Bid,
  }
  ```
- Insert into bids tree at price level 50,000
- Update PriceCache:
  ```
  best_bid: 49,000 → 50,000
  best_ask: 50,500 (unchanged)
  ```
- Result:
  ```
  status: Placed
  events: None (no matches)
  l2_data: Some(L2MarketData) (orderbook snapshot)
  ```

**Stage 4: Risk Engine R2 (Post-Processing)**
- Shard 0 processes user 100
- No MatcherTradeEvents (no trades occurred)
- Update balance fields in command for response:
  ```
  balance[0] = UserBalance { available: 0, locked: 0 }        // BTC unchanged
  balance[1] = UserBalance { available: 750,000, locked: 250,000 }  // USD after R1 lock
  ```
- Result:
  ```
  status: Placed (unchanged)
  balance fields populated for client response
  ```

**Stage 5: Events Handler**
- Extract gateway_id: `Snowflake::gateway_from_id(0x0001A2B3C4D50003) = 3`
- Publish BalanceEvents to Kafka: topic `balances`, keys `100:<base asset>` and `100:<quote asset>`
- Publish OrderEvent to Kafka: topic `orders`, key `0x0001A2B3C4D50003` (in decimal)
- Publish L2 snapshot to Kafka: topic `orderbook`, key `1`
- Gateway response via Aeron:
  ```rust
  publications.publish_response(&cmd)
  // Sends to gateway 3's MDC control port
  ```
- Client receives confirmation: Order 0x0001A2B3C4D50003 placed successfully

**Final State**:
- User 100 balance: 750,000 available USD, 250,000 locked USD
- Orderbook: 5 BTC bid at 50,000 (order 0x0001A2B3C4D50003)
- PriceCache: best_bid = 50,000

---

#### Example 2: Market IOC Order (Taker - Sweeps Book)

**Scenario**: User 200 places IOC BID to buy 3 BTC at market price via Gateway 5. Orderbook has:
- Ask 1: 2 BTC @ 50,100 (user 101, order 0x0001A2B3C4D40101)
- Ask 2: 2 BTC @ 50,200 (user 102, order 0x0001A2B3C4D40202)

**Stage 0: Gateway Fragment Handler**
- Gateway 5 receives market buy order
- `FragmentHandler` decodes:
  ```
  command: PlaceOrder
  client_order_id: 67890
  user_id: 200
  market_id: 1
  price: u64::MAX (market buy indicator)
  size: 3
  side: Bid
  time_in_force: IOC
  ```
- Field transformations:
  ```
  status = Processing
  order_id = 5 (gateway_id)
  ```

**Stage 1: Journaling Processor**
- Sequence: 1001
- Snowflake: `order_id = 0x0001A2B3C4D60005` (gateway 5)
- Timestamp: 1735696793249404928
- Archive publication

**Stage 2: Risk Engine R1**
- Shard selection: `200 & 3 = 0` → **Shard 0**
- Market order handling:
  ```
  best_ask = price_cache.get_best_ask(1) = 50,100
  slippage = 50 bps = 0.005
  slippage_adjustment = floor(50,100 * 50 / 10,000) = floor(250.5) = 250
  conservative_price = 50,100 + 250 = 50,350
  ```
- Price mutation: `cmd.price = 50,350` (conservative estimate)
- Balance check:
  ```
  required = 50,350 * 3 = 151,050 USD
  available_usd = 500,000 USD
  ```
- Fund locking:
  ```
  available_usd: 500,000 → 348,950
  locked_usd: 0 → 151,050
  ```

**Stage 3: Matching Engine**
- Shard: `1 & 3 = 1` → **Shard 1**
- IOC matching process:

**Match 1** (Ask @ 50,100):
```
Taker order: 3 BTC @ conservative 50,350
Maker order: 2 BTC @ 50,100 (user 101)
Fill: 2 BTC @ 50,100
```
- Create MatcherTradeEvent:
  ```rust
  MatcherTradeEvent {
      price: 50,100,
      size: 2,
      maker_user_id: 101,
      matched_order_id: 0x0001A2B3C4D40101,
      active_order_completed: false,  // Taker has 1 BTC remaining
      matched_order_completed: true,  // Maker fully filled
      next_event: Some(Box::new(...)),  // Link to next match
      maker_balance: [
          UserBalance { available: 0, locked: 2 },  // BTC (will be settled in R2)
          UserBalance { available: 0, locked: 0 }   // USD
      ]
  }
  ```
- Remove maker order 0x0001A2B3C4D40101 from book
- Taker remaining: 1 BTC

**Match 2** (Ask @ 50,200):
```
Taker remaining: 1 BTC
Maker order: 2 BTC @ 50,200 (user 102)
Fill: 1 BTC @ 50,200
```
- Create MatcherTradeEvent (linked):
  ```rust
  MatcherTradeEvent {
      price: 50,200,
      size: 1,
      maker_user_id: 102,
      matched_order_id: 0x0001A2B3C4D40202,
      active_order_completed: true,   // Taker fully filled
      matched_order_completed: false, // Maker has 1 BTC left
      next_event: None,  // Last event
      maker_balance: [
          UserBalance { available: 0, locked: 2 },
          UserBalance { available: 0, locked: 0 }
      ]
  }
  ```
- Update maker order: size 2 → 1
- IOC fully filled, no remainder to cancel

**Final matching state**:
```
status: Filled
events: Some(Box<MatcherTradeEvent>) → linked list of 2 events
l2_data: Updated orderbook snapshot
```

**Stage 4: Risk Engine R2 (Settlement)**

**Taker Settlement** (User 200, Shard 0):
- Initial locked: 151,050 USD
- Process event chain:

Event 1:
```
Traded: 2 BTC @ 50,100
Cost: 2 * 50,100 = 100,200 USD
Taker fee: 2 * 10 bps = 0.002 BTC
Unlock quote: locked_usd -= 100,200
Receive base: available_btc += (2 - 0.002) = 1.998 BTC
```

Event 2:
```
Traded: 1 BTC @ 50,200
Cost: 1 * 50,200 = 50,200 USD
Taker fee: 1 * 10 bps = 0.001 BTC
Unlock quote: locked_usd -= 50,200
Receive base: available_btc += (1 - 0.001) = 0.999 BTC
```

Price improvement refund:
```
Conservative price: 50,350
Actual spent: 100,200 + 50,200 = 150,400
Refund: 151,050 - 150,400 = 650 USD
Unlock refund: locked_usd -= 650
Return to available: available_usd += 650
```

Final taker balance (after settlement):
```
BTC: available = 2.997, locked = 0
USD: available = 348,950 + 650 = 349,600, locked = 0
```

Update command balance fields:
```
balance[0] = UserBalance { available: 2.997 BTC, locked: 0 }
balance[1] = UserBalance { available: 349,600 USD, locked: 0 }
```

**Maker Settlement** (User 101, Shard 1):
```
Sold: 2 BTC @ 50,100
Revenue: 2 * 50,100 = 100,200 USD
Maker fee: 100,200 * 5 bps = 50.1 USD
Unlock base: locked_btc -= 2
Receive quote: available_usd += (100,200 - 50.1) = 100,149.9 USD
```

Maker balance in MatcherTradeEvent updated by R2 Shard 1:
```
maker_balance[0] = UserBalance { available: prev_btc, locked: prev_locked - 2 }
maker_balance[1] = UserBalance { available: prev_usd + 100,149.9, locked: 0 }
```

**Maker Settlement** (User 102, Shard 2):
```
Sold: 1 BTC @ 50,200
Revenue: 50,200 USD
Maker fee: 50,200 * 5 bps = 25.1 USD
Unlock base: locked_btc -= 1
Receive quote: available_usd += (50,200 - 25.1) = 50,174.9 USD
```

Maker balance in MatcherTradeEvent updated by R2 Shard 2:
```
maker_balance[0] = UserBalance { available: prev_btc, locked: prev_locked - 1 }
maker_balance[1] = UserBalance { available: prev_usd + 50,174.9, locked: 0 }
```

**Stage 5: Events Handler**
- Extract gateway: `Snowflake::gateway_from_id(0x0001A2B3C4D60005) = 5`
- Publish TradeEvents to Kafka (2 events, one per maker-taker pair)
- Publish BalanceEvents (3 users: taker + 2 makers)
- Publish L2 snapshot
- Gateway response to client: Order filled, 2.997 BTC received, 349,600 USD available

---

#### Example 3: Cancel Order

**Scenario**: User 100 cancels their resting GTC order (0x0001A2B3C4D50003) via Gateway 3.

**Stage 0: Gateway Fragment Handler**
- Gateway 3 receives cancel request
- `FragmentHandler` decodes:
  ```
  command: CancelOrder
  order_id: 0x0001A2B3C4D50003 (order to cancel)
  user_id: 100 (from client)
  market_id: 1
  ```
- Field transformations:
  ```
  status = Processing
  user_id = 3 (gateway_id overwrite for response routing)
  ```
- Note: order_id is NOT modified (contains target order ID)

**Stage 1: Journaling Processor**
- Check command type: CancelOrder
- Skip Snowflake generation: `order_id` already contains target
- Timestamp: `cmd.timestamp = 1735696793249406000`
- Archive publication

**Stage 2: Risk Engine R1**
- Shard: `100 & 3 = 0` → **Shard 0**
- Cancel orders skip R1 processing (no fund reservation needed)
- Status remains `Processing`

**Stage 3: Matching Engine**
- Shard: `1 & 3 = 1` → **Shard 1**
- Locate order in orderbook:
  ```
  price_level = 50,000 (from order-price map)
  orders = bids.get(50,000).orders  // VecDeque
  ```
- Binary search:
  ```rust
  orders.binary_search_by_key(&0x0001A2B3C4D50003, |o| o.order_id)
  // Returns: Ok(pos) where pos is index in VecDeque
  ```
- Remove order at position:
  ```rust
  removed_order = orders.remove(pos)
  total_volume -= removed_order.size  // 5 BTC
  ```
- Update command fields:
  ```
  cmd.price = 50,000 (from removed order)
  cmd.size = 5 (from removed order)
  cmd.status = Cancelled
  ```
- If price level now empty, remove from tree
- Update PriceCache:
  ```
  best_bid: 50,000 → 49,500 (next best bid)
  ```

**Stage 4: Risk Engine R2**
- Shard: `100 & 3 = 0` → Shard 0
- Unlock funds for cancelled order:
  ```
  locked_usd: 250,000 → 0
  available_usd: 750,000 → 1,000,000
  ```
- Update balance in command:
  ```
  balance[0] = UserBalance { available: 0, locked: 0 }  // BTC unchanged
  balance[1] = UserBalance { available: 1,000,000, locked: 0 }  // USD unlocked
  ```

**Stage 5: Events Handler**
- Extract gateway from user_id: `cmd.user_id = 3` (gateway stored here for CancelOrder)
- Publish CancelEvent to Kafka
- Publish BalanceEvent (user 100)
- Publish L2 snapshot
- Gateway response: Order 0x0001A2B3C4D50003 cancelled, 250,000 USD unlocked

**Final State**:
- User 100: 1,000,000 USD available, 0 locked
- Orderbook: Order removed, best bid now 49,500
- Order lifecycle: Placed → Cancelled

---

**Key Observations**:

1. **Field Repurposing**: Gateway ID stored in `order_id` (PlaceOrder) or `user_id` (CancelOrder) before journaling
2. **Shard Routing**: User-based for Risk, Market-based for Matching
3. **Price Mutation**: Market orders have price mutated in Risk R1 for conservative fund locking
4. **Event Chains**: Multi-level fills create linked MatcherTradeEvents processed sequentially in R2
5. **Binary Search**: Cancel operations benefit from monotonic order_id insertion order
6. **R2 Balance Updates**: R2 updates balance fields in OrderCommand for ALL order types (placed, filled, cancelled) to provide current state in gateway response
7. **Settlement Coordination**: R2 shards independently process events for their users, updating both taker balance (in main OrderCommand) and maker balances (in MatcherTradeEvent.maker_balance array)

## Concurrency and Sharding

### Sharding Design

**Two Orthogonal Sharding Dimensions**:

1. **User Sharding** (Risk Engines): Partition users across shards
2. **Market Sharding** (Matching Engines): Partition markets across shards

**Bit-Level Shard Assignment**:

```rust
// For N shards (must be power of 2)
// shard_mask = N - 1

User shard:    user_id & shard_mask
Market shard:  market_id & shard_mask
```

**Distribution Properties**:
- Uniform distribution (assuming random IDs)
- Deterministic assignment (same ID always maps to same shard)
- Fast computation (single bitwise AND operation)

### Cross-Shard Coordination

**Scenario**: User A (Shard 0) trades with User B (Shard 1) on Market X (Shard 2).

**Processing Flow**:
```
1. Risk R1 Shard 0: Lock funds for User A   (independent, mutex held)
2. Risk R1 Shard 1: Lock funds for User B   (independent, mutex held)
3. Matching Shard 2: Generate trade event   (reads PriceCache only)
4. Risk R2 Shard 0: Settle User A balance   (independent, mutex held)
5. Risk R2 Shard 1: Settle User B balance   (independent, mutex held)
```

**Key Property**: Each shard operates on disjoint sets of users/markets. Risk engines acquire locks on their balance stores as needed during processing.

### Memory Ordering and Barriers

**Barrier Implementation** (simplified):
```rust
pub struct SequenceBarrier {
    cursor: AtomicU64,  // Last published sequence
    dependencies: Vec<AtomicU64>,  // Sequences to wait for
}

impl SequenceBarrier {
    pub fn wait_for(&self, sequence: u64) -> u64 {
        loop {
            let available = self.get_highest_published_sequence();
            if available >= sequence {
                return available;
            }
            // BusySpin: tight loop, no yielding
        }
    }
}
```

**Memory Ordering**:
- Publishers use `Ordering::Release` when updating cursors
- Consumers use `Ordering::Acquire` when reading cursors
- Guarantees: Writes before Release are visible after Acquire

## Transport Layer

### Deployment Model

Gateways are independent processes at different geo-locations, each operating its own Aeron MediaDriver.

### Stream Architecture

Aeron uses unidirectional streams. Bidirectional communication requires paired publications and subscriptions.

**Stream IDs**:

```rust
ALL_GATEWAYS_STREAM_ID: 1001    // Handshake channel
DUOLOGUE_STREAM_ID: 1002        // Per-gateway bidirectional pairs
RECORDING_STREAM_ID: 2001       // Archive recording
REPLAY_STREAM_ID: 2002          // Archive replay
```

Stream IDs multiplex independent message streams over shared connections.

### Multi-Destination-Cast (MDC)

**Problem**: NAT traversal for server-to-client messaging.

**Solution**: MDC allows a publication to send unicast UDP to multiple registered destinations via a control port.

**How it works**:
- Server creates publication with `control` parameter specifying control endpoint
- Clients register with control endpoint (dynamic mode) or server adds destinations manually (manual mode)
- Server sends unicast packets to each registered client's address/port
- NAT treats packets as responses to client's outbound registration, allowing them through

**VEX-CORE Usage**: Core creates MDC publication in dynamic mode for handshake responses. Gateways auto-register when connecting to handshake stream.

**Channel URI**:
```
aeron:udp?control={core_address}:{control_port}|control-mode=dynamic
```

### Session ID Isolation

**Purpose**: Session IDs isolate streams to prevent cross-gateway message delivery when multiple gateways share the same stream ID.

**Allocation Strategy**: Random selection from reserved range `[reserved_session_id_low, reserved_session_id_high)` to prevent sequential session ID guessing.

**Per-Gateway Sessions**: Each gateway receives a unique session ID during handshake. All subsequent publications and subscriptions for that gateway use this session ID.

**Channel URI with Session**:
```
aeron:udp?endpoint={address}:{port}|session-id={session}
```

### Port Allocation

**PortAllocator**: Randomly selects ports from `[base_gateway_port, base_gateway_port + max_gateways)`.

**Why Random**: Reduces effectiveness of sequential port scanning. Avoids predictable port assignment patterns.

**Collision Detection**: Tracks allocated ports in `Session` struct. Allocation loops until finding unused port or reaching max attempts.

**Two Ports Per Gateway**:
- Data port: Gateway sends orders to core
- Control port: Core sends responses to gateway via MDC

### Gateway Handshake Protocol

**Phase 1 - Handshake**:

1. Gateway publishes `HELLO gateway-{id} {credential} {encryption_key}` to `ALL_GATEWAYS_STREAM_ID`:
   `credential` is the shared `gateway_authentication_key` (a cleartext bearer credential);
   `encryption_key` is a fresh random i32 per handshake. It sends HELLO only once both its
   publication and its MDC reply subscription are connected: the core answers once, and a reply
   sent before the subscription joins is lost
2. Core authenticates the credential (when enabled) and validates gateway_id < MAX_GATEWAYS
3. Core allocates session_id and two ports (data, control)
4. Core responds `{hello_session} gateway-{id} ACCEPT {data_port} {control_port} {session_id XOR encryption_key}`
   via MDC publication; the mask is never the credential, so ACCEPT cannot leak it

**Phase 2 - Duologue Establishment**:

5. Core creates:
   - Subscription for gateway orders: `aeron:udp?endpoint={core}:{data_port}|session-id={session}`
   - MDC publication for responses: `aeron:udp?control={core}:{control_port}|control-mode=dynamic|session-id={session}`

6. Gateway creates:
   - Publication for orders: `aeron:udp?endpoint={core}:{data_port}|session-id={session}`
   - Subscription for responses: `aeron:udp?control={core}:{control_port}|control-mode=dynamic|session-id={session}`

7. Connection established

**Duologue**: Paired publication/subscription for bidirectional gateway-core communication.

### Image Callbacks

**Available Image**: Fires when remote publication connects to subscription. Used to validate expected session ID and log connection establishment.

**Unavailable Image**: Fires when remote publication disconnects. Triggers gateway cleanup via mpsc channel to GatewayManager.

**Asynchronous Close**: `subscription.close()` completes on Aeron's conductor thread, which may call the image handlers until then, so they move into the close notification, which releases them. The fragment handler runs only inside `poll`, on the server thread, so closing a session releases it at once, and with it the session's clone of the ring producer. The pipeline joins its consumers when its last producer drops, and that must happen on the server thread, which the process waits for: at shutdown the notification runs after the server thread has dropped its own producers, or never once the Aeron client is torn down.

### Publications Array for Response Routing

**Problem**: Events handler must route responses to originating gateway based on gateway_id in order_id.

**Structure**:
```rust
pub struct Publications {
    gateways: [ArcSwapOption<AeronPublication>; MAX_GATEWAYS + 1],
}
```

**ArcSwapOption Usage**:
- Lock-free concurrent reads in events handler hot path
- Atomic swaps during gateway connect/disconnect
- Index `[0..MAX_GATEWAYS-1]`: per-gateway response publications
- Index `[MAX_GATEWAYS]`: archive publication for journaling

**Gateway ID Extraction**: Lower 4 bits of order_id contain gateway_id, enabling O(1) publication lookup.

### Aeron Archive Integration

**Channel Configuration**:

Archive operates using `aeron:ipc` for local communication since Archive and Core share the same MediaDriver.

```rust
RECORDING_CHANNEL: "aeron:ipc"
RECORDING_STREAM_ID: 2001
REPLAY_STREAM_ID: 2002
```

**Control Channels**:

- `request_control_channel`: Archive listens for commands (e.g., `aeron:udp?endpoint=localhost:8010`)
- `response_control_channel`: Archive sends responses (e.g., `aeron:udp?endpoint=localhost:0`)
- `recording_events_channel`: Archive publishes lifecycle events

Port 0 indicates OS-allocated ephemeral port.

**Recording Operations**:

Journaling processor publishes OrderCommands to `RECORDING_STREAM_ID` on `aeron:ipc`. Archive subscribes to this channel and persists messages to disk.

- `start_recording()`: Creates new recording on specified channel/stream, returns `subscription_id`
- `extend_recording()`: Appends to existing recording after replay completes

**Replay Operations**:

1. `list_recordings_for_uri()`: Queries Archive for recordings matching channel/stream. Returns recording descriptors containing:
   - `recording_id`: Unique identifier for the recording
   - `start_position`: Byte offset where recording begins
   - `stop_position`: Byte offset where recording ends
   - `session_id`: Original session ID of recorded publication

2. `start_replay()`: Instructs Archive to replay recording to specified replay channel/stream. Archive creates a publication to the replay channel and sends recorded data.

**Replay Subscription**:

Core creates subscription on replay channel with session ID returned by `start_replay()`:

```rust
let replay_channel = format!("aeron:ipc?session-id={replay_session_id}");
subscription = aeron.add_subscription(replay_channel, REPLAY_STREAM_ID);
```

Session ID isolation ensures replay stream doesn't interfere with concurrent live recording.

**Replay Loop**:

Polls replay subscription, processing fragments and publishing to disruptor. Tracks position from `start_position` to `stop_position` in fixed frame size increments. Busy-spins when no fragments available.

**Startup**: every start with an archive configured replays the journal; there is no start without replay.
1. Calls `list_recordings_for_uri()` to find the recordings, and reads the journal tail: the last command of the newest recording that holds any
2. Calls `start_replay()` on the newest recording, unless it holds no command (an empty archive starts fresh)
3. Creates subscription to receive replayed messages
4. Polls subscription, publishing each message to disruptor, then waits for the pipeline to drain: it fails only after 10 s without progress, since the events stage may be re-publishing to a slow Kafka
5. Calls `extend_recording()` to append future messages to the replayed recording, or `start_recording()` for a fresh start

During the replay, journaling skips every command (they are journaled already), and the events handler re-publishes only the commands above the delivered watermark and answers no gateway (see Events Handler).

### Connection Lifecycle

**Connect**:
1. Handshake processed by `HandshakeMessageHandler`
2. `GatewayManager` allocates session/ports
3. Creates duologue with dedicated subscription/publication pair
4. Stores publication in `Publications` array for response routing
5. Adds to `Session` tracking

**Disconnect**:
1. `ImageUnavailableHandler` fires on connection loss
2. Sends gateway_id to cleanup channel (mpsc)
3. `GatewayManager.poll()` receives cleanup request
4. Closes duologue, removes publication, releases resources

**Shutdown** (on the server thread, once the shutdown flag is set):
1. Closes every gateway duologue, connected or not, releasing its ring producer (see Asynchronous Close), then the handshake subscription and its handlers
2. Drains the pipeline, then waits until the archive has recorded everything journaling offered
3. Stops archive recording via `stop_recording_subscription()` and closes the archive connection
4. Drops the server, with its Aeron client, then the last ring producer: the pipeline joins its consumers, and the events stage delivers what it holds within its 20 s grace and writes the delivered watermark

### Message Encoding

Fixed 64-byte OrderCommand structure serialized via SBE (Simple Binary Encoding).

**SBE Properties**:
- Zero-copy: operate directly on Aeron buffers
- No heap allocations
- Fixed schema (no runtime negotiation)

### Fragment Handlers

**FragmentHandler**: Callback processing incoming messages from gateway:

```rust
fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) {
    let cmd = decode_order_command(buffer)?;
    if self.producer.try_publish(|target| { *target = cmd.clone(); }).is_err() {
        // Ring full: nothing enqueued, so tell the gateway to retry.
        self.publications.try_publish_response(&busy_response(cmd)); // one offer, never sleeps
    }
}
```

**Polling**: `subscription.poll(fragment_handler, fragment_limit)` processes up to `fragment_limit` fragments per call.

**Per-Gateway Handlers**: Each duologue has dedicated fragment handler with associated gateway_id.

### Performance Characteristics

**Busy-Spin Polling**: Main loop uses `AeronIdleStrategy::busy_spinning_idle()` instead of yielding for deterministic latency.

**Zero-Copy Path**: Gateway → Aeron buffer → FragmentHandler → Disruptor (single copy).

**Thread Safety**: `Publication` is thread-safe. `Subscription` is not (polled from single thread).

**Back-Pressure**: `publication.offer()` returns negative codes when buffer full. VEX-CORE does not queue: a pipeline response is re-offered at most 10 times with 1 ms backoff (`Publications::publish_response`), while the ingress thread's ring-full `Busy` reply is offered once and never sleeps (`Publications::try_publish_response`).

**Gateway Send Outcomes**: `Publisher::try_send_order_command` never sleeps. `BACK_PRESSURED` returns `GatewayError::Busy`, as does an `ADMIN_ACTION` (log-buffer term rotation) that persists across one immediate re-offer; `NOT_CONNECTED` (no core subscribed) returns `GatewayError::NotConnected`; any other failure (`CLOSED`, `MAX_POSITION_EXCEEDED`, `ERROR`) is `GatewayError::SendError`. `GatewayError::Busy` and a `Status::Busy` reply are the same retryable outcome.

## Data Structures and Algorithms

### Orderbook Implementation

**Type Signature**:
```rust
pub struct OrderBook<A: OrderBookSide, B: OrderBookSide> {
    pub asks: A,
    pub bids: B,
    pub market_id: u32,
}
```

The sides are generic and can be implemented using different data structures. The current implementation uses BTreeMap for maintaining sorted price levels.

**Order Structure**:
```rust
pub struct Order {
    pub order_id: u64,
    pub user_id: u64,
    pub price: u64,
    pub size: u64,
    pub side: Side,
    pub timestamp: u64,
}
```

**Price-Time Priority Algorithm**:

1. **Price Priority**: Best bid (highest price) and best ask (lowest price) have priority
2. **Time Priority**: Within same price level, FIFO (first-in-first-out) via VecDeque

### PriceCache for Market Orders

**Problem**: Market orders need opposite side's best price for conservative fund locking, but Risk R1 and Matching engines are in separate shards with independent execution.

**Solution**: Shared read-only cache of best bid/ask prices, enabling cross-shard visibility without synchronization overhead.

**Structure**:
```rust
pub struct PriceCache {
    prices: HashMap<u32, MarketPrice>,
}

pub struct MarketPrice {
    pub best_bid: AtomicU64,
    pub best_ask: AtomicU64,
}
```

**Protocol Importance**:

The PriceCache is essential to the market order execution protocol:

1. **Decoupled Sharding**: Risk engines shard by `user_id`, matching engines shard by `market_id`. Without PriceCache, Risk R1 would need direct orderbook access across shard boundaries, violating the isolation principle.

2. **Fund Safety**: Conservative pricing (best price + slippage) locks MORE funds than minimally required. The price ceiling enforcement in matching (only matches at or below conservative price) guarantees no overspend. IOC/FOK semantics cancel unfilled portions.

3. **Acceptable Staleness**: Stale reads don't violate safety (user locked enough funds). In worst case, order gets fewer fills than expected, never overspends.

4. **Update Protocol**:
```rust
// Matching engine updates after every orderbook change
price_cache.update_prices(market_id, new_best_bid, new_best_ask);
```

Matching engines update prices with `Ordering::Release` after modifying orderbook state. Risk R1 reads with `Ordering::Acquire`, establishing happens-before relationship.

**Memory Ordering**:
- Release-Acquire ensures visibility of orderbook changes before price update
- Risk R1 sees either old or new price atomically (never torn read across bid/ask)
- No locks required in read path (critical for R1 hot path performance)

### Snowflake ID Generation

**Algorithm** (bounds checks omitted):
```rust
pub fn generate(&self, timestamp_ns: u64, gateway: u64) -> Result<u64, SnowflakeError> {
    let since_epoch = timestamp_ns - self.epoch_ns;
    let tick = since_epoch >> TIME_GRANULARITY_SHIFT; // ~1.024 µs
    let sequence = since_epoch & TICK_NANOS_MASK; // nanoseconds within the tick
    Ok((tick << TIMESTAMP_SHIFT) | (sequence << SEQUENCE_SHIFT) | gateway)
}
```

**ID Properties**:
- Unique and strictly increasing across gateways, as the journaling timestamps they derive from are, including across restarts while journaling is enabled
- Sortable by timestamp
- Extractable metadata (gateway ID, timestamp)

## Performance Characteristics

### Latency Profile

**Measurement Methodology**: HDR Histogram with microsecond precision. All measurements in release mode with optimizations.

**End-to-End Latency** (order submission → response):

Performance characteristics vary based on workload, system configuration, and hardware. Typical latency ranges:
- Median (p50): Low microseconds
- p99: Sub-100 microseconds
- Tail latencies influenced by OS jitter and system load

### Throughput

**Sustained Throughput**: System is designed to handle high-volume order flow with sustained throughput capabilities varying based on order patterns, system configuration, and hardware specifications.

### Saturation Point

**Ring Buffer Saturation**: Occurs when publishers produce faster than consumers process.

**Saturation Behavior**:
- Ingress does not wait for a slot: a command that finds the ring full is answered `Status::Busy` and not executed
- No silently dropped orders: the gateway is told the command was not executed and can retry it
- Latency increases with queue depth

### Compiler Optimizations

**Release Profile**:

```toml
[profile.release]
lto = "fat"              # Link-Time Optimization
codegen-units = 1        # Single codegen unit
opt-level = 3            # Maximum LLVM optimizations
strip = true             # Remove debug symbols
debug = false            # No debug info
```

**LTO Impact**:
- **Fat LTO**: Optimizes across all crates
- Enables cross-crate inlining and dead code elimination

## Build and Deployment

### Build System

**Primary Build Tool**: Cargo (Rust's package manager)

**Workspace Structure**:
```
vex-core (workspace root)
├── common          (shared types and utilities)
├── processors      (risk, matching, events processors)
├── orderbook       (orderbook implementation)
├── networking      (Aeron transport layer)
├── server          (Disruptor pipeline and engine)
├── vex-config      (configuration management)
└── xtask           (test and benchmark tools)
```

**Build Commands**:
```bash
# Development build
cargo build --workspace

# Release build
cargo build --workspace --release

# Run tests
cargo test --workspace

# Run performance tests
cargo run --release --bin perf
```

### Makefile Automation

The Makefile provides high-level orchestration for complex workflows involving Aeron MediaDriver management.

**Key Targets**:

| Target | Purpose |
|--------|---------|
| `build` | Compile Rust workspace |
| `aeron` | Download Aeron JAR (if missing) |
| `media-driver` | Start Aeron MediaDriver for VEX-CORE |
| `stop-media-driver` | Stop Aeron MediaDriver |
| `media-driver-gateway` | Start Aeron MediaDriver for Gateway |
| `stop-media-driver-gateway` | Stop Gateway MediaDriver |
| `server` | Start VEX-CORE server (depends on media-driver) |
| `test` | Run integration test suite |

### Configuration Management

**Environment Variables**:
```bash
RUST_LOG=info               # Logging level
VEX_ENV=development         # Required: development | test | production
VEX__KAFKA_BROKER=localhost:9092  # Kafka broker (overrides the configured kafka_broker)
VEX__SCHEMA_REGISTRY_URL=http://localhost:8081  # Schema Registry (overrides the configured schema_registry_url)
```

**Static Configuration** (vex-config crate):
- Market specifications (fees, slippage, tick sizes)
- Core pinning assignments
- Disruptor buffer size
- Shard counts (must be power of 2)

### Monitoring and Observability

**Structured Logging** (tracing crate):

```rust
order_info!(
    "command_ingested",
    cmd,
    stage = "journal",
    order_id = cmd.order_id,
    user_id = cmd.user_id
);
```

**Log Levels**:
- `ERROR`: Unrecoverable errors
- `WARN`: Recoverable errors (order rejection)
- `INFO`: High-level lifecycle events
- `DEBUG`: Detailed processing traces

### Disaster Recovery

**Journal Replay**:

Aeron Archive persists all ingested orders. State reconstruction requires replaying all prior events in sequence. Snapshots are not currently implemented.

**State Reconstruction**:
- All balance changes are deterministic from order sequence
- Orderbook state can be rebuilt by replaying PlaceOrder and CancelOrder commands
- Trade events can be regenerated from matching engine
