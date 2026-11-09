# Kafka Connect read model and the core outbox

Design for trade-vex/vex-core#189. The design review, to which the owner delegated sign-off,
approved it with changes on 2026-11-10. This version records the outcome: the decisions (§8), the
PR plan (§6.1) and the risks (§9) bind the implementation. Nothing here is implemented yet.

Baselines: vex-core `29092e3` (#190), vex-gateway `f34bc7b` (#109), and vex-kafka-sink PR #3 at
`687abe3`. Citations marked #194 are on PR #194 (`2b34071`, which fixes #188); C1 builds on it.
Paths with no repository prefix are in vex-core. Gateway source paths are relative to
`crates/libs/`; the gateway's `scripts/` and `RUNBOOK.md` are at its root.

## Index

1. [Summary](#1-summary)
2. [Current state](#2-current-state)
   - [2.1 The core's Kafka output](#21-the-cores-kafka-output)
   - [2.2 What vex-kafka-sink does](#22-what-vex-kafka-sink-does)
   - [2.3 What the gateway reads](#23-what-the-gateway-reads)
3. [Target architecture](#3-target-architecture)
   - [3.1 Overview](#31-overview)
   - [3.2 Postgres through the JDBC sink](#32-postgres-through-the-jdbc-sink)
   - [3.3 ClickHouse](#33-clickhouse)
   - [3.4 Schema Registry and converters](#34-schema-registry-and-converters)
   - [3.5 Migrations and seeds](#35-migrations-and-seeds)
   - [3.6 Configuration as code](#36-configuration-as-code)
   - [3.7 Errors and dead-letter queues](#37-errors-and-dead-letter-queues)
4. [Outbox](#4-outbox)
   - [4.1 The gap](#41-the-gap)
   - [4.2 Options](#42-options)
   - [4.3 Design](#43-design)
   - [4.4 Why nothing is lost or applied twice](#44-why-nothing-is-lost-or-applied-twice)
   - [4.5 Code it touches](#45-code-it-touches)
5. [Gateway changes](#5-gateway-changes)
6. [PR plan, cutover and retirement](#6-pr-plan-cutover-and-retirement)
   - [6.1 PR plan](#61-pr-plan)
   - [6.2 Bringing up an environment](#62-bringing-up-an-environment)
   - [6.3 Resetting the ledger](#63-resetting-the-ledger)
   - [6.4 Rollback](#64-rollback)
   - [6.5 Retiring vex-kafka-sink](#65-retiring-vex-kafka-sink)
7. [Test plan](#7-test-plan)
8. [Decisions](#8-decisions)
9. [Risks](#9-risks)

## 1. Summary

- **Kafka Connect replaces vex-kafka-sink.**
  - One JDBC sink per table writes Postgres history tables (one row per `(key, timestamp)`) and
    latest-state tables (one row per key).
  - A ClickHouse sink in the same Connect cluster writes ClickHouse raw tables. Materialized views
    build the analytics tables from them.
- **Duplicates and delivery order stop mattering.** One trigger function makes every Postgres write
  last-write-wins on `(key, timestamp)`. In ClickHouse, `ReplacingMergeTree` does the same. The two
  break a tie between copies of one event differently (Q6).
- **The gateway reads balances and orders from the Postgres latest-state tables.** It can only
  `SELECT` from them. It reads ClickHouse only through views that apply `FINAL`, and Redis keeps
  only the gateway's own state.
- **The core re-publishes what a process death lost.**
  - The core tracks a delivered watermark from the broker's acknowledgements and stores it in a
    compacted topic.
  - The core replays the journal at every start (R). Replay re-publishes every command above the
    watermark.
  - The duplicates are bounded, and the sinks absorb them.
- **One home for the read model's files.** Migrations, seeds, connector configurations and the
  Connect image all live in vex-gateway under `readmodel/` (Q1).
- **Five PRs** (§6.1): this document, then G1 (Postgres read model), C1 (core outbox), G2 (gateway
  reads) and G3 (ClickHouse).

## 2. Current state

### 2.1 The core's Kafka output

**Encoding.** `KafkaEventsHandler` (`processors/src/events.rs:716-1299`) publishes the events listed
below after each command.
- **Values** are prost-encoded `trading.proto` messages, framed for the Schema Registry (magic byte,
  schema id, message index). Each subject is named `<topic>-value` and holds the whole
  `trading.proto` (`events.rs:536-579`).
- **Keys** are plain UTF-8 strings (`events.rs:598`).
- **`timestamp`** is the command's journaling time, in nanoseconds since the Unix epoch
  (`processors/src/journaling.rs:52-63`).

**Topics.** Message names refer to `processors/src/protos/trading.proto`.

| Topic | Message | Key | Emitted for | Projection |
|---|---|---|---|---|
| `balances` | `BalanceEvent` (L64-71) | `user_id:asset_id` | the taker after a placement, cancel or fills; each maker once per command, with its last snapshot; a deposit or withdrawal (`events.rs:786-849, 1005-1026`) | latest + history |
| `orders` | `OrderEvent` wrapping `Order` (L33-50) | `order_id` | the taker; each maker per fill (`events.rs:851-963`) | latest + history |
| `orderbook` | `OrderbookEvent` (L78-83); its levels are JSON strings | `market_id` | a placement, cancel or fill that carries an L2 snapshot (`events.rs:1053-1101`) | latest |
| `cancels` | `CancelOrderEvent` (L85-90) | `order_id` | a cancellation (`events.rs:1028-1051`) | history |
| `trades` | `TradeEvent` (L52-62) | `taker_order_id:maker_order_id` | each fill (`events.rs:965-1003`) | history |
| `deposits` | `DepositEvent` (L92-97) | `user_id` | a processed deposit (`events.rs:1103-1127`) | history |
| `withdrawals` | `WithdrawEvent` (L99-104) | `user_id` | a processed withdrawal (`events.rs:1129-1153`) | history |

**Ordering contract** (#190, `events.rs:655-715`):
1. **Per key, partition order is engine order.** One ordered sender thread is fed by a bounded
   channel. The producer is idempotent, with `enable.idempotence=true` and `acks=all`
   (`events.rs:421-440`).
2. **`timestamp` strictly increases per command**, across restarts too. With #194, every start
   stamps live commands above the last command in the journal (`next_timestamp` reads
   `Publications::journal_tail`, #194 `journaling.rs:50-60`).
3. **At most one event per key per command**, on every topic.
4. **That event holds the key's state after the command.** A balance may already include a later
   command that R1 has admitted.

So `(key, timestamp)` is unique within a topic.

**Delivery.**
- While the process runs, nothing is dropped. A Kafka or registry outage back-pressures intake: the
  core answers `Busy`, which the gateway returns as 503.
- A failure that no retry can cure aborts the process (`events.rs:684-698`).
- Known gap: events still queued when the process dies are never re-published (`events.rs:700-703`;
  `RUNBOOK.md:432, 438`). Replay publishes nothing (`events.rs:1162-1170`).

**Stale documentation.** `ARCHITECTURE.md:466-490` and `:1243` still describe per-market topics
(`market-{id}-orders`, `asset-{id}-balances`) and an events stage that replay bypasses. C1 corrects
them (§4.5).

### 2.2 What vex-kafka-sink does

`main` (`e7ca94c`) is an early Kafka Connect and Redis demo. Its `run.sh` sets up Redis sink
connectors with JSON converters on `market-.*-*` topics.

The open stack replaces it with a Rust consumer:
- **#1** `feat/optimisation` (`7210e75`), then **#2** `feat/update-db` (`72b50d0`), then
  **#3** `feat/time-series-db` (`687abe3`).

For each message, the consumer:
- writes Postgres history tables in schema `trading` (`migrations/003`, `005`);
- writes the Redis hashes and sets that the gateway reads: `user:{id}:asset:{a}:balance`,
  `order:{id}` and `user:{id}:orders` (`src/redis_client.rs:37, 68, 93`). It also writes trade
  streams and per-user stats;
- writes ClickHouse raw tables, from which views build per-minute balances, prices and equity
  (`clickhouse_schema.sql`, `src/clickhouse_client.rs`).

The repository also:
- serves a WebSocket market-data API: order book, trades and OHLC candles (`src/websocket.rs`,
  `WEBSOCKET_README.md`);
- holds the DDL for the gateway's `users` (001) and `api_keys` (002). Its `markets` (004) is not the
  table the gateway reads: it has no `quote_scale_k` and names assets instead of numbering them, so
  `ScalingCache` cannot load it (`lib-core/src/cache/scaling_cache.rs:46-47`).

**It cannot consume the core's output as it stands:**
- **Wrong topics.** It subscribes to `market-.*-orders`, `asset-.*-balances` and similar
  (`config.toml:4-10`); the core publishes `orders`, `balances`, and so on.
- **Wrong encoding.** It decodes JSON (`src/event_processor.rs:104`); the core publishes
  registry-framed protobuf.
- **Wrong time unit.** It reads timestamps as milliseconds (`src/clickhouse_client.rs:80`); the
  core's are nanoseconds.
- **At-most-once delivery.** It commits an offset as soon as a message is queued for the batcher
  (`src/main.rs:318`), and logs and drops a message or batch that fails
  (`src/event_processor.rs:77`, `src/main.rs:178`).
- **Weak history tables.** Only `trades` has a uniqueness constraint (`migrations/003`).
  `orders_events` lacks the `order_filled_size`, `order_time_in_force` and `order_status` columns
  that the gateway reads.
- **Unordered Redis writes.** The timestamp-guard fix (`e245e42`, local only, never built) was
  abandoned when the plan moved to Connect.

**Unmerged branches** (none has a PR) hold the rest of the gateway's schema and two more
consumers:
- **`fix/python-consumer`** (`5fbb7e1`, also in `test/consumer-test`) has the only DDL for
  `assets` with `native_scale`, a `markets` table with the scale columns, and
  `user_favorite_markets`:
  - `migrations/004_create_asset_table.sql`, `006_create_markets_table.sql` and
    `007_create_user_favorite_markets_table.sql`;
  - its market-id packing (quote asset in the upper 16 bits) and asset ids (USD 1, BTC 2, ETH 3)
    match the core (`common/src/market_specification.rs:134-147`, `config.dev.yaml:51-60`);
  - the gateway's handler test loads exactly these files, from a developer's local path
    (`lib-api/tests/user_handler_tests.rs:57-67`).
- **`feat/scale-define`** (`11c1e10`, also in `feat/refactor`, `feat/update-schema` and
  `feat/update-topics`) has `004_create_markets_table.sql` with the scale columns and
  `006_create_assets_table.sql`, but packs market ids the other way round (base asset in the upper
  bits).
- **Consumers.** `feat/update-topics` (`a960775`) subscribes to the core's topic names, and
  `fix/python-consumer` replaces the Rust consumer with `trading-events-consumer.py`. Both still
  decode JSON (`src/event_processor.rs:89`, `trading-events-consumer.py:1785`), so neither can
  consume the core's output either.

### 2.3 What the gateway reads

| Store | What | Where (vex-gateway) | Written by |
|---|---|---|---|
| Postgres `trading.balances_events` | latest balance per (user, asset), via `ROW_NUMBER` over history ranked by `timestamp` | `lib-core/src/repository/repository.rs:310-335` | sink |
| Postgres `trading.orders_events` | latest row per order: open and history filters, lookup by id | `repository.rs:184-306, 381-491` | sink |
| Postgres `users`, `api_keys`, `user_favorite_markets`, `markets`, `assets` | auth, API keys, favourites, scale factors (`lib-core/src/cache/scaling_cache.rs:46-47, 91-92`) | lib-auth, lib-middleware, lib-core | gateway / seed |
| Redis `user:{id}:balances`, `user:{id}:asset:{a}:balance` | balances, with Postgres as the fallback | `lib-core/src/cache/user_store.rs:100-178` | sink (nothing writes the index) |
| Redis `order:{id}`, `user:{id}:orders` | orders, with Postgres as the fallback | `lib-core/src/cache/order_store.rs:240-637` | sink |
| Redis sessions, tokens, OTP, OAuth state, rate limits, `api_signature:*`, `vex:order-idempotency:*` | gateway-owned state | lib-auth, lib-middleware, `lib-core/src/order_idempotency.rs` | gateway |
| ClickHouse `balances_last_per_min`, `asset_prices_last_per_min`, `deposit_events`, `withdrawal_events`, `trade_events` | portfolio, PnL and the balance chart | `repository.rs:613-1044` | sink and views |

Two further gaps:
- **Writers the gateway must not have.** The trading repository still contains
  `update_user_balance`, `create_user_balance` (`repository.rs:346-379`) and `create_order`
  (`:435-452`). No production code calls them.
- **No migrations.** `scripts/setup_database.sh:36-49` runs `migrations/*.sql`, but that directory
  does not exist in the gateway repository. The DDL for `assets`, the `markets` scale columns and
  `user_favorite_markets` exists only on unmerged vex-kafka-sink branches (§2.2). No branch creates
  the `orders_events` columns `order_filled_size`, `order_time_in_force` and `order_status`
  (`repository.rs:209`).

## 3. Target architecture

### 3.1 Overview

```
                 protobuf, Schema Registry framed
  vex-core ───────────────────────────────────────► balances orders orderbook cancels
     ▲  │                                            trades deposits withdrawals
     │  └── watermark W ──► vex-core.delivered            │
     │      (compacted; read back at start)               ▼
  journal (Aeron Archive)                   Kafka Connect (ProtobufConverter)
  replay re-publishes                       ├─ JDBC sink ×9 ──► Postgres trading.* ──SELECT──► vex-gateway
  commands above W                          └─ ClickHouse sink ─► raw tables ─MV─► per-minute
                                                                  └──── FINAL views ────┴─► vex-gateway
```

### 3.2 Postgres through the JDBC sink

**Column names.** Columns are the proto fields after Connect's `Flatten` transform. Its delimiter
defaults to `.`, so every connector sets `transforms.flatten.delimiter=_`: `order.status` then
becomes `order_status`, as the gateway already expects. A `ReplaceField` transform, after `Flatten`,
renames the event's timestamp to `event_ts`, so that one guard function serves every table. That
field is `timestamp` on every topic but `orders`, where it is `Order.timestamp`
(`trading.proto` L33-50) and so becomes `order_timestamp`.

| Topic | Latest-state table (PK) | History table (PK) |
|---|---|---|
| `balances` | `trading.balances` (`user_id, asset_id`) | `trading.balance_events` (`user_id, asset_id, event_ts`) |
| `orders` | `trading.orders` (`order_order_id`) | `trading.order_events` (`order_order_id, event_ts`) |
| `orderbook` | `trading.orderbook` (`market_id`) | none (Q4) |
| `cancels` | none; an order is cancelled at most once | `trading.cancel_events` (`order_id, event_ts`) |
| `trades` | none | `trading.trade_events` (`taker_order_id, maker_order_id, event_ts`) |
| `deposits` | none | `trading.deposit_events` (`user_id, event_ts`) |
| `withdrawals` | none | `trading.withdrawal_events` (`user_id, event_ts`) |

**The guard.** Every table gets the same `BEFORE UPDATE` row trigger:

```sql
CREATE FUNCTION trading.keep_newer() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- Last write wins on (key, event_ts); the first copy of a (key, event_ts) stays.
    IF NEW.event_ts <= OLD.event_ts THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END $$;
```

**How connectors write.** Connectors run with `insert.mode=upsert` and `pk.mode=record_value`, so
each write is `INSERT … ON CONFLICT (pk) DO UPDATE`.
- **On a latest-state table**, the trigger turns the upsert into last-write-wins on
  `(key, event_ts)`.
- **On a history table**, the PK contains `event_ts`, so a conflict is always a duplicate. The
  trigger makes it a no-op, so history is insert-only.
- **Why upsert mode.** Insert mode writes a plain `INSERT`, and the first duplicate fails it with a
  unique violation. Upsert mode turns the duplicate into an `UPDATE` that the trigger skips. The
  connector does not mind that no row changed: kafka-connect-jdbc 10.4 and later never check
  update counts (PR #1176 removed the check; `BufferedRecords.executeUpdates` only tests for
  `EXECUTE_FAILED`). The image pins 10.4 or later (§3.6).
- **Order does not matter.** Neither kind of table depends on delivery order. That matters because
  a re-published suffix (§4) puts older copies after newer events in a partition.

There is one connector per table, because `pk.fields` differs between tables: 6 history connectors
and 3 latest-state connectors. `tasks.max` need not exceed the topic's partition count.

**The JDBC URL must not set `reWriteBatchedInserts=true`.**
- pgjdbc does rewrite the connector's upsert batch into one multi-row
  `INSERT … ON CONFLICT DO UPDATE`. Its parser refuses to rewrite only a statement with a bind
  parameter after the `VALUES` list closes. Its test
  `ParserTest.insertBatchedReWriteOnConflictUpdateConstant` covers this statement shape.
- One batch routinely carries the same balance key twice. Postgres rejects such a statement
  ("ON CONFLICT DO UPDATE command cannot affect row a second time"), which fails the task.

**Example connector** (the latest-state balances table):

```json
{
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "topics": "balances",
  "connection.url": "${file:/etc/connect/secrets.properties:pg_url}",
  "table.name.format": "trading.balances",
  "insert.mode": "upsert",
  "pk.mode": "record_value",
  "pk.fields": "user_id,asset_id",
  "auto.create": "false",
  "auto.evolve": "false",
  "transforms": "flatten,ts",
  "transforms.flatten.type": "org.apache.kafka.connect.transforms.Flatten$Value",
  "transforms.flatten.delimiter": "_",
  "transforms.ts.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
  "transforms.ts.renames": "timestamp:event_ts",
  "errors.tolerance": "none",
  "max.retries": "60",
  "retry.backoff.ms": "10000"
}
```

The `orders` connectors differ in their table, `"pk.fields": "order_order_id"` (plus `event_ts` for
history) and `"transforms.ts.renames": "order_timestamp:event_ts"`.

**Grants.** Connect's role has `SELECT`, `INSERT` and `UPDATE` on `trading` (an
`ON CONFLICT DO UPDATE` needs all three); the gateway's role has `SELECT` only. Postgres then
enforces "the gateway never writes balances", instead of a convention.

### 3.3 ClickHouse

**Ingestion (Q2).** The issue asks for ClickHouse's native Kafka ingestion; it cannot read the
core's values.
- **The Kafka table engine** reads the Confluent wire format only for Avro (`AvroConfluent`).
  ClickHouse has no Confluent-Protobuf format: issue #39724 is open, and PR #94750 is a draft.
- **`kafka_schema_registry_skip_bytes`** (25.11) only skips a fixed number of bytes per table. It
  reads neither the schema id nor the message index.
- **Decision.** ClickHouse's own Kafka Connect sink, `clickhouse-kafka-connect`, runs in the same
  Connect cluster with the same converter. That gives one decoder, one way to configure connectors
  and one set of alerts. One connector covers every topic (`topic2TableMap`).

**Tables.** They port `clickhouse_schema.sql` from vex-kafka-sink PR #3, which the gateway already
queries, with these changes:
- **Raw tables.** One per topic, each a `ReplacingMergeTree` with
  `ORDER BY (<key columns>, event_ts)`, partitioned by `toYYYYMM(ts)`.
  - Every proto integer is a `UInt64`, `uint32` included (§3.4).
  - `event_ts` is a `UInt64` in nanoseconds.
  - `ts` is `DateTime64(9) MATERIALIZED fromUnixTimestamp64Nano(toInt64(event_ts))`, for
    partitioning and time queries. It must be `MATERIALIZED`: the sink skips `MATERIALIZED` and
    `ALIAS` columns, but writes `DEFAULT` ones (it inserts with `RowBinaryWithDefaults`).
  - The connector's exactly-once mode stays off (`exactlyOnce=false`). It deduplicates by Kafka
    offset, but the outbox's duplicates arrive at new offsets. `ReplacingMergeTree` collapses both
    kinds.
- **Fields with no column are dropped silently.** The sink writes the table's columns and ignores
  any record field that has none (`writeRecordsRowBinary` iterates the table's columns). A schema
  skew therefore fails loudly only in Postgres (§3.4). The field-coverage test (§7) is the guard
  for ClickHouse.
- **View targets.** Every materialized view writes to a `ReplacingMergeTree` keyed by the source key
  and the minute, with `event_ts` as the version. A duplicate insert into a raw table then cannot
  double count. The sink's per-minute tables already work this way; `market_prices` becomes a view
  over `trade_events`.
- **Units (Q9).** Raw tables keep engine integers. The views that compute prices and equity scale
  them, joining ClickHouse `markets` and `assets` reference tables. The same deploy job seeds those
  as it seeds Postgres (§3.5). The sink's `mv_asset_prices_last_per_min` already joins its own
  `markets` table this way (`clickhouse_schema.sql`).
- **Ties.** Among rows with equal sorting key, and equal version where there is one,
  `ReplacingMergeTree` keeps the last inserted. ClickHouse therefore keeps the last copy of an
  event, where Postgres keeps the first (Q6).
- **Reads.** The gateway reads every table through a view defined as `SELECT … FROM <table> FINAL`.
  Its ClickHouse user may `SELECT` from those views only, so no query can forget `FINAL` (§5).
  - Every view states `SQL SECURITY DEFINER DEFINER = <user>` explicitly. Without the clause the
    view gets empty SQL security and runs with the caller's rights, because
    `ignore_empty_sql_security_in_create_view_query` defaults to true.
  - The clause needs ClickHouse 24.2 or later. Deploy 24.2 or later (any current LTS).

### 3.4 Schema Registry and converters

**Producer side.** Nothing changes. The core registers `trading.proto` under `<topic>-value` on each
topic's first event, using TopicNameStrategy (`events.rs:557-579`).

**Connect worker.**
- `key.converter=org.apache.kafka.connect.storage.StringConverter`.
- `value.converter=io.confluent.connect.protobuf.ProtobufConverter`, with
  `value.converter.schema.registry.url` set.
- The converter finds the message type from the schema id and message index in each value.
- Enums arrive as their value names, such as `STATUS_FILLED`. The gateway's filters already use
  those names (`repository.rs:295-306`).

**Type mapping.** Connect has no unsigned types (Q3).

| proto | Connect | Postgres | ClickHouse |
|---|---|---|---|
| `uint64` | `INT64` (same bits) | `BIGINT` (same bits) | `UInt64` |
| `uint32` (`asset_id`, `market_id`) | `INT64` | `BIGINT` | `UInt64` |
| enum | `STRING` (value name) | `TEXT` | `LowCardinality(String)` |
| `string` (order-book levels as JSON) | `STRING` | `TEXT` | `String` |

- The ProtobufConverter maps `uint32` to `INT64`. The ClickHouse sink refuses a 64-bit field for a
  32-bit column (`validateDataSchema`), and with that check bypassed it wraps silently.
- An `INT64` written to `UInt64` keeps its 8 bytes, so values of 2^63 and above land correctly.

**Compatibility.** Each subject is set to `BACKWARD_TRANSITIVE`, as code (§3.6).
- `FULL` or `FORWARD` would reject a new top-level message in `trading.proto`. The forward check
  sees it as a removed message, which Confluent's Protobuf checker does not accept
  (`SchemaDiff.COMPATIBLE_CHANGES` lacks `MESSAGE_REMOVED`).
- Consumers always decode with the writer's schema, by id, so forward compatibility buys nothing.
  Confluent recommends `BACKWARD_TRANSITIVE` for Protobuf for this reason.
- A schema the registry rejects stalls the core (`RUNBOOK.md:436`), never the sinks.

**Schema changes.**
- A schema change ships in this order: first the migration that adds the column, then the core.
- `auto.create=false` and `auto.evolve=false`: in Postgres a field with no column fails its
  connector loudly (the JDBC sink's `DbStructure.amendIfNecessary` throws), instead of the sink
  inventing DDL.
- The ClickHouse sink drops such a field silently (§3.3).
- So the read-model CI checks that every field of every event message in `trading.proto` has a
  column in its Postgres tables and its ClickHouse raw table (§7). A schema change cannot land
  without its migration.

### 3.5 Migrations and seeds

**One set.** One versioned, forward-only set of migrations covers:
- **Postgres** (G1, `readmodel/migrations/`): the gateway-owned tables, the `trading.*`
  projections, the guard function and the grants;
- **ClickHouse** (G3, `readmodel/migrations/clickhouse/`): the raw tables, the reference tables and
  the views.

It lives in one repository: vex-gateway, under `readmodel/` (Q1). A deploy job applies it with
golang-migrate, which supports both engines.

**Seeds.** A script in `readmodel/seeds/` generates the `markets` and `assets` rows, for Postgres
and ClickHouse, from the core's `config.<env>.yaml` (Q1).
- `ScalingCache` depends on these rows (`lib-core/src/cache/scaling_cache.rs:45-49, 90-94`), and
  the ClickHouse views on their copies (§3.3).
- The deploy job runs it with the migrations. CI runs it against the pinned core.

**When it runs.**
- A deploy job applies the migrations and seeds before Connect and the gateway start.
- CI applies the same files to throwaway databases and runs the gateway's repository tests against
  them.

**Starting point.** Today the real schema is scattered across several places, which are the inputs
to the first migration:
- PR #3's `migrations/001-003, 005` and `clickhouse_schema.sql`. Its `004` `markets` is not used,
  because it lacks `quote_scale_k`;
- `fix/python-consumer` (`5fbb7e1`): `004_create_asset_table.sql`, `006_create_markets_table.sql`
  and `007_create_user_favorite_markets_table.sql`, whose market-id packing matches the core;
- `feat/scale-define` (`11c1e10`) only as a cross-check, since it packs market ids the other way
  round (§2.2);
- the `orders_events` columns the gateway reads that no branch creates (§2.3).

The first migration writes it all down once. Nothing is deployed, so there is no data to migrate.

### 3.6 Configuration as code

**Connectors.** `readmodel/connect/connectors/<name>.json` holds one file per connector.
- The deploy job applies each file with `PUT /connectors/<name>/config`, which creates or updates,
  so applying twice is harmless.
- The same job deletes any connector that has no file.
- Secrets come from Connect config providers (`${file:…}`, with `config.providers=file` in the
  worker configuration) and never appear in the files.

**Connect image.** `readmodel/connect/Dockerfile` builds the Connect worker with pinned plugin
versions: the JDBC sink (10.4 or later, §3.2), the Protobuf converter, the ClickHouse sink and the
Postgres driver.

**Connect's internal topics** (`connect-offsets`, `connect-configs`, `connect-status`) also need
replication factor 3. Connect creates them itself, so the worker configuration sets
`offset.storage.replication.factor`, `config.storage.replication.factor` and
`status.storage.replication.factor` to 3.

**Topics and subject compatibility** live in vex-core under `deploy/kafka/`, because the core
produces that contract (Q1).
- **The 7 event topics:**
  - Partition counts are fixed. Adding partitions later moves keys (`events.rs:709-710`).
  - Replication factor 3, with `min.insync.replicas=2`. That setting is what makes `acks=all`
    durable.
  - Retention is at least the longest connector outage we intend to ride out.
- **`vex-core.delivered`:** compacted, one partition.
- **Subjects:** `BACKWARD_TRANSITIVE` (§3.4).
- **Broker:** `auto.create.topics.enable=false`. A topic deleted while the core runs then stalls it
  (`RUNBOOK.md:437`) instead of being re-created with default settings.

**ACLs**, if the cluster enables them:
- the core: write the 7 event topics and `vex-core.delivered`; read `vex-core.delivered`; describe
  its topics;
- Connect: read the 7 event topics; read and write its internal topics; use its consumer groups.

**Startup check.** The topic settings that no-loss rests on are checked, not trusted.
- With journaling configured, the core describes its topics with the admin client at every start,
  before the pipeline starts (§4.3 item 4).
- It refuses to start, naming the topic and the setting, if:
  - a topic is missing;
  - `vex-core.delivered` does not have exactly one partition, or is not `cleanup.policy=compact`;
  - in Production, an event topic's `min.insync.replicas` is below 2, as other Production-only
    checks do (`vex-config/src/lib.rs:130-140`).
- Production refuses an empty archive channel (`vex-config/src/networking.rs:203-207`), so there
  the check always runs.
- The xtask stack creates its topics from `deploy/kafka/` like any other environment.

### 3.7 Errors and dead-letter queues

**Fail and stop.** `errors.tolerance=none` is the default. A record that fails conversion, a
transform or its SQL fails its task. Its offset is not committed, so nothing is skipped: after a
restart, the connector resumes from that record.

**Transient failures.** For a Postgres failover or a ClickHouse restart, `max.retries` ×
`retry.backoff.ms` (for example 60 × 10 s) rides out the failure before the task fails.

**No dead-letter queue anywhere, ClickHouse included (Q5).**
- With `errors.tolerance=all`, a record routed to a DLQ becomes a missing history row or a stale
  balance. That is the silent loss the core's contract forbids.
- The only producer is the core, with a schema the registry has checked. A poison record therefore
  means a bug or a schema/migration skew, and stopping that connector is the right outcome.

**Alerts and recovery.**
- Alert on a task in state `FAILED` (Connect REST or JMX), and on consumer lag per connector group
  (`connect-<name>`).
- Runbook: fix the cause, then call
  `POST /connectors/<name>/restart?includeTasks=true&onlyFailed=true`.

## 4. Outbox

### 4.1 The gap

When the process dies, it loses everything still held in:
- the sender channel (8,192 events);
- librdkafka's queue (100,000 events);
- requests in flight.

That covers:
- a crash, or `kill -9`;
- an abort (`events.rs:175-186`);
- a shutdown that ran past its 20 s grace (`events.rs:84-86, 502-534`);
- a shutdown that the orchestrator killed.

Replay rebuilds the engine from the journal, but the events stage skips every replayed command
(`events.rs:1162-1170`). Those events never reach Kafka.

### 4.2 Options

| Option | Lost | Duplicated | Cost / problem | Verdict |
|---|---|---|---|---|
| A. Re-publish the whole journal at every start | nothing | every event, at every restart | grows with history | rejected |
| **B. Delivered watermark in a compacted topic; replay re-publishes the commands above it** | nothing | commands after W (about 100 ms, the backlog at death, and the last command before an idle spell) | one tiny record per 100 ms; an atomic counter per in-flight command | **chosen** |
| C. Scan each partition's tail at startup | nothing | almost nothing | must decode tails, own the partitioner, and breaks once retention empties a partition | rejected |
| D. Persisted outbox: a local durable queue, deleted on ack | nothing (host-local) | in flight | a second journal, a disk write per event, lost with the host | rejected |
| E. Kafka transactions, with W written in the same transaction | nothing | none in Kafka | see below | rejected |

Option E fails for four reasons:
- With `transactional.id` set, librdkafka bounds `message.timeout.ms` by the finite
  `transaction.timeout.ms`, so events could expire again. That contradicts the never-expire
  back-pressure (`message.timeout.ms=0`, `events.rs:427-429`).
- Each commit adds latency.
- Every consumer must use `read_committed`.
- The sinks must deduplicate anyway, because Connect itself redelivers.

### 4.3 Design

1. **Watermark.** W is the greatest command timestamp such that every event of every command up to
   it has been acknowledged (`acks=all`). It only advances over closed, fully acknowledged
   commands.
2. **Tracking, with no lock on the event path.**
   - `OutboundEvent` (`events.rs:93-101`) gains a `command_ts` field.
   - The sender keeps an in-flight queue with one entry per command: the consecutive events that
     share a `command_ts` (one command, per contract item 2).
   - The entry holds an `Arc<AtomicU32>` count of outstanding events. The sender increments it as
     it takes each event from the channel, before enqueuing it.
   - That `Arc` is the delivery opaque. `DeliveryOpaque` becomes `Option<Arc<AtomicU32>>` (`()`
     today, `events.rs:455`), with `None` for the W record (item 3). `enqueue<C>`'s
     `DeliveryOpaque = ()` bound (`events.rs:596`) changes with it.
   - The delivery callback (`events.rs:457-488`) decrements the count only on `Ok`. A discarded or
     purged event never decrements it, so W never passes its command.
   - **An entry closes** when the sender receives an event with a greater `command_ts`, or when
     the channel closes at shutdown. Until then the events stage may still submit more of that
     command's events: it submits them one at a time (`events.rs:771-784`), so a count of zero
     alone does not mean the command is done. The events stage is the sole handler of its stage
     (`server/src/engine.rs:302-308`), so all of a command's events reach the channel before any
     event of the next command.
   - Only the sender thread touches the queue. It pops entries from the front while they are
     closed and their count is zero, and sets W to the last popped `command_ts`.
   - An idle sender still wakes every 100 ms: `run_sender` (`events.rs:346-362`) uses
     `recv_timeout` instead of the blocking iterator. While it is stalled on one event, W is
     neither advanced nor written, which can only add duplicates.
   - Closing on the next command's first event leaves the last command before an idle spell open,
     so a death during the spell re-publishes it. An end-of-command message from the events stage
     would close it at once, for one more channel message per command; one duplicate command does
     not justify that.
3. **Persistence.**
   - The sender produces W (a big-endian `u64`) to `vex-core.delivered` (compacted, one partition,
     key `w`), untracked, through the same producer.
   - It does so when W has advanced, at most every 100 ms.
   - **It never blocks or retries on W.** If `producer.send` returns `QueueFull` for the W record,
     the sender skips it; the next 100 ms cycle tries again.
   - **A failed delivery of the W record never aborts.** It is logged, rate-limited, and ignored.
     Today `delivery_failure` (`events.rs:166-173`) aborts on any failure outside a shutdown, so
     the callback tells the W record apart by its `None` opaque.
   - At shutdown, the closed channel closes the last entry. After the shutdown flush, the sender
     writes W once more and flushes that record within the remaining grace.
   - A lost, skipped or late record leaves an older W, which costs duplicates and never leaves a
     gap.
4. **Restart.**
   - Before the pipeline starts, the core checks the topics (§3.6) and reads W, the last record of
     `vex-core.delivered`. `KafkaEventsHandler::new` (`events.rs:722-768`) does both; it is built
     at `server/src/lib.rs:238`.
   - It does so only when journaling is configured (`request_control_channel` non-empty). Without
     journaling there is nothing to replay, and the unit tests that build the handler run without
     Kafka (`events.rs:1619-1631`).
   - An empty topic, or a tombstone for key `w` (§6.4), reads as 0.
   - **Kafka unreachable:** the core waits, logging every 10 s like the sender's stalls.
   - **Topics described and wrong:** the core refuses to start (§3.6).
5. **Re-publication.**
   - `handle_processed_command` skips a replayed command only if its timestamp ≤ W. Today it skips
     every replayed command (`events.rs:1162-1170`).
   - Commands above W publish their events through the same ordered sender, in engine order, before
     the first live command. Startup drains the replay first (`server/src/engine.rs:29-35, 616`).
   - No response is sent for a replayed command.
6. **Re-publication waits for Kafka, and fails startup only when it stops progressing.**
   - While Kafka stalls, the events stage blocks and the ring fills.
   - The replay publisher gives up on a command it cannot place in the ring within
     `DRAIN_TIMEOUT`, 10 s (`networking/src/server/cmd_handler.rs:107-120`,
     `networking/src/server/progress.rs:6`). That wait is per command, so it already measures
     progress.
   - `PipelineProgress::drain` (`networking/src/server/progress.rs:31-42`) gives the whole pipeline
     10 s in total. It runs at the end of `start_replay` (`networking/src/server/mod.rs:737-739`)
     and in `finish_replay` (`server/src/engine.rs:29-35`).
   - With re-publication, that drain depends on Kafka's throughput. The 1,024-command ring
     (`server/src/engine.rs:101`) holds up to some 4,000-8,000 events, which must enter the
     8,192-event channel. The channel blocks only once librdkafka's 100,000-event queue is full too.
     A whole-journal rebuild (a tombstoned W, §6.4) on a slow broker would fail startup in a loop.
   - C1 therefore makes `drain` time out only after `DRAIN_TIMEOUT` without `completed` advancing,
     the shape `drain_replay` already has (`networking/src/server/replay.rs:105-133`). With Kafka
     down nothing advances, so startup still fails after 10 s.
   - Either way, W has not passed the unacknowledged commands, and the W reached so far has been
     persisted, at most 100 ms behind. The next start re-publishes the rest, so restarts converge.
7. **The core always replays (R).**
   - Every start replays the journal; there is no start without replay. A fresh archive holds
     nothing to replay, so the start is fresh.
   - Resetting a ledger is one runbook procedure (§6.3). It deletes the archive's recordings, the
     event topics' data, `vex-core.delivered` and the read model together.
   - #194's journal-tail seeding is unchanged, and stays the only seeding mechanism.
8. **Balance preload** (development and test only) is engine-only, and never published.
   - `preload_balances` (`server/src/lib.rs:142-182`) is never journaled. It runs before
     `engine.run`, while the replay flag is set, as it now is at every start.
   - Journaling therefore does not stamp it: its timestamp stays 0 (`common/src/cmd.rs:175-185`),
     and the gate `ts ≤ W` never publishes it.
   - A development read model shows a preloaded balance only after the key's first journaled
     command.
   - Follow-up issue: replace preload with journaled genesis deposits, applied only to an empty
     archive, so they replay and publish like any deposit.

### 4.4 Why nothing is lost or applied twice

- **No event is lost.** Take any journaled command c.
  - If `ts(c) ≤ W` at restart, W passed c only once c's entry had closed and its count was zero.
    The entry closed only when an event of a later command arrived or the channel closed, so every
    event of c had been submitted by then. Each had then been acknowledged, and was therefore
    durable on `min.insync.replicas` brokers (checked at startup, §3.6).
  - If `ts(c) > W`, replay regenerates c's events and publishes them. Replay keeps c's journaled
    order id and timestamp (`journaling.rs:30-38`; #194 `:31-36`), and the sender never drops while
    the process runs.
  - If the process dies again, W still has not passed the first unacknowledged command, so the next
    start repeats the work. W never decreases, so restarts converge.
- **Duplicates are bounded.** Only commands between the persisted W and the journal tail are
  re-published: at most what the sender's channel and librdkafka's queue hold (§4.1), about 100 ms
  of commands more, and, after an idle spell, the last command before it, whose entry stays open
  until the next command arrives.
- **Duplicates have no effect.**
  - In Postgres, each `(key, event_ts)` is stored once, and a latest-state row holds the greatest
    `event_ts` seen, whatever the arrival order (§3.2).
  - In ClickHouse, `ReplacingMergeTree` on `(key, event_ts)` collapses duplicates; reads go through
    `FINAL` views, and the derived tables are replacing too.
  - Kafka Connect's own at-least-once redelivery is absorbed the same way.
  - This holds for identical copies. Which copy is kept when one differs within contract item 4 is
    Q6. A copy that differs because replay changed an outcome is A2's caveat, below.
- **The contract changes.** After a restart, a partition can hold an older copy after a newer event
  of the same key.
  - "Latest = the last event in the partition" (`events.rs:680-682`) no longer holds.
  - "Latest = the event with the greatest timestamp" still holds. Events with equal
    `(key, timestamp)` are copies of one event, identical unless A2 fails.
  - C1 rewrites that paragraph, adds the A2 caveat, and deletes the known-gap paragraph
    (`:700-703`).
- **Assumptions.**
  - A1: Kafka never runs ahead of the journal. It holds when the core process dies, the outbox's
    main case: the IPC term buffer and the archive survive. It fails only when the media driver,
    the archive or the host dies (Q7, §9).
  - A2: Replay regenerates the same events for each command. Payloads may differ within contract
    item 4 (Q6).
  - **A2 caveat:** replay is not deterministic for market BUYs (#140 item 2, declined in #178 as a
    pipeline-topology decision). R1 reads `PriceCache` while matching writes it, so a replayed
    market BUY can lock a different price than the original run did. See §9.

### 4.5 Code it touches

C1's full scope, on #194. Line numbers are `29092e3`'s unless marked #194; #194 shifts some of
them in `RUNBOOK.md`, `ARCHITECTURE.md` and `networking/src/server/mod.rs`.

| Where | Change |
|---|---|
| `processors/src/events.rs` | `OutboundEvent.command_ts`; the sender's in-flight queue, entry closing, W pop and `recv_timeout(100 ms)`; the delivery opaque `Option<Arc<AtomicU32>>`, decremented only on `Ok`, and `enqueue<C>`'s bound (`:596`); the W record: produced, skipped on `QueueFull`, a failed delivery logged and never aborting; the startup topic check and the W read, with journaling only; the replay gate `ts ≤ W` in `handle_processed_command`, with no response for a replayed command; the contract text (`:655-715`): the latest-state paragraph, the A2 caveat, the known gap deleted (`:700-703`), the `--replay` mentions; the shutdown log line (`:530`), which should say "replay will re-publish them" |
| `networking/src/server/progress.rs` | `PipelineProgress::drain` (`:31-42`) times out only after `DRAIN_TIMEOUT` without progress |
| `networking/src/server/mod.rs` | `VexCoreServer::new` loses its `replay` parameter (`:188`), the `(false, _) => None` arm (#194 `:225-226`) and the "With or without replay" comment (#194 `:217-218`) |
| `networking/src/server/replay.rs` | the `--replay` in `replay_lineage`'s doc comment (#194 `:144`) |
| `server/src/lib.rs` | `start` and `init_internal` lose `replay` (`:85, 225`), and the replay flag always starts enabled (`:229-233`); `init_internal` passes the environment it already has (`:227`) and whether journaling is configured to `KafkaEventsHandler::new` (`:238`) |
| `server/src/engine.rs` | the server thread no longer passes a replay choice to `VexCoreServer::new` (`:584`) |
| `src/main.rs`, `xtask/src/bin/tests/test_server.rs` | the `--replay` flag goes (`src/main.rs:62-81`, `test_server.rs:65-85`): one start path |
| `RUNBOOK.md` | "Kafka is Down" (`:432, 438`); "Stop Service" (`:172-176`): the pipeline drain now times out after 10 s without progress, not 10 s in total; `vex-core.delivered` and the startup topic check; the core waits for Kafka at start, logging every 10 s; a new "Resetting the ledger" section (§6.3); the "Journal Replay" section (`:198-203`) deleted, and the other `--replay` mentions (`:55-56, 342, 458-463`, and #194's `:198`) rewritten |
| `ARCHITECTURE.md` | Events Handler: topics, keys and replay behaviour (`:466-490`, `:1243`; #194 `:428-452`, `:1206`); `--replay` (`:253`; #194 `:215`) |
| `docker/docker-compose.yml`, `docker/DOCKER_README.md` | the `--replay` mentions (`docker-compose.yml:48-50`, `DOCKER_README.md:72`) |
| `xtask/` | topics created from `deploy/kafka/`, with `auto_create_topics_enabled=false` (`xtask/tests/docker-compose.yml:22`); an `ArchivingMediaDriver` service for the restart scenarios, as `make server` runs one (`Makefile:73`). The stack has no archive today (`xtask/tests/docker-compose.yml:61`, `test_server.rs:20-27`); `KafkaVerifier` (`xtask/src/test_framework/kafka.rs:44`) records `(topic, key, timestamp)`; the E2E scenarios of §7 |
| `deploy/kafka/` (new) | the 7 event topics (fixed partitions, replication factor 3, `min.insync.replicas=2`), `vex-core.delivered` (compacted, one partition) and the subjects (`BACKWARD_TRANSITIVE`) (§3.6) |
| `docs/` | an index, per the project rules |

Unchanged: `journaling.rs`, `trading.proto`, topic names and keys.

## 5. Gateway changes

All paths are in vex-gateway.

**Reads.**
- **Balances** (G2): `SELECT … FROM trading.balances WHERE user_id = $1` replaces the `ROW_NUMBER`
  query (`repository.rs:310-335`).
- **Orders** (G2): the open, history, count and by-id queries read `trading.orders`, with an index
  on `(order_user_id, event_ts)` (`repository.rs:184-306, 381-491`).
- **ClickHouse** (G3): every query reads the `FINAL` views instead of the tables (§3.3).
  - Today the raw `deposit_events` and `withdrawal_events` (`repository.rs:790-807`) and
    `trade_events` (`:962-966`) are read without `FINAL`; the per-minute tables already use it
    (`:684, 702, 827`).
  - The portfolio code reads amounts as already-scaled decimals (`repository.rs:630-633`), but the
    sink stored unscaled integers (vex-kafka-sink `src/clickhouse_client.rs:112-114`). It reads the
    scaling views instead (Q9).
  - `lib-core/tests/portfolio_repository_clickhouse.rs` uses the migrated DDL instead of its own.
- **`u64` values:** columns stay `i64`, and reads decode with `as u64` (already the case in
  `user_store.rs:28-30` and `order_store.rs:30-32`; Q3).

**Deleted** (G2).
- **Write and unused methods:** `update_user_balance`, `create_user_balance`, `get_user_balance` and
  `create_order` (`repository.rs:337-379, 435-452`), with their test doubles. Also the unused models
  `deposits.rs`, `trades.rs` and `withdrawals.rs`.
- **Redis projected reads:**
  - `UserBalanceStore::cached_balances` and `parse_balance_hash` (`user_store.rs:100-178, 212-232`);
  - the Redis paths in `OrderStore` (`order_store.rs:240-637`);
  - `UserBalanceCacheConfig`, `OrderCacheConfig` and `with_redis_cache`
    (`lib-server/src/state.rs:147-171`).

  The stores keep their scaling helpers.
- **Test data writes:** `scripts/setup_test_data.sh:111` inserts into `trading.balances_events`.
- **RUNBOOK:** the "Balance Cache" section (`RUNBOOK.md:211-221`).

**Redis keeps only gateway-owned state,** unchanged:
- sessions: `session:*` and `user:<id>:sessions`;
- tokens: `refresh:*`, `blacklist:*` and `token_version:*`;
- email OTP, OAuth state and rate limits (lib-auth);
- `api_signature:*`;
- `vex:order-idempotency:*`.

The "Redis Eviction Policy" section stays (`RUNBOOK.md:198-209`).

**Never writes balances.** No write path remains, and the gateway's role can only `SELECT` from
`trading` (§3.2).

**Freshness.** Reads are as fresh as the connector lag, which is alerted on (§3.7). Today's Redis
path had the same lag.

## 6. PR plan, cutover and retirement

Nothing is deployed, so there is no backfill, dual run or data migration.

### 6.1 PR plan

One commit per PR.

**PR 0, vex-core #195: this document.**
- Merges first, as the decision record.
- With it, file: the journal-durability issue (Q7), the preload-to-genesis issue (§4.3 item 8),
  and a note on #140 item 2 (§9).

**G1, vex-gateway `readmodel/`, the Postgres half.** Independent of C1.
- `readmodel/migrations/`:
  - the gateway-owned tables, including `assets`, `markets` with its scale columns, and
    `user_favorite_markets`;
  - the `trading.*` latest-state and history tables of §3.2, with `BIGINT` throughout, `event_ts`,
    and the `keep_newer()` trigger on every one;
  - the roles and grants: Connect `SELECT, INSERT, UPDATE`; the gateway `SELECT`.
- `readmodel/seeds/`: the generator from the core's configuration (§3.5).
- `readmodel/connect/`: the 9 JDBC connector files; the Dockerfile (JDBC sink 10.4 or later,
  ProtobufConverter and pgjdbc, all pinned); the worker configuration with
  `config.providers=file`.
- A CI stack: Kafka, the registry, Connect, Postgres and the pinned core.
- `scripts/setup_database.sh` repointed at the migrations; the developer-local path in
  `lib-api/tests/user_handler_tests.rs:57-67` deleted.
- Tests: §7, read model (G1).

**C1, vex-core: the outbox.** Independent of G1. It builds on #194: `trading.orders` keys on order
ids, which must be unique across restarts, and #194's journal-tail seeding makes them so.
- Scope: §4.5.
- Tests: §7, vex-core (C1).

**G2, vex-gateway: reads.** After G1. It pins C1's core, so the CI stack exercises re-publication.
- Scope: §5, the Postgres reads and the deletions; the RUNBOOK's "Balance Cache" section removed.
- Tests: §7, vex-gateway (G2).

**G3, vex-gateway: ClickHouse.** After G1; it can run in parallel with G2.
- `readmodel/migrations/clickhouse/`:
  - raw `ReplacingMergeTree` tables per topic: `UInt64` for every proto integer, `event_ts UInt64`,
    `ts DateTime64(9) MATERIALIZED`, `ORDER BY (<key columns>, event_ts)`, partitioned by
    `toYYYYMM(ts)`;
  - `markets` and `assets` reference tables, seeded by the same job;
  - materialized views into `ReplacingMergeTree` targets keyed by (key, minute), with `event_ts` as
    the version; the scaled price and equity views;
  - `FINAL` views with an explicit `SQL SECURITY DEFINER DEFINER = <user>`; the gateway user
    granted only the views.
- One `clickhouse-kafka-connect` connector: `topic2TableMap`, `exactlyOnce=false`,
  `errors.tolerance=none`.
- Gateway: §5, the ClickHouse reads.
- Tests: §7, read model (G3).

G1 is the largest PR. ClickHouse is split into G3 on purpose; it is not folded back (§9).

### 6.2 Bringing up an environment

1. topics and compatibility;
2. migrations and seeds;
3. Connect and its connectors, starting from the earliest offsets;
4. the core;
5. the gateway.

### 6.3 Resetting the ledger

The core always replays (§4.3 item 7), so starting a new ledger means removing the old one
everywhere at once. A partial reset leaves the engine, Kafka and the read model on different
ledgers. This becomes a RUNBOOK section in C1.
1. Stop the gateway, the core and the connectors.
2. Delete the archive's recordings: the journal.
3. Delete the 7 event topics and `vex-core.delivered`, and re-create them from `deploy/kafka/`.
4. Delete the connectors' consumer groups (`connect-<name>`), so they read the new topics from the
   start.
5. Drop the read model, and re-apply the migrations and seeds.
6. Bring the environment up (§6.2).

### 6.4 Rollback

- **Code:** revert the PR. There are no compatibility paths to keep.
- **A running environment:** connectors can be paused or deleted without affecting the core. Kafka
  keeps the events, and a resumed connector continues from its committed offsets.
- **Rebuilding the read model**, keeping the ledger: drop and re-apply the migrations and seeds,
  then do one of these:
  - reset the connectors' offsets to earliest, if retention still covers the history;
  - write a tombstone for the watermark key `w`, so the next start re-publishes the whole journal.
    The stall-based drain lets that take as long as Kafka needs (§4.3 item 6).

### 6.5 Retiring vex-kafka-sink

- Close trade-vex/vex-kafka-sink#1, #2 and #3 with a link to this design once PR 0 merges. The WIP
  commit `e245e42` was never pushed.
- Archive the repository (an owner action) once G2 and G3 are deployed in every environment. Its
  unmerged branches (§2.2), the Python consumer among them, go with it; G1's first migration has
  already taken the DDL it needs from them.
- Its WebSocket market-data API retires with it (Q8).

## 7. Test plan

Each test fails, or cannot run, without the change it covers.

**vex-core (C1), unit tests in `events.rs`:**
- **Watermark tracker:**
  - Acknowledgements arrive out of order across commands. W advances only over the fully
    acknowledged prefix.
  - A command with one unacknowledged event holds W at the previous command.
  - A command's first event is acknowledged before its second is submitted: W does not reach that
    command.
  - The last command before an idle spell: W reaches it only when the next command's first event
    arrives, or when the channel closes.
  - Discarded and purged events never decrement the count, so W never passes them.
- **Watermark record:**
  - It round-trips through encoding; an empty topic and a tombstone read as 0.
  - A `QueueFull` skips the W write, and the next cycle writes it.
  - A failed delivery of the W record is logged and does not abort.
- **Replay gate:**
  - With W = t, replayed commands at or below t publish nothing.
  - Replayed commands above t publish exactly what they published live.
  - Replay publishes no response.
- **Topic check** (the decision over described topic configurations): each of these refuses the
  start: a missing topic; a `vex-core.delivered` with more than one partition, or not compacted; in
  Production, an event topic with `min.insync.replicas=1`. An unreachable broker waits instead.

**vex-core (C1), unit tests in `progress.rs`:** a pipeline that keeps advancing drains past
`DRAIN_TIMEOUT`; one that stops fails after it.

**vex-core (C1), xtask E2E** (Redpanda, the registry and an `ArchivingMediaDriver`,
`xtask/tests/docker-compose.yml`). The orders are limit orders only (A2, §9).
- **Kill with events queued:**
  1. `docker pause` the broker.
  2. Place orders until some events are queued behind the paused broker. Intake need not answer
     `Busy`, which takes more than 108,000 queued events.
  3. `SIGKILL` the core, unpause the broker, and restart the core.
  4. `KafkaVerifier` finds every expected `(topic, key, timestamp)`.
- **Graceful shutdown past the 20 s grace** with the broker paused: the same check.
- **Broker down at start:** startup waits, then completes once the broker is back.
- **Clean shutdown, then a restart:** no events are re-published. This also covers the last
  command, whose entry only the closing channel closes.
- **Missing `vex-core.delivered`:** startup fails, naming the topic.
- **A `vex-core.delivered` that is not compacted:** startup fails.

**Read model (G1, Postgres)**, in the vex-gateway CI stack (Kafka, the registry, Connect and
Postgres, with the pinned core):
- **Migrations:** they apply to empty databases; a second run is a no-op. A test fails if any
  `trading.*` table lacks the `keep_newer` trigger (a query over `pg_trigger`), so a new table
  cannot skip the guard.
- **Guard (SQL tests):** older and equal updates are skipped, newer ones applied, and history
  duplicates are no-ops.
- **Field coverage:** every field of every event message in the pinned `trading.proto` has a column
  in its Postgres tables (§3.4).
- **End to end with the real core:**
  - Every topic lands in its tables (§3.2).
  - The latest-state row is the greatest `event_ts` for its key.
  - Replaying a captured suffix of duplicates and older copies changes neither kind of table.
- **Types:** every proto field lands with the types in §3.4. A balance of 10^19 and an order id
  ≥ 2^63 read back exactly.
- **Skew:** a field with no column fails its connector, and nothing is skipped. After the
  migration, a restart resumes with no gap.
- **Grants:** the gateway's role gets "permission denied" on any `INSERT` or `UPDATE` to
  `trading.*`.

**Read model (G3, ClickHouse)**, in the same CI stack with ClickHouse added:
- **Field coverage:** every field of every event message in the pinned `trading.proto` has a column
  in its ClickHouse raw table. It is the only guard here, since the sink drops unknown fields
  (§3.3).
- **Duplicates** collapse under `FINAL`, and the per-minute views do not double count.
- **Types:** a balance of 10^19 and an order id ≥ 2^63 read back exactly from `UInt64`.
- **Scaling:** the scaled views match `ScalingCache` for the seeded markets.
- **Grants:** the gateway's ClickHouse user is denied every table, so its reads can only go through
  the `FINAL` views.
- **Gateway:** `lib-core/tests/portfolio_repository_clickhouse.rs` runs against the migrated DDL;
  it builds its own today.

**vex-gateway (G2):**
- Repository tests run against the migrated schema.
- The balance and order endpoints are served from Postgres, with Redis empty.
- No production path writes `trading.*` (a grep test).

**Acceptance (the issue's):** the latest-state rows equal the engine's state after each of these:
- a restart, which replays;
- a Kafka outage;
- `kill -9` with events queued.

The engine-side reference is the E2E test client's own ledger, built from command responses and
cross-checked against `KafkaVerifier`'s latest event per key. The orders are limit orders only
(§9).

## 8. Decisions

The design review settled every open question. Each item gives the decision and its reason.
"Changed" marks where the review overruled the reviewed draft's recommendation; "Added" marks a
requirement the review added.

**Q1. Where do migrations live?**
- **Decision:** vex-gateway, under `readmodel/`: the Postgres and ClickHouse migrations, the seeds,
  the connector configurations and the Connect image. Kafka topics and subject compatibility stay
  in vex-core (`deploy/kafka/`), the producer of that contract. A deploy job applies the migrations
  with golang-migrate: plain SQL, both engines, forward-only.
- **Reason:** the gateway is the only reader, so a schema change and its reader land in one PR and
  are tested together. The core has no database.
- **Added:** a script in `readmodel/seeds/` generates the `markets` and `assets` rows from the
  core's `config.<env>.yaml`. The same deploy job runs it, and CI runs it against the pinned core.
  `ScalingCache`'s correctness depends on these rows (`lib-core/src/cache/scaling_cache.rs:45-49,
  90-94`).

**Q2. How does ClickHouse ingest?**
- **Decision:** the `clickhouse-kafka-connect` sink, in the same Connect cluster (§3.3).
- **Reason:** settled from primary sources. ClickHouse has no Confluent-Protobuf format (issue
  #39724 is open; PR #94750 is a draft). Its only workaround, `kafka_schema_registry_skip_bytes`
  (25.11), skips a fixed number of bytes and reads neither the schema id nor the message index.
- **Changed:** the spike that was to decide this, and the Kafka-engine fallback, are dropped.

**Q3. How are `u64` values stored in signed columns?**
- **Decision:** the same bits in `BIGINT`. The gateway decodes with `as u64`, as it already does. A
  `trading.u64(bigint) → numeric` function serves ad-hoc SQL. ClickHouse uses `UInt64`.
- **Reason:** Connect and Postgres have no unsigned 64-bit type, so values ≥ 2^63 arrive as negative
  `INT64`. Examples:
  - a balance above 9.22 units of an asset with 18 decimals;
  - order ids from about mid-2029: the snowflake has 48 bits of 1,024 ns ticks from 2025.

  Carrying such values as strings in the proto would change the contract, which the issue puts out
  of scope.
- **Changed:** every proto `uint32` lands in ClickHouse as `UInt64`, not `UInt32` (§3.4). The
  ProtobufConverter maps `uint32` to `INT64`. The ClickHouse sink rejects a 64-bit field for a
  32-bit column, and with that check bypassed it wraps silently.

**Q4. Should Postgres keep order-book history?**
- **Decision:** no. Postgres keeps only the latest-state `trading.orderbook`; ClickHouse keeps the
  history.
- **Reason:** nothing reads it, and it would add one full-depth snapshot per command per market.

**Q5. Should the connectors use a dead-letter queue?**
- **Decision:** no. `errors.tolerance=none` everywhere, ClickHouse included (§3.7).
- **Reason:** a record routed to a DLQ is a silent gap, which the core's contract forbids. The only
  producer is the core, with a schema the registry has checked, so a poison record is a bug or a
  skew, and stopping is right.
- **Changed:** the reviewed draft had called a ClickHouse-only DLQ defensible. There is none.

**Q6. Which copy wins on equal `(key, event_ts)`?**
- **Decision:** the guard keeps `<=`. Postgres keeps the first copy, so history is insert-only;
  ClickHouse keeps the last (§3.3).
- **Reason:**
  - For identical copies the choice is invisible.
  - A balance copy can differ within contract item 4. The key's later command then produces its
    own event, with a greater `event_ts`, which supersedes both copies. The latest state converges
    either way.
  - `<=` keeps history insert-only, which is the stronger audit property.
  - A copy that differs because replay changed an outcome (#140 item 2) is an engine bug that no
    tie-break can fix (§9).

**Q7. Does the journal hold every command whose events reached Kafka (assumption A1)?**
- **Decision:** a separate issue, filed with PR 0. It is a pre-production blocker, but not a C1
  dependency: it may land after C1.
- **Reason:** A1 does not always hold.
  - `publish_to_archive` (`networking/src/server/gateway_publications.rs:200-250`) returns once
    `offer` succeeds. Nothing waits for the archive's recorded position (`RecordingPos`).
  - When the core process dies, the outbox's main case, A1 holds: the IPC term buffer and the
    archive survive.
  - A1 fails only when the media driver, the archive or the host dies. The outbox neither causes
    that nor can fix it. The consequence is an engine that disagrees with the read model, not lost
    Kafka events.
- **The fix, in that issue:** journaling stores the `offer` position on the command. The events
  stage waits until the recording's `RecordingPos` counter reaches it before it publishes the
  events or the response. Production sets `aeron.archive.file.sync.level` to 1 or more.

**Q8. What happens to the WebSocket market-data API in sink PRs #1-#3?**
- **Decision:** it retires with the sink. An issue is opened only if a streaming API is wanted.
- **Reason:** it was never merged, and nothing uses it.

**Q9. What units does ClickHouse store?**
- **Decision:** raw tables keep engine integers (`UInt64`). Views scale them, using
  ClickHouse-local `markets` and `assets` reference tables that the same deploy job seeds (§3.3).
- **Reason:** scaling must happen in ClickHouse, because the materialized views sum equity across
  assets of different scales. The sink's `clickhouse_schema.sql` already has a `markets` table,
  which `mv_asset_prices_last_per_min` joins; the design keeps that pattern.
- **Changed:** not a dictionary over Postgres. That would add a cross-database dependency, and its
  credentials, for a handful of static rows.
- **Note:** the gateway reads already-scaled decimals today (`repository.rs:630-633`), while the
  sink stored unscaled integers (vex-kafka-sink `src/clickhouse_client.rs:112-114`). The gateway's
  ClickHouse reads change anyway (G3).

**Q10. Which JDBC sink?**
- **Decision:** Confluent's JDBC sink.
- **Reason:** the Confluent Community License is fine for internal use, and the Protobuf converter
  is under the same license anyway. Debezium's JDBC sink (Apache-2.0) stays the fallback; the
  design does not change.

**R. Does the core keep a start without replay?**
- **Decision:** no. The core always replays the journal at start, and the `--replay` flag is
  removed. A fresh archive has nothing to replay, so its start is fresh. Resetting a ledger is one
  runbook procedure (§6.3).
- **Reason:**
  - A start without replay on an existing archive begins a new recording
    (`networking/src/server/mod.rs:508-540`) and an empty ledger, while Kafka and the read model
    keep the old one.
  - The next replay then restores only the newest recording (#194 `replay_lineage`,
    `networking/src/server/replay.rs:144-149`).
  - That is wrong in every environment, development included, where the acceptance tests run. The
    flag is a setting that correctness depends on, so it goes.
- **Changed:** the reviewed draft let a start without replay begin a new ledger (its §4.3
  item 7), and left whether Production should refuse it to #188 item 2. A Production-only refusal
  was also proposed. The review overruled both: a Production-only refusal leaves the footgun in
  development, and threads the environment into networking.
- #194's journal-tail seeding is unaffected and stays the only seeding mechanism. C1 deletes only
  the `(false, _) => None` arm, the `replay: bool` plumbing, the flag and the RUNBOOK's "Journal
  Replay" section (§4.5).

## 9. Risks

- **A2, #140 item 2 (the highest).**
  - A replayed market BUY can lock a different price than the original run: R1 reads `PriceCache`
    while matching writes it, and #178 declined the fix as a pipeline-topology decision.
  - The engine after replay then disagrees with responses already sent, and with the events Kafka
    already holds. No sink tie-break repairs that.
  - It predates #189, and the outbox does not make it worse. But for any journal that contains
    market orders, it falsifies the issue's acceptance criterion: "latest-state rows match the
    engine across a replay".
  - The acceptance E2E therefore uses limit orders only (§7).
  - The owner decides whether #140 item 2 is a pre-production blocker for the ledger itself. The
    design review's assessment is that it is.
- **Journal durability (Q7).** If the media driver, the archive or the host dies, the journal can
  miss commands whose events and responses went out. It is a pre-production blocker, tracked in its
  own issue.
- **Startup now requires Kafka** when journaling is configured (the W read and the topic check).
  Today the core starts with Kafka down and back-pressures. That is acceptable, since intake would
  answer `Busy` anyway, but the RUNBOOK must say that the core waits at start, logging every 10 s.
- **The Rust registry encoder lexes `trading.proto`; it does not parse it**
  (schema_registry_converter 4.7.0, `proto_resolver.rs`). A comment with the word `message`
  followed by another word would shift the message indexes it computes. `trading.proto` is clean
  today, and the E2E decode through Connect is the guard. Keep it in mind when editing the proto.
- **Watermark timing.** W lags one command through an idle spell, and up to 100 ms otherwise. The
  last W write at shutdown competes for what is left of the grace. All of this only adds
  duplicates. The `shutdown_with_undelivered_events` log line (`events.rs:530`) should now say
  "replay will re-publish them".
- **ClickHouse deduplicates per partition.** `ReplacingMergeTree` collapses rows only within a
  partition. Duplicates share `event_ts`, so they share the partition, as long as the partition key
  derives from `ts` only.
- **Seed drift.** If `markets` and `assets` drift from the core's symbol configuration, the gateway
  mis-scales silently. The generator and its CI run (G1) are the only guard.
- **G1 is the largest PR.** Splitting ClickHouse into G3 is deliberate; do not fold it back.
