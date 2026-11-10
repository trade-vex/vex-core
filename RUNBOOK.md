# VEX Core Service Runbook

## Table of Contents
1. [Deployment](#deployment)
2. [Configuration](#configuration)
3. [Common Operations](#common-operations)
4. [Troubleshooting](#troubleshooting)
5. [Emergency Procedures](#emergency-procedures)
6. [Architecture Reference](#architecture-reference)

---

## Deployment

### Prerequisites

1. **Rust Toolchain**
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   rustup default stable
   ```

2. **System Dependencies**
   - **Aeron Media Driver** (with Archive support for journaling/replay)
   - **Kafka** broker for event streaming, with a Schema Registry, and the core's topics applied
     from `deploy/kafka/` (see [Kafka Topics](#kafka-topics))
   - Shared memory (`/dev/shm`) for Aeron IPC

3. **Optional: Java** (for standalone Archiving Media Driver)
   - OpenJDK 17+ required if using the Java-based `ArchivingMediaDriver` from the Makefile

### Build

```bash
# Build release binary
cargo build --release

# Binary location: target/release/vex-core

# With balance preload (test/local environments only)
cargo build --release --features balance-preload
```

### Run

```bash
# Start Aeron Media Driver first (required - vex-core connects to it)
make media-driver

# Development mode (uses config.dev.yaml or Development defaults)
VEX_ENV=development RUST_LOG=info cargo run --bin vex-core

# Or using Makefile (starts media driver + vex-core)
VEX_ENV=development make server
```

Every start replays the journal first; see [Restart Service](#restart-service).

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `VEX_ENV` | Environment: exactly `development`, `test` or `production`; any other value is a startup error | Required |
| `VEX__KAFKA_BROKER` | Kafka broker address; overrides the configured `kafka_broker` like any `VEX__*` key | config `kafka_broker` (`localhost:9092` in the dev/test defaults) |
| `VEX__SCHEMA_REGISTRY_URL` | Schema Registry URL for the events' protobuf schemas; overrides the configured `schema_registry_url` like any `VEX__*` key. Must be an `http://` or `https://` URL | config `schema_registry_url` (`http://localhost:8081` in the dev/test defaults) |
| `ENABLE_CORE_PINNING` | Enable CPU core pinning for processor threads: `true` or `false`; any other value is a startup error | `false` (dev/test), `true` (prod) |
| `RUST_LOG` | Log level (trace, debug, info, warn, error) | `info` |
| `VEX__*` | Configuration overrides (e.g., `VEX__CORE_NETWORKING__CONTEXT_DIR`) | - |

### Port Reference

| Environment | Initial Port | Control Port | Gateway Base Port | Aeron Archive |
|-------------|--------------|--------------|-------------------|---------------|
| Development | 40001 | 40002 | 50000 | localhost:8010 |
| Test | 40001 | 40002 | 50000 | localhost:8010 |
| Production | 3521 | 3522 | 50000 | localhost:8010 |

### Kafka Topics

`deploy/kafka/topics.toml` declares every topic the core produces, and the Schema Registry
compatibility of their values; `deploy/kafka/README.md` explains it. Brokers create no topic by
themselves (`auto.create.topics.enable=false`). Apply it before the core's first start, and on
every deploy:

```bash
cargo xtask kafka apply --brokers <bootstrap servers> --schema-registry <url>

# A development broker of one (e.g. for `make server`):
cargo xtask kafka apply --brokers localhost:9092 --schema-registry http://localhost:8081 --single-broker
```

With journaling configured, every start checks the topics before the pipeline starts. It refuses
to start, naming each topic and the setting at fault, if a topic is missing (or the core may not
describe it), if `vex-core.delivered` does not have exactly one partition or is not compacted, if,
in production, an event topic's `min.insync.replicas` is below 2, if Kafka answers for a topic
with an error that waiting cannot clear, such as a denied authorization, or if it denies the core
the read of the watermark (see the ACLs in `deploy/kafka/README.md`). `vex-core.delivered` holds
the delivered watermark, which decides what a start re-publishes (see
[Restart Service](#restart-service)).

---

## Configuration

### Configuration Files

Configuration is loaded from YAML, TOML, or JSON files. Search order (first found wins):

The environment must be selected explicitly with `VEX_ENV`. Development and test selections use the environment defaults in place of a file if none is present; production does not. `VEX__*` overrides and validation apply to those defaults exactly as they do to a file.

1. `./config.<env>.{toml,yaml,yml,json}`
2. `./config/config.<env>.{toml,yaml,yml,json}`
3. `/etc/vex/config.<env>.{toml,yaml,yml,json}`

`<env>` is `dev`, `test` or `prod`; within each directory the extensions are tried in the order shown
(`vex-config/src/environment.rs` `default_config_paths`).

### Example Configuration (config.dev.yaml)

```yaml
environment: Test

kafka_broker: "localhost:9092"
schema_registry_url: "http://localhost:8081"

logging:
  level: "info"
  format: "pretty"
  output: "stdout"

symbols:
  symbols:
    65538:
      market_id: 65538
      base_asset: 2
      quote_asset: 1
      quote_scale_k: 1000000
      maker_fee: 10
      taker_fee: 20
      slippage: 5
      market_type: Spot
      active: true

# Balance preload (TEST/LOCAL ONLY - requires balance-preload feature)
balance_preload:
  enabled: true
  users:
    1:
      - asset_id: 2
        amount: 1000000
      - asset_id: 1
        amount: 50000000
```

Balance preload credits the engine only, at every start, before the replay. It is never journaled
and never published to Kafka, so a read model shows a preloaded balance only after the key's first
journaled command (#198 replaces it with journaled genesis deposits).

### Configuration Precedence

1. Environment-specific config file (based on `VEX_ENV`)
2. Environment variables with `VEX__` prefix (nested keys use `__`, e.g., `VEX__CORE_NETWORKING__INITIAL_PORT`)
3. Hardcoded overrides in `main.rs` (e.g., `context_dir`, `local_address`)

### Note on main.rs Overrides

The `main` function overrides these values at runtime:

- `core_networking.local_address` → `0.0.0.0`
- `core_networking.context_dir` → `/dev/shm/aeron`

Ensure the **Aeron Media Driver uses the same `context_dir`** as vex-core (or the directory vex-core is configured to use).

---

## Common Operations

### Start Service

```bash
# 1. Start Aeron Media Driver (if not already running)
make media-driver

# 2. Start vex-core
VEX_ENV=development RUST_LOG=info ./target/release/vex-core

# Or use Makefile (handles both)
VEX_ENV=development make server
```

### Stop Service

```bash
# Graceful shutdown (SIGTERM or Ctrl+C)
kill -TERM <pid>

# vex-core handles Ctrl+C and performs graceful shutdown, with gateways connected or not:
# it closes every gateway session, drains the pipeline, waits for the archive to record every
# accepted command, then delivers the events still queued and writes the delivered watermark.
# It logs `flush_complete` once the brokers have acknowledged every event, or
# `shutdown_with_undelivered_events` with the count of those the next start re-publishes.
# A shutdown that delivers everything leaves the next start nothing to re-publish.

# A graceful shutdown can take about 45 s when Kafka is slow or down: pipeline drain
# (fails once 10 s pass without progress), archive catch-up (up to 10 s), Kafka delivery
# (20 s, plus up to 5 s). Give the orchestrator at least that before it kills the process
# (Docker: stop_grace_period, set in docker/docker-compose.yml; Kubernetes:
# terminationGracePeriodSeconds), or the undelivered-event count is never logged. Nothing
# undelivered is lost either way: the next start re-publishes it (see "Kafka is Down").

# Force stop (use only if graceful shutdown fails)
kill -9 <pid>
# The next start waits until the media driver releases the killed process's journal, about the
# driver's client liveness timeout (10 s by default) after the kill, logging
# `waiting_for_journal_release`.
```

### Stop Media Driver

```bash
make stop-media-driver
```

### Restart Service

```bash
# Graceful restart
kill -TERM <pid>
# Wait for shutdown, then:
make media-driver  # If needed
VEX_ENV=development ./target/release/vex-core
```

Every start, with journaling configured:

1. **Checks the Kafka topics and reads the delivered watermark**, before the pipeline starts (see
   [Kafka Topics](#kafka-topics)). While Kafka does not answer, the core waits, logging
   `waiting_for_kafka` every 10 s; it logs `watermark_read` once it has read the watermark.
2. **Waits until no killed process holds the journal.** A process that died while journaling (a
   crash, `kill -9`, an orchestrator's kill) holds it until the media driver's client liveness
   timeout passes (`aeron.client.liveness.timeout`, 10 s by default): the driver keeps its
   publication, and the archive keeps recording it. The start waits for the driver to release it
   and for the archive to stop that recording at its final position, so that the replay holds
   every command the dead process journaled. It logs `waiting_for_journal_release` every 5 s, and
   `journal_released` once done. A start right after a crash therefore takes about that timeout
   longer, instead of failing. After a graceful stop, the driver frees the closed publication
   within two of its timer checks (draining, then lingering; `aeron.timer.interval`, 1 s by
   default), so a start right after it waits up to about 2 s.
3. **Replays the journal**: the newest recording that holds commands, which the start then
   extends. An empty archive holds nothing to replay, so the start is fresh. The replay
   re-publishes the events of every command above the watermark, the events a previous run may
   not have delivered, and answers no gateway. It takes as long as Kafka needs to accept them,
   and fails only once 10 s pass without progress (`DrainError`).
4. **Stamps every live command above the last journaled command** (`journal_tail_read`), so
   order ids and timestamps never repeat any the archive holds, even if this host's clock is
   behind. A start fails if it cannot read it.

There is no start without replay: a new ledger is a [reset](#resetting-the-ledger). With
journaling disabled there is no journal, nothing is replayed or re-published, and ids and
timestamps follow the wall clock alone.

### View Logs

```bash
# Logs go to stdout by default (development)
# Redirect if needed:
VEX_ENV=development ./target/release/vex-core 2>&1 | tee logs/vex-core.log

# Production config may write to /var/log/vex/vex-core.log
tail -f /var/log/vex/vex-core.log
```

---

## Troubleshooting

### Service Won't Start

**Symptom**: Process exits immediately or fails during initialization

**Diagnosis**:
```bash
# Check configuration loading
VEX_ENV=development RUST_LOG=debug ./target/release/vex-core 2>&1 | head -50

# Verify config file exists for your environment
ls -la config.dev.yaml config.test.yaml config.prod.yaml 2>/dev/null

# Check for config validation errors in output
```

**Solutions**:
- Set `VEX_ENV` to the intended environment (`development`, `test` or `production`)
- Ensure a config file exists for the selected environment, or that development/test defaults are acceptable
- `the Kafka topics are not as the core requires`: a topic is missing or misconfigured; the error names each one and why. Apply `deploy/kafka/topics.toml` (see [Kafka Topics](#kafka-topics)). A topic that exists but is reported missing is one the core may not describe: grant the ACLs in `deploy/kafka/README.md`
- `Kafka answered for topic <topic> with an error that waiting cannot clear` or `Kafka described no setting of topic <topic>`: usually a denied ACL (`Topic authorization failed`, or no DescribeConfigs); grant the ACLs in `deploy/kafka/README.md`
- `the core may not read the watermark from topic vex-core.delivered`: grant the core Read on `vex-core.delivered` and Describe on the consumer group `vex-core.watermark-reader` (`deploy/kafka/README.md`, "ACLs")
- The process runs but never accepts gateways, logging `waiting_for_kafka`: the configured `kafka_broker` (or `VEX__KAFKA_BROKER`) does not answer. The core waits for it before it starts
- `the journal (aeron:ipc stream 2001) is still held after ...`: a live recording of the journal, or a publication of it, outlasted the media driver's client liveness timeout and 5 s more. A killed core holds them only until that timeout passes and the archive has recorded the rest of its publication. So either another process is journaling to this media driver and archive (run one core per archive), or the archive is lagging: it is still recording a large backlog that the killed core's publication held unrecorded (its recording position still advances; start again once the recording has stopped). The error lists the recordings and the publications' session ids (`make aeron-stat` shows the publications)
- A config file must set `schema_registry_url`, or `VEX__SCHEMA_REGISTRY_URL` must; only the built-in dev/test defaults, used when no file exists, supply one. A value that is not an `http://` or `https://` URL is rejected
- Check that `/dev/shm` has sufficient space for Aeron
- Verify symbol configuration is valid (market_id, assets, fees)

### Aeron / Media Driver Connection Failures

**Symptom**: "Aeron connection failed", "Failed to create VexCoreServer", or similar

**Diagnosis**:
```bash
# Check Aeron context directory exists and is writable
ls -la /dev/shm/aeron
ls -la /dev/shm/aeron-test-server  # If using Test defaults before main override

# Verify media driver is running
pgrep -f "ArchivingMediaDriver|aeron"
make aeron-stat  # Show Aeron statistics

# Check for port conflicts
netstat -tuln | grep -E "40001|40002|3521|3522|8010"
```

**Solutions**:
- Start the media driver: `make media-driver`
- Ensure vex-core and media driver use the **same** Aeron context directory
- **Note**: `main.rs` hardcodes `context_dir` to `/dev/shm/aeron`; the Makefile media driver uses `/dev/shm/aeron-test-server` by default. Align these (e.g., set `AERON_DIR=/dev/shm/aeron` when starting the media driver, or update the Makefile `AERON_DIR`) so both use the same path
- Check that `request_control_channel` (default `localhost:8010`) matches the Archiving Media Driver
- Verify no firewall blocking UDP ports 40001, 40002, 8010 (or production ports)

### Kafka Connection Failures

**Symptom**: Events handler or Kafka producer errors in logs

**Diagnosis**:
```bash
# Test Kafka connectivity
kafka-topics.sh --bootstrap-server $VEX__KAFKA_BROKER --list

# Check VEX__KAFKA_BROKER is set (otherwise the configured kafka_broker is used)
echo $VEX__KAFKA_BROKER

# Test Schema Registry connectivity, using the URL vex-core logs at startup (schema_registry=...)
curl -s "<schema_registry_url>/subjects"
```

**Solutions**:
- Verify Kafka is running and reachable
- Set `kafka_broker` in the config, or `VEX__KAFKA_BROKER`, correctly (e.g., `localhost:9092`)
- Set `schema_registry_url` in the config, or `VEX__SCHEMA_REGISTRY_URL`, correctly (e.g., `http://localhost:8081`). A wrong URL does not fail startup: the first event stalls with `protobuf_encoding_failed` and intake halts (see "Kafka is Down"). Fix it and restart
- Check network connectivity to Kafka

### Gateway Connection Failures

**Symptom**: Gateways cannot connect to vex-core; "Connection rejected" or timeouts

**Diagnosis**:
```bash
# Check vex-core is listening
netstat -tuln | grep -E "40001|3521"

# Verify the gateway's [core] section matches core
# Gateway: core.address, core.port, core.control_port (override: APP__CORE__<KEY>)
# Core: initial_port, initial_control_port (from config)

# Check session ID limits
# reserved_session_id_low/high in config
```

**Solutions**:
- Ensure vex-core is running and media driver is up
- Verify the gateway's `[core]` `address`, `port` and `control_port` (`APP__CORE__ADDRESS`, `APP__CORE__PORT`, `APP__CORE__CONTROL_PORT`) match vex-core's address, `initial_port` and `initial_control_port`
- Check `max_gateways` is not exceeded
- Gateway authentication (required in production, which refuses to start without both):
  `VEX__CORE_NETWORKING__ENABLE_AUTHENTICATION=true` and
  `VEX__GATEWAY_NETWORKING__GATEWAY_AUTHENTICATION_KEY=<signed 32-bit integer>` on vex-core; every
  gateway must be given the same value (vex-gateway `[core]` `auth_key`, or `APP__CORE__AUTH_KEY`). Without a key,
  dev/test run the handshake unauthenticated. `Connection rejected: Handshake rejected` on the
  gateway together with `action="handshake_rejected"` and
  `reason=Authentication failed: Invalid credentials` in the vex-core log means
  the keys differ. The key travels in cleartext in the HELLO message, so keep gateway ↔ core
  traffic on a trusted network segment

### Order Processing Delays or Backpressure

**Symptom**: Orders slow to process, high latency

**Diagnosis**:
```bash
# Check CPU usage
top -p $(pgrep vex-core)

# Review disruptor/processor logs
VEX_ENV=development RUST_LOG=debug ./target/release/vex-core 2>&1 | grep -E "disruptor|processor|backpressure"
```

**Solutions**:
- Enable CPU core pinning in production: `ENABLE_CORE_PINNING=true`
- Increase `buffer_size` in networking config if needed
- Ensure adequate CPU cores (journaling, 4 risk engines, 4 matching engines, events handler)
- Profile with `perf` or `flamegraph` if persistent

### Replay Failures

**Symptom**: Startup fails while replaying the journal (`ReplayError`, or a `DrainError` while the replay re-publishes to a stalled Kafka)

**Diagnosis**:
```bash
# Check Aeron Archive is running (ArchivingMediaDriver)
# Replay reads from the archive

# Verify recording exists
# Check Aeron archive/recording directories
```

**Solutions**:
- Ensure Archiving Media Driver (not plain MediaDriver) is used
- Verify previous runs recorded to the archive successfully
- Check `request_control_channel` and archive configuration
- A `DrainError` means the pipeline stopped advancing for 10 s, usually because Kafka stopped accepting the re-published events: restore Kafka and start again. Each start re-publishes from the last watermark written, so restarts converge

### High Memory Usage

**Symptom**: vex-core consumes excessive memory

**Diagnosis**:
```bash
ps aux | grep vex-core
top -p $(pgrep vex-core)
```

**Solutions**:
- Check disruptor `BUFFER_SIZE` (default 1024 in release)
- Review Kafka consumer buffers
- Use `tikv-jemallocator` (already default) for allocator
- Restart periodically if memory grows over time

---

## Emergency Procedures

### Service is Down

1. **Check Process**
   ```bash
   ps aux | grep vex-core
   pgrep -f vex-core
   ```

2. **Check Media Driver**
   ```bash
   pgrep -f "ArchivingMediaDriver|aeron"
   make aeron-stat
   ```

3. **Check Logs**
   ```bash
   tail -100 /var/log/vex/vex-core.log
   # Or wherever logs are configured
   ```

4. **Restart**
   ```bash
   make media-driver    # If needed
   VEX_ENV=development ./target/release/vex-core
   ```

### Media Driver is Down

1. **Impact**
   - vex-core cannot start or will lose connection
   - All gateway order flow stops

2. **Restart Media Driver**
   ```bash
   make stop-media-driver
   make media-driver
   ```

3. **Restart vex-core**
   ```bash
   VEX_ENV=development ./target/release/vex-core
   ```

### Kafka is Down

1. **Impact**
   - **Order intake halts; no event is dropped.** While Kafka, or the Schema Registry, is unavailable, the core holds every event in engine order. librdkafka retries undelivered events with no time limit (`message.timeout.ms=0`). A topic's first event after a start needs the registry, and the sender retries it with a backoff of up to 1 s. Once librdkafka's queue (100,000 events) and the sender's channel (8,192 events) are full, the events stage blocks and ingress answers `Busy`. The gateway returns that as 503 with `Retry-After`. Responses to commands that were already accepted are delayed too
   - Logs: `producer_send_waiting` (a full queue, or a topic the broker reports missing) or `protobuf_encoding_failed` (registry). Each repeats at most every 10 s while the stall lasts
   - **Recovery is automatic**: when Kafka and the registry answer again, the held events are delivered in order and intake resumes
   - A failure that no retry can cure aborts the process loudly instead of dropping an event. It is logged as one of:
     - `delivery_failed`: a denied ACL, an event already queued for a topic that a broker then reports missing, or a fatal producer error (librdkafka logs `FATAL` first)
     - `send_failed`
     - `protobuf_encoding_impossible`: an event type missing from the compiled `trading.proto`
     - `sender_panicked`
   - **Events still queued when the process dies are re-published by the next start.** The core writes the delivered watermark, the last command whose events the brokers have all acknowledged, to `vex-core.delivered`: at most every 100 ms while delivery advances, and once more at the end of a graceful shutdown. Every start replays the journal and re-publishes the events of every command above it. This covers a crash, a kill, an abort above, a graceful shutdown that could not deliver within 20 s (it logs `shutdown_with_undelivered_events` with the count, "replay will re-publish them"), one whose pipeline drain failed first, and one that the orchestrator killed. The sinks receive duplicates of the events after the last watermark written, which they absorb: a reader takes a key's latest state from the greatest timestamp, not from the last event in the partition
   - **A start waits for Kafka.** With journaling configured, the core checks its topics and reads the watermark before it starts: while Kafka does not answer, it logs `waiting_for_kafka` every 10 s and accepts no gateway. It starts by itself once Kafka answers
   - `watermark_write_skipped` or `watermark_not_delivered` (each at most every 10 s): a watermark record could not be enqueued or delivered. Nothing is lost: the next write carries the then-current watermark, and a start from an older one only re-publishes more

2. **Mitigation**
   - Restore Kafka or the Schema Registry; intake resumes by itself
   - A registry that rejects the schema (for example as incompatible with the subject's earlier versions) also shows up as a `protobuf_encoding_failed` stall. The client library reports it exactly like an outage. Fix the subject's compatibility setting or the deployed schema
   - A topic deleted while the core runs shows up as a `producer_send_waiting` stall. Re-create it from `deploy/kafka/topics.toml` (see [Kafka Topics](#kafka-topics)). If it was `vex-core.delivered`, the next start re-publishes the whole journal
   - After an abort, or a shutdown that reported undelivered events, start the core again: it re-publishes them

### Gateway Cannot Connect

1. **Verify Core**
   ```bash
   ps aux | grep vex-core
   netstat -tuln | grep -E "40001|3521"
   ```

2. **Verify Network**
   ```bash
   ping <core_host>
   # From gateway host
   ```

3. **Check Config**
   - The gateway's `[core]` `address`, `port` and `control_port` must match vex-core
   - Ensure `max_gateways` and session ID limits are not exceeded

### Need to Recover from Journal

Start the core: every start replays the journal, and re-publishes what Kafka may lack (see
[Restart Service](#restart-service)).

### Resetting the ledger

The core always replays its journal, so a new ledger means removing the old one everywhere at
once: the engine, Kafka and the read model. A partial reset leaves them on different ledgers. A
new partition count for an event topic is a reset too (`deploy/kafka/README.md`).

1. Stop the gateway, the core and the Kafka Connect connectors.
2. Delete the archive's recordings (the journal), with the media driver stopped: the archive
   directory (`aeron.archive.dir`).
3. Delete the seven event topics and `vex-core.delivered`, then re-create them with
   `cargo xtask kafka apply` (see [Kafka Topics](#kafka-topics)).
4. Delete the connectors' consumer groups (`connect-<connector>`), so that they read the new
   topics from the start.
5. Drop the read model, and re-apply its migrations and seeds (vex-gateway `readmodel/`).
6. Bring the environment up: topics, migrations and seeds, Connect, the core, the gateway.

### Rebuilding the read model

To rebuild the read model while keeping the ledger: drop it and re-apply its migrations and seeds,
then either reset the connectors' offsets to the earliest, if the topics' retention still covers
the whole history, or make the core re-publish the whole journal. For the latter, stop the core,
write a tombstone (a null value) for the key `w` on `vex-core.delivered`, and start it: a
tombstone reads as watermark 0. The replay then re-publishes every journaled command, taking as
long as Kafka needs.

```bash
# With the core stopped. kcat: -Z sends the empty value as null (a tombstone).
echo "w:" | kcat -b <brokers> -t vex-core.delivered -P -K: -Z
```

---

## Architecture Reference

### Component Overview

VEX Core is a **low-latency matching engine** with no HTTP API. It communicates with gateways via **Aeron UDP** and publishes events to **Kafka**.

```
┌─────────────────┐    Aeron UDP        ┌─────────────────┐
│   VEX Gateway   │ ◄─────────────────► │   VEX Core      │
│   (Client API)  │                     │   (Matching     │
│                 │                     │    Engine)      │
└─────────────────┘                     └─────────────────┘
        │                                        │
        │                                        │
        ▼                                        ▼
┌─────────────────┐                     ┌─────────────────┐
│   Market Data   │                     │   Kafka         │
│   Consumers     │                     │   (Events)      │
└─────────────────┘                     └─────────────────┘
```

### Processing Pipeline

```
[Gateways/Aeron] → [Disruptor Ring Buffer] → [Journaling] → [Risk R1] → [Matching] → [Risk R2] → [Events/Kafka]
```

- **Journaling**: Persists orders to Aeron Archive for replay
- **Risk R1**: Pre-trade risk checks
- **Matching Engine**: Order book matching
- **Risk R2**: Post-trade settlement
- **Events**: Publishes to Kafka (fills, cancellations, etc.)

### Key Files

| Path | Purpose |
|------|---------|
| `src/main.rs` | Entry point, config loading, shutdown handling |
| `server/src/lib.rs` | Engine startup, balance preload |
| `server/src/engine.rs` | Core engine, disruptor setup, processor pipeline |
| `networking/src/server/` | Aeron server, gateway handshake, publications |
| `vex-config/` | Configuration loading and validation |
| `config.dev.yaml` | Example development config |
