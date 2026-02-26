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
   - **Kafka** broker for event streaming
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

# Development mode (uses config.dev.yaml or Test defaults)
RUST_LOG=info cargo run --bin vex-core

# Or using Makefile (starts media driver + vex-core)
make server

# With journal replay (recover from recorded state)
./target/release/vex-core --replay
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `VEX_ENV` / `ENVIRONMENT` / `ENV` | Environment: `development`, `test`, `production` | `development` |
| `KAFKA_BROKER` | Kafka broker address | `localhost:9092` |
| `ENABLE_CORE_PINNING` | Enable CPU core pinning for processor threads | `false` (dev/test), `true` (prod) |
| `RUST_LOG` | Log level (trace, debug, info, warn, error) | `info` |
| `VEX__*` | Configuration overrides (e.g., `VEX__CORE_NETWORKING__CONTEXT_DIR`) | - |

### Port Reference

| Environment | Initial Port | Control Port | Gateway Base Port | Aeron Archive |
|-------------|--------------|--------------|-------------------|---------------|
| Development | 40001 | 40002 | 50000 | localhost:8010 |
| Test | 40001 | 40002 | 50000 | localhost:8010 |
| Production | 3521 | 3522 | 50000 | localhost:8010 |

---

## Configuration

### Configuration Files

Configuration is loaded from YAML, TOML, or JSON files. Search order (first found wins):

- `./config.dev.yaml`, `./config.test.yaml`, `./config.prod.yaml`
- `./config/config.dev.yaml`, etc.
- `/etc/vex/config.dev.toml`, etc.

Environment-specific file names: `config.dev`, `config.test`, `config.prod` (with `.yaml`, `.yml`, `.toml`, or `.json` extension).

### Example Configuration (config.dev.yaml)

```yaml
environment: Test

kafka_broker: "localhost:9092"

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
      base_scale_k: 100000000
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

### Configuration Precedence

1. Environment-specific config file (based on `VEX_ENV` / `ENVIRONMENT`)
2. Environment variables with `VEX__` prefix (nested keys use `__`, e.g., `VEX__CORE_NETWORKING__INITIAL_PORT`)
3. Optional runtime environment overrides in `main.rs` (e.g., `AERON_DIR`, `CORE_LOCAL_ADDRESS`, `KAFKA_BROKER`, `ENABLE_CORE_PINNING`)

### Note on main.rs Runtime Overrides

The `main` function applies these overrides only when env vars are set:

- `core_networking.local_address` → `$CORE_LOCAL_ADDRESS`
- `core_networking.context_dir` → `$AERON_DIR`
- `kafka_broker` → `$KAFKA_BROKER`
- `core_networking.enable_core_pinning` → `$ENABLE_CORE_PINNING`

Ensure the **Aeron Media Driver uses the same `context_dir`** as vex-core (or the directory vex-core is configured to use).

---

## Common Operations

### Start Service

```bash
# 1. Start Aeron Media Driver (if not already running)
make media-driver

# 2. Start vex-core
RUST_LOG=info ./target/release/vex-core

# Or use Makefile (handles both)
make server
```

### Stop Service

```bash
# Graceful shutdown (SIGTERM or Ctrl+C)
kill -TERM <pid>

# vex-core handles Ctrl+C and performs graceful shutdown

# Force stop (use only if graceful shutdown fails)
kill -9 <pid>
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
./target/release/vex-core
```

### Journal Replay

```bash
# Start with replay from recorded journal
./target/release/vex-core --replay
```

### View Logs

```bash
# Logs go to stdout by default (development)
# Redirect if needed:
./target/release/vex-core 2>&1 | tee logs/vex-core.log

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
RUST_LOG=debug ./target/release/vex-core 2>&1 | head -50

# Verify config file exists for your environment
ls -la config.dev.yaml config.test.yaml config.prod.yaml 2>/dev/null

# Check for config validation errors in output
```

**Solutions**:
- Ensure a config file exists for the detected environment, or that defaults are acceptable
- Verify `KAFKA_BROKER` is reachable if Kafka is required
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
kafka-topics.sh --bootstrap-server $KAFKA_BROKER --list

# Check KAFKA_BROKER is set
echo $KAFKA_BROKER
```

**Solutions**:
- Verify Kafka is running and reachable
- Set `KAFKA_BROKER` correctly (e.g., `localhost:9092`)
- Check network connectivity to Kafka

### Gateway Connection Failures

**Symptom**: Gateways cannot connect to vex-core; "Connection rejected" or timeouts

**Diagnosis**:
```bash
# Check vex-core is listening
netstat -tuln | grep -E "40001|3521"

# Verify gateway config matches core
# Gateway: core_address, core_port, core_control_port
# Core: initial_port, initial_control_port (from config)

# Check session ID limits
# reserved_session_id_low/high in config
```

**Solutions**:
- Ensure vex-core is running and media driver is up
- Verify gateway `core_address`, `core_port`, `core_control_port` match vex-core `initial_port` and `initial_control_port`
- Check `max_gateways` is not exceeded
- If `enable_authentication` is true, ensure gateway credentials are valid

### Order Processing Delays or Backpressure

**Symptom**: Orders slow to process, high latency

**Diagnosis**:
```bash
# Check CPU usage
top -p $(pgrep vex-core)

# Review disruptor/processor logs
RUST_LOG=debug ./target/release/vex-core 2>&1 | grep -E "disruptor|processor|backpressure"
```

**Solutions**:
- Enable CPU core pinning in production: `ENABLE_CORE_PINNING=true`
- Increase `buffer_size` in networking config if needed
- Ensure adequate CPU cores (journaling, 4 risk engines, 4 matching engines, events handler)
- Profile with `perf` or `flamegraph` if persistent

### Replay Failures

**Symptom**: `--replay` fails or crashes

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
   ./target/release/vex-core
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
   ./target/release/vex-core
   ```

### Kafka is Down

1. **Impact**
   - Event publishing (e.g., fills, order updates) may fail
   - Downstream consumers (market data, analytics) will not receive events
   - **Order matching may still operate** (in-memory), but events are not emitted

2. **Mitigation**
   - Restore Kafka
   - Consider replay or backfill if events were lost (depends on design)

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
   - Gateway `core_address`, `core_port`, `core_control_port` must match vex-core
   - Ensure `max_gateways` and session ID limits are not exceeded

### Need to Recover from Journal

```bash
# Start with replay to recover state from last recording
./target/release/vex-core --replay
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
