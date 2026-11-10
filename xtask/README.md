# Vex-Core `xtask` Automation

This document provides instructions on how to use the `cargo xtask` command-line tool to build, test, and benchmark the `vex-core` project. This tool automates common development tasks, particularly integration tests.

## Prerequisites

Before you begin, ensure you have the following installed: Rust, Docker.

## Overview

The `xtask` crate simplifies complex workflows by providing a set of simple commands. All commands are run via `cargo xtask`.

You can see a list of all available commands by running:
```sh
cargo xtask --help
```

## Available Commands

### Build Docker Images

Builds the necessary Docker images for the `aeron-archive`, `vex-server` and `vex-client` services, which are used in all testing scenarios.

**Usage:**
```sh
docker compose --project-directory ./xtask/tests --file ./xtask/tests/docker-compose.yml up media-driver --build
cargo xtask build-docker
```

### Kafka Topics

Applies `deploy/kafka/topics.toml`, the source of truth for the Kafka topics the core produces and
their Schema Registry subjects, to a cluster: creates the missing topics and sets every topic's
configuration and every subject's compatibility to what the file declares. A deploy job runs it
for every environment (`deploy/kafka/README.md`).

**Usage:**
```sh
cargo xtask kafka apply --brokers <bootstrap servers> --schema-registry <url> [--single-broker]
```

`--single-broker` gives every topic one replica and `min.insync.replicas=1`, for a development or
test cluster of one broker.

### The Test Stack

`xtask/tests/docker-compose.yml` runs Redpanda (with its Schema Registry, and no topic
auto-creation), an Aeron `ArchivingMediaDriver` (`aeron-archive`) and the core (`vex-server`),
which joins the archive's network and IPC namespaces: commands are journaled, and every start
replays the journal, as in production. Before vex-server starts, xtask applies
`deploy/kafka/topics.toml` to Redpanda with `--single-broker`.

### Unified Test Command

All tests are now run through the unified `cargo xtask test` command with subcommands:

```
cargo xtask test
  ├── e2e [OPTIONS]           # Docker-based network E2E tests
  ├── integration [SUITE]     # Integration tests
  ├── outbox                  # Re-publication, startup wait and topic check (Docker)
  ├── benchmark               # Performance benchmarks
  └── unit                    # Run cargo test for xtask
```

#### E2E Tests

Runs end-to-end tests to verify network communication and message integrity between the server and clients. Supports simulating different network conditions.

**Usage:**
```sh
cargo xtask test e2e [OPTIONS]
```

**Options:**
* `--scenario <SCENARIO>`: Sets the network conditions for the test.
  * `basic-connectivity` (Default): No adverse network conditions.
  * `high-latency`: Simulates a high-latency network (100ms RTT).
  * `packet-loss`: Simulates a network with 10% packet loss.
* `--clients <COUNT>`: Specifies the number of clients to run concurrently. Defaults to `1`.
* `--all`: Run all scenarios sequentially.

**Examples:**
```sh
# Run a simple connectivity test with one client
cargo xtask test e2e --scenario basic-connectivity --clients 1

# Run a test with 5 clients under high latency conditions
cargo xtask test e2e --scenario high-latency --clients 5

# Run all scenarios
cargo xtask test e2e --all
```

#### Integration Tests

Runs the integration test suite for order types and trading scenarios.

**Usage:**
```sh
cargo xtask test integration [SUITE] [OPTIONS]
```

**Suites:**
* `all` (Default): Run comprehensive integration test (all order types + edge cases)
* `balance`: Run balance management tests
* `gtc`: Run GTC order tests
* `ioc`: Run IOC order tests
* `fok`: Run FOK order tests
* `cancellation`: Run cancellation tests

**Options:**
* `--verbose` / `-v`: Enable debug logging
* `--fail-fast` / `-f`: Stop on first failure

**Examples:**
```sh
# Run all integration tests
cargo xtask test integration

# Run only GTC tests with verbose output
cargo xtask test integration gtc --verbose

# Run balance tests and stop on first failure
cargo xtask test integration balance --fail-fast
```

#### Outbox Tests

Runs the core's outbox scenarios, in order, on one continuous ledger: a `kill -9` with events
queued behind a paused broker, restarted at once (the start must wait for the media driver to
release the killed core's journal), a graceful shutdown that runs out of its delivery grace with
the broker paused, a start while the broker does not answer, a clean restart, and starts that must
refuse a missing or non-compacted `vex-core.delivered`. Every graceful shutdown keeps the test
gateway connected, as gateways are in production; the clean one must deliver everything and write
the watermark of the last command, so that the restart re-publishes nothing. The oracle is the
test client's own ledger, built from the core's responses: Kafka must hold every event it expects,
by `(topic, key, timestamp)`, and its latest balance per key must equal the ledger's. The orders
are limit orders only, because replay does not regenerate a market buy's events exactly (#140).

**Usage:**
```sh
cargo xtask test outbox
```

#### Benchmarks

Runs performance benchmarks to measure message throughput and latency.

**Usage:**
```sh
cargo xtask test benchmark [OPTIONS]
```

**Options:**
* `--clients <COUNT>`: Specifies the number of clients to run concurrently. Defaults to `1`.

**Example:**
```sh
# Run a benchmark with 5 clients
cargo xtask test benchmark --clients 5
```

#### Unit Tests

Runs the xtask crate unit tests.

**Usage:**
```sh
cargo xtask test unit [OPTIONS]
```

**Options:**
* `--filter <PATTERN>`: Filter tests by pattern

**Example:**
```sh
# Run all xtask unit tests
cargo xtask test unit

# Run only tests matching "parse"
cargo xtask test unit --filter parse
```

## Test Artifacts and Logs

After running tests or benchmarks, the following artifacts are generated:

* **Test Results:** Raw data from tests is stored in `xtask/tests/test-results/`. Each e2e or benchmark client writes the ids it got a response for to `received_ids-<client id>.txt`, which the run reads back to check completeness.
* **Docker Logs:** Logs for each container (`vex-server`, `vex-client`) are saved in `xtask/tests/test-results/logs/`.

The environment is automatically torn down after each run, but the artifacts are preserved for inspection.
