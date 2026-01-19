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

Builds the necessary Docker images for the `vex-server` and `vex-client` services, which are used in all testing scenarios.

**Usage:**
```sh
docker compose --project-directory ./xtask/tests --file ./xtask/tests/docker-compose.yml up media-driver --build
cargo xtask build-docker
```

### Unified Test Command

All tests are now run through the unified `cargo xtask test` command with subcommands:

```
cargo xtask test
  ├── e2e [OPTIONS]           # Docker-based network E2E tests
  ├── integration [SUITE]     # Integration tests
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

## Legacy Commands (Deprecated)

The following commands are deprecated but still functional for backwards compatibility. They will print a deprecation warning and suggest the new command:

| Old Command | New Command |
|------------|-------------|
| `cargo xtask test-e2e --scenario X` | `cargo xtask test e2e --scenario X` |
| `cargo xtask benchmark` | `cargo xtask test benchmark` |
| `cargo xtask test-suite` | `cargo xtask test integration` |

## Test Artifacts and Logs

After running tests or benchmarks, the following artifacts are generated:

* **Test Results:** Raw data from tests is stored in `xtask/tests/test-results/`.
* **Docker Logs:** Logs for each container (`vex-server`, `vex-client`) are saved in `xtask/tests/test-results/logs/`.

The environment is automatically torn down after each run, but the artifacts are preserved for inspection.
