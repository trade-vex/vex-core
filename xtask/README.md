# Vex-Core `xtask` Automation

This document provides instructions on how to use the `cargo xtask` command-line tool to build, test, and benchmark the `vex-core` project. This tool automates common development tasks, particularly integration tests.

## Prerequisites

Before you begin, ensure you have the following installed Rust, Docker.

## Overview

The `xtask` crate simplifies complex workflows by providing a set of simple commands. All commands are run via `cargo xtask`.

You can see a list of all available commands by running:
```sh
cargo xtask --help
```

## Available Commands

### 1. Build Docker Images

This command builds the necessary Docker images for the `vex-server` and `vex-client` services, which are used in all testing scenarios.

**Usage:**
```sh
docker compose --project-directory ./xtask/tests --file ./xtask/tests/docker-compose.yml up media-driver --build
cargo xtask build-docker
```

### 2. End-to-End Correctness Testing

This command runs end-to-end tests to verify the network communication and message integrity between the server and one or more clients. It supports simulating different network conditions.

**Usage:**
```sh
cargo xtask test-e2e [OPTIONS]
```

**Options:**

*   `--scenario <SCENARIO>`: Sets the network conditions for the test.
    *   `basic-connectivity` (Default): No adverse network conditions are applied.
    *   `high-latency`: Simulates a high-latency network (100ms RTT).
    *   `packet-loss`: Simulates a network with 10% packet loss.
*   `--clients <COUNT>`: Specifies the number of clients to run concurrently. Defaults to `1`.

**Examples:**

*   Run a simple connectivity test with one client:
    ```sh
    cargo xtask test-e2e --scenario basic-connectivity --clients 1
    ```
*   Run a test with 5 clients under high latency conditions:
    ```sh
    cargo xtask test-e2e --scenario high-latency --clients 5
    ```
*   Run a test with 5 clients under packet loss conditions:
    cargo xtask test-e2e --scenario packet-loss --clients 5

### 3. Performance Benchmarking

This command runs a benchmark to measure the message throughput and latency between the server and clients.

**Usage:**
```sh
cargo xtask benchmark [OPTIONS]
```

**Options:**

*   `--clients <COUNT>`: Specifies the number of clients to run concurrently. Defaults to `1`.

**Example:**

*   Run a benchmark with 5 clients:
    ```sh
    cargo xtask benchmark --clients 5
    ```

## Test Artifacts and Logs

After running tests or benchmarks, the following artifacts are generated:

*   **Test Results:** Raw data from tests is stored in `xtask/tests/test-results/`.
*   **Docker Logs:** Logs for each container (`vex-server`, `vex-client`) are saved in `xtask/tests/test-results/logs/`.

The environment is automatically torn down after each run, but the artifacts are preserved for inspection.
