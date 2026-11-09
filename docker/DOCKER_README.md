# Docker Setup Guide

This guide explains how to use the Docker setup for the VEX Core project, including the media driver and SBE code generation.

## Overview

The project includes several Docker configurations:

1. **Media Driver** - Aeron IPC/UDP media driver for high-performance messaging
2. **SBE Code Generator** - Generates Rust code from SBE schemas
3. **VEX Core** - Main trading engine application

## Quick Start

### 1. Test Media Driver Independently

To test if the media driver is working properly:

```bash
./docker-scripts.sh test-media-driver
```

This will:
- Build and start the media driver
- Run a test client to verify connectivity
- Show logs and status

### 2. Generate SBE Code

To generate Rust code from the `resources/ordercmd.xml` SBE schema:

```bash
./docker-scripts.sh generate-sbe
```

This will:
- Build the SBE tool
- Generate Rust code from `resources/ordercmd.xml`
- Output generated files to `./target/generated/`

### 3. Start Full Application

To run the complete application (media driver + vex-core):

```bash
VEX_ENV=development ./docker-scripts.sh start-full
```

## Docker Files

### 1. `media-driver/Dockerfile`
- Builds Aeron media driver from source
- Includes health checks
- Exposes port 8080 for monitoring

### 2. `Dockerfile.sbe`
- Generates Rust code from SBE schemas
- Uses Simple Binary Encoding tool
- Outputs generated code to a volume

### 3. `Dockerfile` (main)
- Builds the VEX Core trading engine
- Multi-stage build for optimization

## Docker Compose Files

### 1. `docker-compose.yml` (Main)
- Includes media driver, SBE generator, and vex-core
- Proper service dependencies and health checks
- **Development only, with no durability.** It runs a media driver but no Aeron Archive, so it
  starts vex-core with an empty archive channel (`VEX__CORE_NETWORKING__REQUEST_CONTROL_CHANNEL=`).
  Commands are not journaled, nothing survives a restart, and `--replay` is unavailable. Production
  needs an Aeron Archive; vex-core refuses an empty archive channel with `VEX_ENV=production`
- `VEX_ENV` must be set to `development` or `test` in the shell that runs compose. Compose passes it
  through; without it vex-core exits at startup (`No environment selected`)

### 2. `docker-compose.media-driver.yml`
- Standalone media driver for testing
- Includes test client for verification

## Available Commands

Use the helper script `./docker-scripts.sh` with these commands:

| Command | Description |
|---------|-------------|
| `test-media-driver` | Test media driver independently |
| `generate-sbe` | Generate SBE Rust code |
| `start-full` | Start complete application |
| `stop-all` | Stop all containers |
| `clean-all` | Clean up containers and volumes |
| `logs-media-driver` | Show media driver logs |
| `logs-sbe` | Show SBE generator logs |
| `help` | Show help message |

## Manual Docker Commands

If you prefer to use Docker directly:

### Test Media Driver Only
```bash
docker-compose -f docker-compose.media-driver.yml up --build
```

### Generate SBE Code Only
```bash
docker-compose up --build sbe-generator
```

### Start Full Application
```bash
VEX_ENV=development docker-compose up --build
```

## SBE Code Generation

The SBE generator processes `resources/ordercmd.xml` and generates Rust code with:

- **Input**: `resources/ordercmd.xml` (SBE schema)
- **Output**: `./target/generated/` (Generated Rust code)
- **Namespace**: `sbe`

The generated code includes:
- Order command enums and structs
- Message serialization/deserialization
- Type-safe message handling

## Troubleshooting

### Media Driver Issues

1. **Check if media driver is running**:
   ```bash
   docker-compose -f docker-compose.media-driver.yml logs media-driver
   ```

2. **Verify health check**:
   ```bash
   docker-compose -f docker-compose.media-driver.yml ps
   ```

3. **Check Aeron directory**:
   ```bash
   docker exec -it vex-media-driver-test ls -la /tmp/aeron
   ```

### SBE Generation Issues

1. **Check SBE generator logs**:
   ```bash
   docker-compose logs sbe-generator
   ```

2. **Verify generated files**:
   ```bash
   ls -la ./target/generated/
   ```

3. **Clean and rebuild**:
   ```bash
   ./docker-scripts.sh clean-all
   ./docker-scripts.sh generate-sbe
   ```

### Common Issues

1. **Port conflicts**: Make sure port 8080 is available
2. **Permission issues**: Ensure Docker has proper permissions
3. **Volume issues**: Clean volumes with `./docker-scripts.sh clean-all`

## Development Workflow

1. **Test media driver first**:
   ```bash
   ./docker-scripts.sh test-media-driver
   ```

2. **Generate SBE code**:
   ```bash
   ./docker-scripts.sh generate-sbe
   ```

3. **Run full application**:
   ```bash
   VEX_ENV=development ./docker-scripts.sh start-full
   ```

## Environment Variables

### Media Driver
- `AERON_DIR=/dev/shm/aeron` - Aeron IPC directory (must be the directory vex-core uses, below)
- `AERON_EVENT_LOG=all` - Enable all event logging

### VEX Core
- `VEX_ENV=development|test|production` - Required explicit environment selection
- `RUST_LOG=info` - Rust logging level
- `VEX__KAFKA_BROKER`, `VEX__SCHEMA_REGISTRY_URL` - Kafka broker and Schema Registry URL (config `kafka_broker`, `schema_registry_url`), passed through from the calling shell. The compose stack runs neither, and the development defaults (`localhost:9092`, `http://localhost:8081`) point at the container itself, so set both. Without a reachable Kafka and registry, events back up and order intake halts (RUNBOOK "Kafka is Down")
- Aeron IPC directory: always `/dev/shm/aeron` (set in `src/main.rs`). vex-core does not read `AERON_DIR`; point the media driver at this directory instead.

## Monitoring

The media driver exposes port 8080 for monitoring. You can access monitoring endpoints at:
- `http://localhost:8080` (when running)

## Clean Up

To clean up everything:
```bash
./docker-scripts.sh clean-all
```

This will:
- Stop all containers
- Remove all volumes
- Clean up Docker system 
