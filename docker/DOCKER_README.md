# Docker Setup Guide

This guide explains how to use the Docker setup for the VEX Core project, including the media driver and SBE code generation.

## Overview

The project includes several Docker configurations:

1. **Media Driver** - Aeron's Java `ArchivingMediaDriver` (aeron-all 1.49.0): the media driver and the Aeron Archive that journals vex-core's commands
2. **SBE Code Generator** - Generates Rust code from SBE schemas
3. **VEX Core** - Main trading engine application

## Quick Start

### 1. Generate SBE Code

To generate Rust code from the `resources/ordercmd.xml` SBE schema:

```bash
./docker-scripts.sh generate-sbe
```

This will:
- Build the SBE tool
- Generate Rust code from `resources/ordercmd.xml`
- Output generated files to `./target/generated/`

### 2. Start Full Application

To run the complete application (media driver + vex-core):

```bash
VEX_ENV=development ./docker-scripts.sh start-full
```

## Docker Files

### 1. `../xtask/tests/archive.Dockerfile`
- The media driver image: the `aeron-all` jar, pinned by version and checksum, run as `ArchivingMediaDriver`
- The same image the end-to-end tests and CI's recovery tests run

### 2. `Dockerfile.sbe`
- Generates Rust code from SBE schemas
- Uses Simple Binary Encoding tool
- Outputs generated code to a volume

### 3. `Dockerfile` (main)
- Builds the VEX Core trading engine
- Multi-stage build for optimization

## Docker Compose Files

### 1. `docker-compose.yml` (Main)
- Includes media driver and vex-core, plus the SBE generator, which runs only when named
- Proper service dependencies and health checks
- **Development only.** vex-core journals its commands to the archive in the media driver's
  container, and every start replays them. The journal lives in that container, so `docker compose
  down` deletes it
- `VEX_ENV` must be set to `development` or `test` in the shell that runs compose. Compose passes it
  through; without it vex-core exits at startup (`No environment selected`)

## Available Commands

Use the helper script `./docker-scripts.sh` with these commands:

| Command | Description |
|---------|-------------|
| `generate-sbe` | Generate SBE Rust code |
| `start-full` | Start complete application |
| `stop-all` | Stop all containers |
| `clean-all` | Clean up containers and volumes |
| `logs-media-driver` | Show media driver logs |
| `logs-sbe` | Show SBE generator logs |
| `help` | Show help message |

## Manual Docker Commands

If you prefer to use Docker directly:

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
   docker compose logs media-driver
   ```

2. **Verify health check**:
   ```bash
   docker compose ps
   ```

3. **Check Aeron directory**:
   ```bash
   ls -la /dev/shm/aeron-test-server
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

1. **Permission issues**: Ensure Docker has proper permissions
2. **Volume issues**: Clean volumes with `./docker-scripts.sh clean-all`

## Development Workflow

1. **Generate SBE code**:
   ```bash
   ./docker-scripts.sh generate-sbe
   ```

2. **Run full application**:
   ```bash
   VEX_ENV=development ./docker-scripts.sh start-full
   ```

## Environment Variables

### Media Driver
- Configured by the image's Java system properties (`archive.Dockerfile`): Aeron directory
  `/dev/shm/aeron-test-server`, archive control channel `aeron:udp?endpoint=localhost:8010`

### VEX Core
- `VEX_ENV=development|test|production` - Required explicit environment selection
- `RUST_LOG=info` - Rust logging level
- `VEX__KAFKA_BROKER`, `VEX__SCHEMA_REGISTRY_URL` - Kafka broker and Schema Registry URL (config `kafka_broker`, `schema_registry_url`), passed through from the calling shell. The compose stack runs neither, and the development defaults (`localhost:9092`, `http://localhost:8081`) point at the container itself, so set both. Without a reachable Kafka and registry, events back up and order intake halts (RUNBOOK "Kafka is Down")
- Aeron IPC directory: the image sets `/dev/shm/aeron` with `VEX__CORE_NETWORKING__CONTEXT_DIR` (and the bind address with `VEX__CORE_NETWORKING__LOCAL_ADDRESS=0.0.0.0`); `docker-compose.yml` overrides it with the media driver's `/dev/shm/aeron-test-server`. vex-core does not read `AERON_DIR`.

## Clean Up

To clean up everything:
```bash
./docker-scripts.sh clean-all
```

This will:
- Stop all containers
- Remove all volumes
- Clean up Docker system 
