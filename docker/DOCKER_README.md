# Docker Setup Guser_ide

This guser_ide explains how to use the Docker setup for the VEX Core project, including the media driver and SBE code generation.

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
./docker-scripts.sh start-full
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
docker-compose up --build
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
   ./docker-scripts.sh start-full
   ```

## Environment Variables

### Media Driver
- `AERON_DIR=/tmp/aeron` - Aeron IPC directory
- `AERON_EVENT_LOG=all` - Enable all event logging

### VEX Core
- `RUST_LOG=info` - Rust logging level
- `AERON_DIR=/tmp/aeron` - Aeron IPC directory (must match media driver)

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