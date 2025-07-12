# Multi-stage build for Rust application
FROM rust:latest as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy all source code
COPY . .

# Build the application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the built binary
COPY --from=builder /app/target/release/vex-core /usr/local/bin/vex-core

# Create shared volume for Aeron IPC
VOLUME ["/tmp/aeron"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD pgrep -f vex-core || exit 1

# Set environment variables
ENV RUST_LOG=info
ENV AERON_DIR=/tmp/aeron

CMD ["vex-core"]