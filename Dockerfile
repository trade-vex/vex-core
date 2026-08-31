# Multi-stage build for Rust application
FROM rust:1-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    curl \
    pkg-config \
    libssl-dev \
    uuid-dev \
    ca-certificates \
    make \
    gcc \
    g++ \
    clang \
    zlib1g-dev \
    libbsd-dev \
    python3-pip \
    default-jdk \
    protobuf-compiler \
    && pip3 install cmake --upgrade --break-system-packages \
    && rustup component add rustfmt \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy all source code
COPY . .

# Build the application
RUN cargo build --release

# Collect the Aeron shared libraries produced by the rusteron build scripts.
RUN mkdir -p /app/runtime-libs \
    && find /app/target/release -type f \
        \( -name 'libaeron.so' \
        -o -name 'libaeron_driver.so' \
        -o -name 'libaeron_archive_c_client.so' \) \
        -exec cp -v -t /app/runtime-libs {} + \
    && test -f /app/runtime-libs/libaeron.so \
    && test -f /app/runtime-libs/libaeron_driver.so \
    && test -f /app/runtime-libs/libaeron_archive_c_client.so

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libssl3 \
    libbsd0 \
    zlib1g \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the built binary
COPY --from=builder /app/target/release/vex-core /usr/local/bin/vex-core
COPY --from=builder /app/runtime-libs/ /usr/local/lib/
RUN ldconfig

# Create shared volume for Aeron IPC
VOLUME ["/tmp/aeron"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD pgrep -f vex-core || exit 1

# Set environment variables
ENV RUST_LOG=info
ENV AERON_DIR=/tmp/aeron

CMD ["vex-core"]
