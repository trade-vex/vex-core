# Multi-stage build for Rust application
# Base images are pinned by version and digest so a rebuild uses exactly the inputs that were
# validated. Bump the tag and digest together (`docker buildx imagetools inspect <tag>`).
FROM rust:1.99.1-bookworm@sha256:c9155318d07341301a78b45ecdd81af2e43b8b297d64e8fd4c5e80d7735607c3 AS builder

# Install build dependencies. The vendored Aeron needs CMake >= 3.30 and bookworm ships 3.25,
# so CMake comes from PyPI, pinned to the release the build was validated with.
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
    && pip3 install --break-system-packages cmake==4.3.4 \
    && rustup component add rustfmt \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy all source code
COPY . .

# Build the application
RUN cargo build --release --locked

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
FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc

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
