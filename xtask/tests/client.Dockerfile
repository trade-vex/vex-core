# ---- Chef Planner Stage ----
# Edition 2024 requires Rust 1.85+, use latest for full feature support
FROM --platform=$BUILDPLATFORM rust:latest AS chef
RUN cargo install cargo-chef
WORKDIR /usr/src/app

# ---- Chef Recipe Stage ----
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---- Builder Stage ----
FROM chef AS builder
WORKDIR /usr/src/app

# Install system dependencies + mold linker for faster linking
RUN apt-get update && apt-get install -y wget build-essential clang pkg-config git libbsd-dev default-jdk libssl-dev protobuf-compiler \
    && ARCH=$(uname -m) && if [ "$ARCH" = "aarch64" ]; then ARCH="aarch64"; else ARCH="x86_64"; fi \
    && wget https://github.com/Kitware/CMake/releases/download/v3.30.0/cmake-3.30.0-linux-${ARCH}.tar.gz \
    && tar -xzf cmake-3.30.0-linux-${ARCH}.tar.gz --strip-components=1 -C /usr/local \
    && rm cmake-3.30.0-linux-${ARCH}.tar.gz \
    && wget https://github.com/rui314/mold/releases/download/v2.35.1/mold-2.35.1-${ARCH}-linux.tar.gz \
    && tar -xzf mold-2.35.1-${ARCH}-linux.tar.gz -C /usr/local --strip-components=1 \
    && rm mold-2.35.1-${ARCH}-linux.tar.gz \
    && rm -rf /var/lib/apt/lists/*

RUN rustup component add rustfmt

# Configure mold as default linker
ENV RUSTFLAGS="-C link-arg=-fuse-ld=mold"

# Build dependencies - this layer is cached unless dependencies change
COPY --from=planner /usr/src/app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Build application
COPY . .
RUN cargo build --release --bin test_client --package xtask

# ---- Final Stage ----
FROM --platform=$BUILDPLATFORM debian:trixie-slim
# Install runtime dependencies for Aeron C++ libraries and iproute2 for tc (traffic control) in e2e tests
RUN apt-get update && apt-get install -y \
    libbsd0 \
    libstdc++6 \
    libgcc-s1 \
    iproute2 \
    && rm -rf /var/lib/apt/lists/*
# Copy the built test client binary
COPY --from=builder /usr/src/app/target/release/test_client /usr/local/bin/test_client
# Copy the Aeron media driver and the entrypoint script
COPY ./xtask/tests/bin/lib /usr/local/lib/
RUN ldconfig
COPY ./xtask/tests/bin/aeronmd /usr/local/bin/aeronmd
COPY ./xtask/tests/start-client.sh /usr/local/bin/start-client.sh
RUN chmod +x /usr/local/bin/start-client.sh

ENTRYPOINT ["/usr/local/bin/start-client.sh"]
