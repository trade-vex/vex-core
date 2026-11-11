# The test stack's vex-server and vex-client images (docker-compose.yml selects them by target),
# built together: one dependency cook and one release build for both binaries.

# ---- Chef Planner Stage ----
# Edition 2024 requires Rust 1.85+, use latest for full feature support
FROM --platform=$BUILDPLATFORM rust:latest AS chef
RUN cargo install --locked cargo-chef@0.1.78
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
RUN cargo chef cook --release --locked --recipe-path recipe.json

# Build application: the core binary that ships, and the test client
COPY . .
RUN cargo build --release --locked --package vex-core --package xtask --bin vex-core --bin test_client

# The Aeron client libraries the rusteron build scripts produced, as the root Dockerfile ships them.
# One cp per file: rusteron-client and rusteron-archive each build a libaeron.so of the same Aeron
# here, and a single cp refuses to write one name twice.
RUN mkdir -p /usr/src/app/runtime-libs \
    && find target/release -type f \
        \( -name 'libaeron.so' \
        -o -name 'libaeron_driver.so' \
        -o -name 'libaeron_archive_c_client.so' \) \
        -exec cp -v -t /usr/src/app/runtime-libs {} \; \
    && test -f /usr/src/app/runtime-libs/libaeron.so \
    && test -f /usr/src/app/runtime-libs/libaeron_driver.so \
    && test -f /usr/src/app/runtime-libs/libaeron_archive_c_client.so

# ---- vex-server: the core; its media driver runs in the aeron-archive container ----
FROM --platform=$BUILDPLATFORM debian:trixie-slim AS server
# iproute2 for tc (traffic control) in e2e tests, procps for the health check's pgrep
RUN apt-get update && apt-get install -y \
    libbsd0 \
    libstdc++6 \
    libgcc-s1 \
    iproute2 \
    procps \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/app/runtime-libs/ /usr/local/lib/
RUN ldconfig
# Configured through VEX__ variables in docker-compose.yml
COPY --from=builder /usr/src/app/target/release/vex-core /usr/local/bin/vex-core
ENTRYPOINT ["/usr/local/bin/vex-core"]

# ---- vex-client: the test client and its media driver, Aeron's Java MediaDriver ----
# The base and jar of archive.Dockerfile: every stack runs the one driver version (RUNBOOK).
FROM eclipse-temurin:21.0.13_9-jre AS client
RUN apt-get update && apt-get install -y libbsd0 iproute2 && rm -rf /var/lib/apt/lists/*
ARG AERON_VERSION=1.49.0
ARG AERON_SHA256=9f7aa82ecfa2633adbf79b24af6f43ae940f1ec58164be88ca09dac8d275b3a4
ADD --chmod=755 --checksum=sha256:${AERON_SHA256} \
    https://repo1.maven.org/maven2/io/aeron/aeron-all/${AERON_VERSION}/aeron-all-${AERON_VERSION}.jar \
    /opt/aeron/aeron-all.jar
COPY --from=builder /usr/src/app/runtime-libs/ /usr/local/lib/
RUN ldconfig
COPY --from=builder /usr/src/app/target/release/test_client /usr/local/bin/test_client
COPY --chmod=755 ./xtask/tests/start-client.sh /usr/local/bin/start-client.sh
ENTRYPOINT ["/usr/local/bin/start-client.sh"]
