# Dockerfile for Aeron C Media Driver
FROM --platform=$BUILDPLATFORM ubuntu:22.04 AS builder

# Avoid prompts from apt
ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    cmake \
    openjdk-17-jdk-headless \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /build

# Pin Aeron version for cache stability and clone with shallow history
ARG AERON_VERSION=1.49.0
RUN git clone --depth 1 --branch ${AERON_VERSION} https://github.com/real-logic/aeron.git

# Change to aeron directory
WORKDIR /build/aeron

# Build Aeron C++ components including C Media Driver with parallel jobs
# The cppbuild script uses CMAKE_BUILD_PARALLEL_LEVEL env variable
RUN CMAKE_BUILD_PARALLEL_LEVEL=$(nproc) ./cppbuild/cppbuild --no-tests

# Create runtime image
FROM rust:1.79 AS runtime

# Install runtime dependencies
# RUN apt-get update && apt-get install -y \
#     # libbsd0 \
#     libuuser_id1 \
#     # libssl3 \
#     # zlib1g \
#     && rm -rf /var/lib/apt/lists/*

# Copy the built C Media Driver from builder stage
COPY --from=builder /build/aeron/cppbuild/Release/binaries/aeronmd /usr/local/bin/aeronmd
COPY --from=builder /build/aeron/cppbuild/Release/lib/ /usr/local/lib/

# Make it executable
RUN chmod +x /usr/local/bin/aeronmd

# Create output volume for binary extraction
VOLUME ["/output"]

# Copy aeronmd binary to output volume
CMD ["sh", "-c", "mkdir -p /output/lib && cp /usr/local/bin/aeronmd /output/ && cp -r /usr/local/lib/* /output/lib && echo 'aeronmd binary and libraries copied to /output/'"]
