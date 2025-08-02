# Dockerfile for Aeron C Media Driver
FROM ubuntu:22.04 as builder

# Avoid prompts from apt
ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    cmake \
    openjdk-17-jdk-headless \
    git \
    libbsd-dev \
    uuid-dev \
    libssl-dev \
    zlib1g-dev \
    doxygen \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /build

# Clone Aeron repository
RUN git clone https://github.com/aeron-io/aeron.git

# Change to aeron directory
WORKDIR /build/aeron

# Build Aeron C++ components including C Media Driver
RUN ./cppbuild/cppbuild --no-tests

# Create runtime image
FROM ubuntu:22.04 as runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libbsd0 \
    libuuid1 \
    libssl3 \
    zlib1g \
    && rm -rf /var/lib/apt/lists/*

# Copy the built C Media Driver from builder stage
COPY --from=builder /build/aeron/cppbuild/Release/binaries/aeronmd /usr/local/bin/aeronmd
COPY --from=builder /build/aeron/cppbuild/Release/lib/ /usr/local/lib/

# Make it executable
RUN chmod +x /usr/local/bin/aeronmd

# Default environment variables for Aeron C Media Driver
ENV AERON_DIR=/opt/aeron-test-server
ENV AERON_TERM_BUFFER_LENGTH=1048576
ENV AERON_THREADING_MODE=DEDICATED
ENV AERON_PRINT_CONFIGURATION=true
ENV AERON_EVENT_LOG=all
ENV LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

# Expose default ports
EXPOSE 40123 40124

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD pgrep aeronmd || exit 1

# Start the C Media Driver
CMD ["aeronmd"]
