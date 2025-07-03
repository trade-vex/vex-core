# ---------- Build stage ----------
FROM ubuntu:22.04 as builder

# Install build dependencies
RUN apt update && apt install -y \
    git \
    curl \
    build-essential \
    openjdk-17-jdk-headless \
    cmake \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Clone Aeron repository and build
RUN git clone https://github.com/real-logic/aeron.git /opt/aeron
WORKDIR /opt/aeron

# Build Aeron driver
RUN ./cppbuild/cppbuild --no-tests

# ---------- Runtime stage ----------
FROM ubuntu:22.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/aeron

# Copy built aeron from builder stage
COPY --from=builder /opt/aeron /opt/aeron

# Ensure script is executable
RUN chmod +x ./aeron-samples/scripts/logging-c-media-driver

# Create aeron directory with proper permissions
RUN mkdir -p /tmp/aeron && chmod 755 /tmp/aeron

# Set environment variables
ENV AERON_DIR=/tmp/aeron
ENV AERON_EVENT_LOG=all

# Expose Aeron IPC directory
VOLUME ["/tmp/aeron"]

# Use a startup script to handle directory cleanup and health check
RUN echo '#!/bin/bash\n\
# Clean up any existing aeron files\n\
rm -rf /opt/aeron/*\n\
# Ensure directory exists with correct permissions\n\
mkdir -p /opt/aeron\n\
chmod 755 /opt/aeron\n\
# Start the media driver\n\
exec ./cppbuild/Release/binaries/aeronmd "$@"' > /opt/aeron/start-driver.sh \
    && chmod +x /opt/aeron/start-driver.sh    
# Create health check script
RUN echo '#!/bin/bash\n\
# Check if media driver is running by looking for PID file\n\
if [ -f "/tmp/aeron/driver.pid" ]; then\n\
    PID=$(cat /tmp/aeron/driver.pid)\n\
    if kill -0 "$PID" 2>/dev/null; then\n\
        exit 0\n\
    fi\n\
fi\n\
exit 1' > /opt/aeron/health-check.sh \
    && chmod +x /opt/aeron/health-check.sh

# Add health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD ["/opt/aeron/health-check.sh"]

CMD ["sh", "-c", "AERON_DIR=/tmp/aeron AERON_EVENT_LOG=all ./start-driver.sh 2>&1 | tee /tmp/start.log"]