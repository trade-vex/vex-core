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

# Create aeron directory with proper permissions
RUN mkdir -p /tmp/aeron && chmod 755 /tmp/aeron

# Set environment variables
ENV AERON_DIR=/tmp/aeron
ENV AERON_EVENT_LOG=all

# Expose Aeron IPC directory
VOLUME ["/tmp/aeron"]

# Create a proper startup script that handles directory conflicts
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# Set proper environment\n\
export AERON_DIR=/tmp/aeron\n\
export AERON_EVENT_LOG=all\n\
\n\
echo "Starting Aeron Media Driver..."\n\
echo "AERON_DIR: $AERON_DIR"\n\
echo "AERON_EVENT_LOG: $AERON_EVENT_LOG"\n\
\n\
# Clean up any existing aeron files but preserve the directory\n\
if [ -d "/tmp/aeron" ]; then\n\
    echo "Cleaning existing aeron files..."\n\
    find /tmp/aeron -mindepth 1 -delete 2>/dev/null || true\n\
else\n\
    echo "Creating aeron directory..."\n\
    mkdir -p /tmp/aeron\n\
fi\n\
\n\
# Set proper permissions\n\
chmod 755 /tmp/aeron\n\
\n\
# Start the media driver\n\
cd /opt/aeron\n\
exec ./cppbuild/Release/binaries/aeronmd' > /opt/aeron/start-driver.sh \
    && chmod +x /opt/aeron/start-driver.sh
    
# Create health check script
RUN echo '#!/bin/bash\n\
# Check if media driver is running by looking for the process\n\
if pgrep -f "aeronmd" > /dev/null; then\n\
    echo "Media driver is running"\n\
    exit 0\n\
else\n\
    echo "Media driver is not running"\n\
    exit 1\n\
fi' > /opt/aeron/health-check.sh \
    && chmod +x /opt/aeron/health-check.sh

# Add health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD ["/opt/aeron/health-check.sh"]

# Expose port for monitoring (optional)
EXPOSE 8080

CMD ["/opt/aeron/start-driver.sh"]