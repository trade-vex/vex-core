# ---- Builder Stage ----
FROM rust:1.88 as builder
WORKDIR /usr/src/app
RUN apt-get update && apt-get install -y wget iproute2 build-essential clang pkg-config git \
    && wget https://github.com/Kitware/CMake/releases/download/v3.30.0/cmake-3.30.0-linux-x86_64.tar.gz \
    && tar -xzvf cmake-3.30.0-linux-x86_64.tar.gz --strip-components=1 -C /usr/local \
    && rm cmake-3.30.0-linux-x86_64.tar.gz
COPY . .
# Build the dedicated 'test_server' binary
RUN ls -a
RUN rustup component add rustfmt
RUN cargo build --release --bin test_client --package xtask

# ---- Final Stage ----
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y iproute2
# Copy the built test client binary
COPY --from=builder /usr/src/app/target/release/test_client /usr/local/bin/test_client
# Copy the Aeron media driver and the entrypoint script
COPY ./xtask/tests/bin/lib /usr/local/lib/
COPY ./xtask/tests/bin/aeronmd /usr/local/bin/aeronmd
COPY ./xtask/tests/start-client.sh /usr/local/bin/start-client.sh
RUN chmod +x /usr/local/bin/start-client.sh

ENTRYPOINT ["/usr/local/bin/start-client.sh"]