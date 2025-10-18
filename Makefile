.PHONY: all build clean aeron media-driver aeron-stat aeron-errors server test test-gtc test-ioc test-fok test-cancel test-balance help

AERON_JAR := aeron-all-1.49.0.jar
AERON_DIR := /dev/shm/aeron-test-server

JAVA_OPTS := -Xms256m -Xmx1g
ADD_OPENS := --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
             --add-opens java.base/jdk.internal.misc=ALL-UNNAMED

AERON_PROPS := -Daeron.dir="$(AERON_DIR)" \
               -Daeron.dir.delete.on.start=true \
               -Daeron.event.archive.log=all \
               -Daeron.spies.simulate.connection=true \
               -Daeron.archive.control.channel=aeron:udp?endpoint=localhost:8010 \
               -Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0 \
               -Daeron.archive.control.response.channel=aeron:udp?endpoint=localhost:0

all: build

help:
	@echo "Available targets:"
	@echo "  build         - Build project"
	@echo "  aeron         - Download Aeron JAR"
	@echo "  media-driver  - Start media driver (logs to terminal)"
	@echo "  aeron-stat    - Show Aeron statistics"
	@echo "  aeron-errors  - Show Aeron error stats"
	@echo "  server        - Start VEX server (logs to terminal)"
	@echo ""
	@echo "Test suite (state-aware, single continuous test per suite):"
	@echo "  test          - Run comprehensive integration test (all order types)"
	@echo "  test-gtc      - Run GTC order tests (3 sections)"
	@echo "  test-ioc      - Run IOC order tests (5 sections)"
	@echo "  test-fok      - Run FOK order tests (6 sections)"
	@echo "  test-cancel   - Run cancellation tests (8 sections)"
	@echo "  test-balance  - Run balance management tests"
	@echo ""
	@echo "  clean         - Clean build artifacts"

build:
	cargo build

clean:
	cargo clean
	rm -f $(AERON_JAR)

aeron:
	@if [ ! -f "$(AERON_JAR)" ]; then \
		echo "Downloading Aeron JAR..."; \
		wget https://repo1.maven.org/maven2/io/aeron/aeron-all/1.49.0/$(AERON_JAR); \
	fi

media-driver: aeron
	@mkdir -p $(AERON_DIR) && chmod 777 $(AERON_DIR)
	@-killall -q java 2>/dev/null || true
	@sleep 1
	exec java $(JAVA_OPTS) $(ADD_OPENS) $(AERON_PROPS) \
		-cp $(AERON_JAR) io.aeron.archive.ArchivingMediaDriver

aeron-stat: aeron
	exec java $(ADD_OPENS) -Daeron.dir="$(AERON_DIR)" \
		-cp $(AERON_JAR) io.aeron.samples.AeronStat

aeron-errors: aeron
	exec java $(ADD_OPENS) -Daeron.dir="$(AERON_DIR)" \
		-cp $(AERON_JAR) io.aeron.samples.ErrorStat

server: build
	@-killall -q vex-core 2>/dev/null || true
	@sleep 1
	RUST_LOG=info cargo run --bin vex-core

test: build
	cargo run --bin run_test_suite all

test-gtc: build
	cargo run --bin run_test_suite gtc

test-ioc: build
	cargo run --bin run_test_suite ioc

test-fok: build
	cargo run --bin run_test_suite fok

test-cancel: build
	cargo run --bin run_test_suite cancellation

test-balance: build
	cargo run --bin run_test_suite balance
