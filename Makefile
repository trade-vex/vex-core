.PHONY: all build clean aeron media-driver stop-media-driver media-driver-gateway stop-media-driver-gateway aeron-stat aeron-errors server test help

AERON_JAR := aeron-all-1.49.0.jar
AERON_DIR := /dev/shm/aeron-test-server
AERON_DIR_GATEWAY := /dev/shm/aeron-test-client
PID_FILE := /tmp/vex-media-driver.pid
PID_FILE_GATEWAY := /tmp/vex-media-driver-gateway.pid

JAVA_OPTIONS := -XX:+UnlockExperimentalVMOptions \
                -XX:+TrustFinalNonStaticFields \
                -XX:+UnlockDiagnosticVMOptions \
                -XX:GuaranteedSafepointInterval=300000 \
                -XX:+UseParallelGC

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
	@echo "  build                      - Build project"
	@echo "  aeron                      - Download Aeron JAR"
	@echo "  media-driver               - Start media driver in background"
	@echo "  stop-media-driver          - Stop running media driver"
	@echo "  media-driver-gateway       - Start gateway media driver in background"
	@echo "  stop-media-driver-gateway  - Stop running gateway media driver"
	@echo "  aeron-stat                 - Show Aeron statistics"
	@echo "  aeron-errors               - Show Aeron error stats"
	@echo "  server                     - Start VEX server (logs to terminal)"
	@echo ""
	@echo "  test          - Run the integration suite on the Docker stack (cargo xtask test integration;"
	@echo "                  other suites: cargo xtask test integration [balance|gtc|ioc|fok|cancellation])"
	@echo ""
	@echo "  clean         - Clean build artifacts"

build:
	cargo build --workspace --release

clean:
	cargo clean
	rm -f $(AERON_JAR)

aeron:
	@if [ ! -f "$(AERON_JAR)" ]; then \
		echo "Downloading Aeron JAR..."; \
		wget https://repo1.maven.org/maven2/io/aeron/aeron-all/1.49.0/$(AERON_JAR); \
	fi

media-driver: aeron
	@if [ -f "$(PID_FILE)" ]; then \
		PID=$$(cat $(PID_FILE)); \
		if kill -0 $$PID 2>/dev/null; then \
			echo "Media driver already running (PID: $$PID)"; \
		else \
			echo "Stale PID file found. Starting media driver..."; \
			rm -f $(PID_FILE); \
			nohup java $(JAVA_OPTIONS) $(JAVA_OPTS) $(ADD_OPENS) $(AERON_PROPS) \
				-cp $(AERON_JAR) io.aeron.archive.ArchivingMediaDriver \
				> /dev/null 2>&1 & echo $$! > $(PID_FILE); \
			echo "Media driver started (PID: $$(cat $(PID_FILE)))"; \
		fi; \
	else \
		echo "Starting media driver in background..."; \
		nohup java $(JAVA_OPTIONS) $(JAVA_OPTS) $(ADD_OPENS) $(AERON_PROPS) \
			-cp $(AERON_JAR) io.aeron.archive.ArchivingMediaDriver \
			> /dev/null 2>&1 & echo $$! > $(PID_FILE); \
		echo "Media driver started (PID: $$(cat $(PID_FILE)))"; \
		echo "Use 'make stop-media-driver' to stop it"; \
	fi

stop-media-driver:
	@if [ ! -f "$(PID_FILE)" ]; then \
		echo "No PID file found. Media driver may not be running."; \
		exit 1; \
	fi
	@PID=$$(cat $(PID_FILE)); \
	if kill -0 $$PID 2>/dev/null; then \
		echo "Stopping media driver (PID: $$PID)..."; \
		kill $$PID && rm -f $(PID_FILE); \
		echo "Media driver stopped"; \
	else \
		echo "Process $$PID not found (may have already stopped)"; \
		rm -f $(PID_FILE); \
	fi

aeron-stat: aeron
	exec java $(JAVA_OPTIONS) $(JAVA_OPTS) $(ADD_OPENS) -Daeron.dir="$(AERON_DIR)" \
		-cp $(AERON_JAR) io.aeron.samples.AeronStat

aeron-errors: aeron
	exec java $(JAVA_OPTIONS) $(JAVA_OPTS) $(ADD_OPENS) -Daeron.dir="$(AERON_DIR)" \
		-cp $(AERON_JAR) io.aeron.samples.ErrorStat

server: build media-driver
	@-killall -q vex-core 2>/dev/null || true
	@sleep 1
	RUST_LOG=info cargo run --bin vex-core

test:
	cargo xtask test integration

media-driver-gateway: aeron
	@if [ -f "$(PID_FILE_GATEWAY)" ]; then \
		PID=$$(cat $(PID_FILE_GATEWAY)); \
		if kill -0 $$PID 2>/dev/null; then \
			echo "Media driver for gateway already running (PID: $$PID)"; \
		else \
			echo "Stale PID file found. Starting gateway media driver..."; \
			rm -f $(PID_FILE_GATEWAY); \
			nohup java $(JAVA_OPTIONS) $(JAVA_OPTS) $(ADD_OPENS) \
				-Daeron.dir="$(AERON_DIR_GATEWAY)" \
				-cp $(AERON_JAR) io.aeron.driver.MediaDriver \
				> /dev/null 2>&1 & echo $$! > $(PID_FILE_GATEWAY); \
			echo "Media driver for gateway started (PID: $$(cat $(PID_FILE_GATEWAY)))"; \
		fi; \
	else \
		echo "Starting gateway media driver in background..."; \
		nohup java $(JAVA_OPTIONS) $(JAVA_OPTS) $(ADD_OPENS) \
			-Daeron.dir="$(AERON_DIR_GATEWAY)" \
			-cp $(AERON_JAR) io.aeron.driver.MediaDriver \
			> /dev/null 2>&1 & echo $$! > $(PID_FILE_GATEWAY); \
		echo "Media driver for gateway started (PID: $$(cat $(PID_FILE_GATEWAY)))"; \
		echo "Use 'make stop-media-driver-gateway' to stop it"; \
	fi

stop-media-driver-gateway:
	@if [ ! -f "$(PID_FILE_GATEWAY)" ]; then \
		echo "No PID file found. Gateway media driver may not be running."; \
		exit 1; \
	fi
	@PID=$$(cat $(PID_FILE_GATEWAY)); \
	if kill -0 $$PID 2>/dev/null; then \
		echo "Stopping gateway media driver (PID: $$PID)..."; \
		kill $$PID && rm -f $(PID_FILE_GATEWAY); \
		echo "Gateway media driver stopped"; \
	else \
		echo "Process $$PID not found (may have already stopped)"; \
		rm -f $(PID_FILE_GATEWAY); \
	fi
