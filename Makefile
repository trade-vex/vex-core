.PHONY: all build clean aeron media-driver aeron-stat aeron-errors server test help

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
	@echo "  test          - Run test suite"
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
	cargo run --bin vex-core

test: build
	cargo run --bin run_test_suite
