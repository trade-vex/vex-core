# Aeron's ArchivingMediaDriver, as `make server` runs it (Makefile, `media-driver`): the media
# driver vex-server uses, and the Aeron Archive that journals its commands, so that journaling and
# replay are real in the end-to-end tests. Its archive lives in this container, and vex-server
# shares its network and IPC namespaces (docker-compose.yml), so the journal outlives a vex-server
# that is killed and restarted.
FROM eclipse-temurin:21.0.13_9-jre

# The Java build of the Aeron version that xtask/tests/bin holds (media-driver.Dockerfile).
# Readable by any user: CI's live recovery tests run this image as the runner's user (ci.yml), and
# ADD from a URL leaves the file readable by root only. ADD gives the directory it creates the
# same mode, so that mode must let anyone search it too.
ADD --chmod=755 --checksum=sha256:9f7aa82ecfa2633adbf79b24af6f43ae940f1ec58164be88ca09dac8d275b3a4 \
    https://repo1.maven.org/maven2/io/aeron/aeron-all/1.49.0/aeron-all-1.49.0.jar \
    /opt/aeron/aeron-all.jar

# The directory and archive control channel are vex-server's development defaults
# (vex-config CoreNetworkingConfig::development_defaults).
ENTRYPOINT ["java", "-Xms64m", "-Xmx512m", \
    "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED", \
    "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", \
    "-Daeron.dir=/dev/shm/aeron-test-server", \
    "-Daeron.dir.delete.on.start=true", \
    "-Daeron.spies.simulate.connection=true", \
    "-Daeron.archive.dir=/var/lib/aeron-archive", \
    "-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:8010", \
    "-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0", \
    "-cp", "/opt/aeron/aeron-all.jar", "io.aeron.archive.ArchivingMediaDriver"]
