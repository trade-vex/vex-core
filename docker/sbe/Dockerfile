FROM rust:1.87-slim AS builder

# Install Java + Gradle for SBE
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    git \
    unzip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set up directories
WORKDIR /app

# Copy your Rust project
COPY . .

# Fetch and build SBE tool
RUN git clone --depth=1 https://github.com/real-logic/simple-binary-encoding.git sbe
WORKDIR /app/sbe

# Build the SBE tool and all dependencies
RUN ./gradlew jar

# Generate Rust code from ordercmd.xml
RUN mkdir /app/generated && java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
  -Dsbe.output.dir=/app/generated \
  -Dsbe.target.language=Rust \
  -Dsbe.target.namespace=sbe \
  -cp "sbe-tool/build/libs/sbe-tool-1.36.0-SNAPSHOT.jar:sbe-all/build/libs/sbe-all-1.36.0-SNAPSHOT.jar" \
  uk.co.real_logic.sbe.SbeTool \
  /app/resources/ordercmd.xml

# Runtime stage
FROM alpine:latest

WORKDIR /app

# Copy the generated code
COPY --from=builder /app/generated /app/generated

# Copy the generated SBE files to a volume for extraction
VOLUME ["/output"]

# Copy generated files to output volume
CMD ["sh", "-c", "cp -r /app/generated/* /output/ && echo 'Success' || exit 1"]  
