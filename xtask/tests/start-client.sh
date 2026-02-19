#!/bin/bash
# This script starts the Aeron media driver in the background,
# waits for it to initialize, and then waits indefinitely.

echo "Starting Aeron Media Driver for Client..."
/usr/local/bin/aeronmd &

# Wait for CnC file to be created by the media driver
CNC_FILE="/dev/shm/aeron-test-client/cnc.dat"
for i in $(seq 1 30); do
  if [ -f "$CNC_FILE" ]; then
    echo "Aeron Media Driver is ready (CnC file created)."
    break
  fi
  echo "Waiting for Aeron Media Driver... ($i/30)"
  sleep 1
done

if [ ! -f "$CNC_FILE" ]; then
  echo "ERROR: Aeron Media Driver failed to start (CnC file not found at $CNC_FILE)"
  exit 1
fi

echo "Client container is ready and waiting for test commands..."

# Use tail to wait forever. This keeps the container alive.
# The 'exec' command ensures that this becomes the container's main process (PID 1),
# so it receives signals correctly for shutdown.
exec tail -f /dev/null