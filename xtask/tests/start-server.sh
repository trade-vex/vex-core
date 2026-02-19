#!/bin/bash
echo "Starting Aeron Media Driver..."
/usr/local/bin/aeronmd &

# Wait for CnC file to be created by the media driver
CNC_FILE="/dev/shm/aeron-test-server/cnc.dat"
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

echo "Starting Test Server..."
exec /usr/local/bin/test_server
