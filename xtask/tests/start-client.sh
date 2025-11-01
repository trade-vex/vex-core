#!/bin/bash
# This script starts the Aeron media driver in the background,
# and then waits indefinitely.

echo "Starting Aeron Media Driver for Client..."
/usr/local/bin/aeronmd &

echo "Client container is ready and waiting for test commands..."

# Use tail to wait forever. This keeps the container alive.
# The 'exec' command ensures that this becomes the container's main process (PID 1),
# so it receives signals correctly for shutdown.
exec tail -f /dev/null