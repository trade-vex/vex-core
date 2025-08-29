#!/bin/bash
echo "Starting Aeron Media Driver for Client..."
/usr/local/bin/aeronmd &
sleep 2

echo "Starting Test Client..."
exec /usr/local/bin/test_server