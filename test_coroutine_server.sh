#!/bin/bash
# Test script for coroutine-based HTTP server

echo "Building server..."
cd "$(dirname "$0")/build"
make -j$(nproc) iocontext_http_server

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

echo "Starting server in background..."
./bin/iocontext_http_server &
SERVER_PID=$!

# Wait for server to start
sleep 2

echo "Testing server endpoints..."

# Test root endpoint
echo -n "Testing /: "
RESPONSE=$(curl -s http://localhost:8080/)
if [[ "$RESPONSE" == *"Hello, World!"* ]]; then
    echo "✓ PASS"
else
    echo "✗ FAIL"
    echo "Response: $RESPONSE"
fi

# Test /hello endpoint
echo -n "Testing /hello: "
RESPONSE=$(curl -s http://localhost:8080/hello)
if [[ "$RESPONSE" == "Hello, World!" ]]; then
    echo "✓ PASS"
else
    echo "✗ FAIL"
    echo "Response: $RESPONSE"
fi

# Test keep-alive with multiple requests
echo -n "Testing keep-alive: "
RESPONSE=$(curl -s http://localhost:8080/ http://localhost:8080/hello)
if [[ "$RESPONSE" == *"Hello, World!"* ]]; then
    echo "✓ PASS"
else
    echo "✗ FAIL"
fi

# Test concurrent connections
echo -n "Testing concurrent connections: "
for i in {1..10}; do
    curl -s http://localhost:8080/ > /dev/null &
done
wait
echo "✓ PASS"

echo "Stopping server..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null

echo "All tests completed!"
