#!/bin/bash

# Check command line arguments
if [ $# -eq 0 ]; then
    # If no server address is provided, use local address and start a local server
    SERVER_ADDR="127.0.0.1"
    START_LOCAL_SERVER=true
else
    # Use the provided server address
    SERVER_ADDR="$1"
    START_LOCAL_SERVER=false
fi

# Ensure we're in the correct directory
cd /root/kernel_dev/server_dev

# Create logs directory if it doesn't exist
mkdir -p logs

# Clear the log file
> logs/rpc_benchmark.txt

# Record date and time
echo "RPC Benchmark Results - $(date)" >> logs/rpc_benchmark.txt
echo "Server Address: ${SERVER_ADDR}" >> logs/rpc_benchmark.txt
echo "=======================================" >> logs/rpc_benchmark.txt
echo "" >> logs/rpc_benchmark.txt

# Start a local server if needed
if [ "$START_LOCAL_SERVER" = true ]; then
    echo "Starting local RPC server..." >> logs/rpc_benchmark.txt
    ./build/rpc_seastar &
    SERVER_PID=$!

    # Wait for server to start
    sleep 2
else
    echo "Using remote server at ${SERVER_ADDR}" >> logs/rpc_benchmark.txt
fi

# Function to run benchmark tests with different configurations
run_test() {
    local payload_size=$1
    local requests=$2
    local concurrency=$3

    echo "Running test with payload_size=${payload_size}, requests=${requests}, concurrency=${concurrency}" >> logs/rpc_benchmark.txt
    echo "--------------------------------------" >> logs/rpc_benchmark.txt

    # Run the test and redirect output to log file
    ./build/rpc_seastar --server=${SERVER_ADDR} --payload-size=${payload_size} --requests=${requests} --concurrency=${concurrency} >> logs/rpc_benchmark.txt 2>&1

    echo "" >> logs/rpc_benchmark.txt
    echo "" >> logs/rpc_benchmark.txt
}

# Run tests with different configurations
echo "Running benchmark tests..." >> logs/rpc_benchmark.txt
echo "" >> logs/rpc_benchmark.txt

# Test different payload sizes
run_test 60 1000 10
run_test 1024 1000 10
run_test 4096 1000 10

# Test different concurrency levels
run_test 60 1000 1
run_test 60 1000 4
run_test 60 1000 16
run_test 60 1000 32
run_test 60 1000 64
run_test 60 1000 128

# Test different request counts
run_test 60 100 10
run_test 60 10000 10

# Stop the local server if we started one
if [ "$START_LOCAL_SERVER" = true ]; then
    echo "Stopping local RPC server..." >> logs/rpc_benchmark.txt
    kill $SERVER_PID
fi

echo "" >> logs/rpc_benchmark.txt
echo "Benchmark completed." >> logs/rpc_benchmark.txt
echo "Results saved to logs/rpc_benchmark.txt" >> logs/rpc_benchmark.txt

echo "Benchmark completed. Results saved to logs/rpc_benchmark.txt"