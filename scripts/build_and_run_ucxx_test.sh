#!/bin/bash

# Script to build and run the UCXX installation test

# Create build directory if it doesn't exist
mkdir -p build
cd build

# Configure with CMake
echo "Configuring with CMake..."
cmake .. -DBUILD_UCXX_TEST=ON

# Build the UCXX test
echo "Building UCXX test..."
cmake --build . --target ucxx_test

# Run the UCXX test
echo "Running UCXX test..."
./bin/ucxx_test

# Check the exit status
if [ $? -eq 0 ]; then
    echo "UCXX installation test completed successfully!"
else
    echo "UCXX installation test failed!"
fi
