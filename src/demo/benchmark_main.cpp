/*
 * RDMA/RPC Benchmark Main
 *
 * This file contains the main function for the RDMA/RPC benchmark.
 */
#include <iostream>
#include <string>
#include <vector>
#include <benchmark/benchmark.h>
#include "demo/rdma_rpc_benchmark.hpp"

// Register RPC benchmark
static void BM_RPC(benchmark::State& state) {
    demo::BenchmarkConfig config;
    config.payload_size = state.range(0);
    config.num_iterations = 1000;  // Reduced for benchmarking
    config.concurrency = 32;
    config.use_rdma = false;
    config.verbose = false;
    config.output_file = "logs/benchmark.txt";
    
    demo::RPCBenchmark benchmark(config);
    if (!benchmark.initialize()) {
        state.SkipWithError("Failed to initialize RPC benchmark");
        return;
    }
    
    benchmark.run(state);
}

// Register RDMA benchmark
static void BM_RDMA(benchmark::State& state) {
    demo::BenchmarkConfig config;
    config.payload_size = state.range(0);
    config.num_iterations = 1000;  // Reduced for benchmarking
    config.concurrency = 32;
    config.use_rdma = true;
    config.verbose = false;
    config.output_file = "logs/benchmark.txt";
    
    demo::RDMABenchmark benchmark(config);
    if (!benchmark.initialize()) {
        state.SkipWithError("Failed to initialize RDMA benchmark");
        return;
    }
    
    benchmark.run(state);
}

// Define benchmark parameters
static void benchmark_args(benchmark::internal::Benchmark* b) {
    // Test with different payload sizes
    b->Arg(64)      // 64 bytes
     ->Arg(256)     // 256 bytes
     ->Arg(1024)    // 1 KB
     ->Arg(4096)    // 4 KB
     ->Arg(16384)   // 16 KB
     ->Arg(65536);  // 64 KB
}

// Register benchmarks
BENCHMARK(BM_RPC)->Apply(benchmark_args)->UseRealTime()->Unit(benchmark::kMillisecond);
BENCHMARK(BM_RDMA)->Apply(benchmark_args)->UseRealTime()->Unit(benchmark::kMillisecond);

// Main function
int main(int argc, char** argv) {
    // Initialize benchmark
    ::benchmark::Initialize(&argc, argv);
    
    // Create logs directory if it doesn't exist
    system("mkdir -p logs");
    
    // Run the benchmarks
    ::benchmark::RunSpecifiedBenchmarks();
    
    return 0;
}
