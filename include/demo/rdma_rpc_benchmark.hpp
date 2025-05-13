/*
 * RDMA/RPC Benchmark Framework
 *
 * This file contains the benchmark framework for UCXX and Seastar integration.
 */
#pragma once

#include <memory>
#include <string>
#include <vector>
#include <benchmark/benchmark.h>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/util/log.hh>

#include "demo/rdma_server.hpp"
#include "demo/rdma_client.hpp"

namespace demo {

// Benchmark configuration
struct BenchmarkConfig {
    // Server configuration
    std::string server_addr = "127.0.0.1";
    uint16_t rpc_port = 10000;
    uint16_t ucxx_port = 10001;
    size_t memory_size = 10 * 1024 * 1024;  // 10 MB

    // Client configuration
    uint32_t payload_size = 1024;
    uint32_t num_iterations = 1000;
    uint32_t concurrency = 32;
    bool use_rdma = true;

    // Benchmark parameters
    std::vector<size_t> payload_sizes = {64, 256, 1024, 4096, 16384, 65536};

    // Output configuration
    bool verbose = false;
    std::string output_file = "logs/benchmark.txt";
    bool append_output = false;
};

// Base class for benchmarks
class BenchmarkBase {
protected:
    BenchmarkConfig _config;

public:
    explicit BenchmarkBase(const BenchmarkConfig& config) : _config(config) {}
    virtual ~BenchmarkBase() = default;

    // Run the benchmark
    virtual void run(benchmark::State& state) = 0;

    // Get the benchmark configuration
    const BenchmarkConfig& getConfig() const {
        return _config;
    }
};

// RPC benchmark
class RPCBenchmark : public BenchmarkBase {
private:
    std::unique_ptr<RdmaServer> _server;
    std::unique_ptr<RdmaClient> _client;

public:
    explicit RPCBenchmark(const BenchmarkConfig& config);
    ~RPCBenchmark() override;

    // Initialize the benchmark
    bool initialize();

    // Run the benchmark
    void run(benchmark::State& state) override;

    // Run the server
    seastar::future<> runServer();

    // Run the client
    seastar::future<std::vector<uint64_t>> runClient(size_t payload_size, uint32_t iterations);

    // Stop the benchmark
    seastar::future<> stop();
};

// RDMA benchmark
class RDMABenchmark : public BenchmarkBase {
private:
    std::unique_ptr<RdmaServer> _server;
    std::unique_ptr<RdmaClient> _client;

public:
    explicit RDMABenchmark(const BenchmarkConfig& config);
    ~RDMABenchmark() override;

    // Initialize the benchmark
    bool initialize();

    // Run the benchmark
    void run(benchmark::State& state) override;

    // Run the server
    seastar::future<> runServer();

    // Run the client
    seastar::future<std::vector<uint64_t>> runClient(size_t payload_size, uint32_t iterations);

    // Stop the benchmark
    seastar::future<> stop();
};

// Utility functions
namespace benchmark_utils {

// Run a Seastar application with the given function
template<typename Func>
int run_app(Func&& func) {
    seastar::app_template app;

    return app.run(0, nullptr, [func = std::forward<Func>(func)]() {
        return func().then([] {
            return seastar::make_ready_future<int>(0);
        }).handle_exception([](std::exception_ptr ep) {
            try {
                std::rethrow_exception(ep);
            } catch (const std::exception& e) {
                std::cerr << "Error: " << e.what() << std::endl;
            }
            return seastar::make_ready_future<int>(1);
        });
    });
}

// Calculate statistics from latencies
struct LatencyStats {
    uint64_t min;
    uint64_t max;
    uint64_t p50;
    uint64_t p90;
    uint64_t p99;
    double avg;
    size_t count;
};

LatencyStats calculate_stats(const std::vector<uint64_t>& latencies);

// Save benchmark results to file
void save_results(const std::string& filename,
                 const std::string& benchmark_name,
                 size_t payload_size,
                 const LatencyStats& stats,
                 bool append = false);

} // namespace benchmark_utils

} // namespace demo
