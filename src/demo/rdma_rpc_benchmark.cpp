/*
 * RDMA/RPC Benchmark Implementation
 */
#include "demo/rdma_rpc_benchmark.hpp"
#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <numeric>
#include <fmt/core.h>

namespace demo {

// RPC Benchmark implementation
RPCBenchmark::RPCBenchmark(const BenchmarkConfig& config)
    : BenchmarkBase(config),
      _server(std::make_unique<RdmaServer>(config.rpc_port, config.ucxx_port)),
      _client(std::make_unique<RdmaClient>()) {}

RPCBenchmark::~RPCBenchmark() {
    // Server and client will be cleaned up by their destructors
}

bool RPCBenchmark::initialize() {
    // Initialize server
    if (!_server->initialize()) {
        std::cerr << "Failed to initialize server" << std::endl;
        return false;
    }

    // Allocate memory for RDMA
    if (!_server->allocateMemory(_config.memory_size)) {
        std::cerr << "Failed to allocate memory" << std::endl;
        return false;
    }

    // Initialize client
    if (!_client->initialize()) {
        std::cerr << "Failed to initialize client" << std::endl;
        return false;
    }

    return true;
}

void RPCBenchmark::run(benchmark::State& state) {
    // Get payload size from state
    size_t payload_size = state.range(0);

    // Run the benchmark
    for (auto _ : state) {
        // Setup phase - not measured
        state.PauseTiming();

        // Run the server and client
        auto latencies = benchmark_utils::run_app([this, payload_size]() {
            return runServer().then([this, payload_size] {
                return runClient(payload_size, _config.num_iterations);
            });
        });

        // Resume timing for benchmark framework
        state.ResumeTiming();

        // Calculate statistics
        if (!latencies.empty()) {
            auto stats = benchmark_utils::calculate_stats(latencies);

            // Report metrics to benchmark
            state.SetItemsProcessed(_config.num_iterations);
            state.SetBytesProcessed(_config.num_iterations * payload_size);
            state.counters["min_latency"] = stats.min;
            state.counters["max_latency"] = stats.max;
            state.counters["p50_latency"] = stats.p50;
            state.counters["p90_latency"] = stats.p90;
            state.counters["p99_latency"] = stats.p99;
            state.counters["avg_latency"] = stats.avg;

            // Save results to file
            if (!_config.output_file.empty()) {
                benchmark_utils::save_results(
                    _config.output_file,
                    "RPC",
                    payload_size,
                    stats,
                    _config.append_output);

                // Only append for subsequent writes
                _config.append_output = true;
            }
        }
    }
}

seastar::future<> RPCBenchmark::runServer() {
    // Start the server
    return _server->start().then([] {
        if (seastar::engine().cpu_id() == 0) {
            std::cout << "Server started successfully" << std::endl;
        }
        return seastar::make_ready_future<>();
    });
}

seastar::future<std::vector<uint64_t>> RPCBenchmark::runClient(
    size_t payload_size, uint32_t iterations) {

    if (seastar::engine().cpu_id() == 0) {
        std::cout << "Starting RPC benchmark with payload size: "
                  << payload_size << " bytes, iterations: " << iterations << std::endl;
    }

    return seastar::do_with(
        std::vector<uint64_t>{},
        seastar::sstring(seastar::uninitialized_string(payload_size)),
        [this, iterations](auto& latencies, auto& test_payload) {
            // Fill payload with data
            std::fill(test_payload.begin(), test_payload.end(), 'x');

            // Reserve space for latencies
            latencies.reserve(iterations);

            // Connect to server
            return _client->connect(_config.server_addr, _config.rpc_port).then([&, iterations, this] {
                if (seastar::engine().cpu_id() == 0 && _config.verbose) {
                    std::cout << "Connected to server, starting benchmark..." << std::endl;
                }

                // Create semaphore to limit concurrency
                return seastar::do_with(
                    seastar::semaphore(_config.concurrency),
                    [&, iterations, this](auto& limit) {
                        // Create a vector of futures
                        std::vector<seastar::future<>> futures;
                        futures.reserve(iterations);

                        // Launch all requests
                        for (uint32_t i = 0; i < iterations; i++) {
                            auto fut = seastar::with_semaphore(limit, 1, [&, this]() {
                                auto start = std::chrono::high_resolution_clock::now();

                                return _client->echo(test_payload)
                                    .then([&latencies, start](seastar::sstring) {
                                        auto end = std::chrono::high_resolution_clock::now();
                                        auto latency = std::chrono::duration_cast<
                                            std::chrono::microseconds>(end - start).count();
                                        latencies.push_back(latency);
                                    })
                                    .handle_exception([](std::exception_ptr ep) {
                                        try {
                                            std::rethrow_exception(ep);
                                        } catch (const std::exception& e) {
                                            std::cerr << "Request failed: " << e.what() << std::endl;
                                        }
                                    });
                            });
                            futures.push_back(std::move(fut));
                        }

                        // Wait for all futures to complete
                        return seastar::when_all(futures.begin(), futures.end())
                            .discard_result()
                            .then([&latencies] {
                                return std::move(latencies);
                            });
                    });
            }).finally([this] {
                return _client->stop();
            });
        });
}

seastar::future<> RPCBenchmark::stop() {
    return _server->stop();
}

// RDMA Benchmark implementation
RDMABenchmark::RDMABenchmark(const BenchmarkConfig& config)
    : BenchmarkBase(config),
      _server(std::make_unique<RdmaServer>(config.rpc_port, config.ucxx_port)),
      _client(std::make_unique<RdmaClient>()) {}

RDMABenchmark::~RDMABenchmark() {
    // Server and client will be cleaned up by their destructors
}

bool RDMABenchmark::initialize() {
    // Initialize server
    if (!_server->initialize()) {
        std::cerr << "Failed to initialize server" << std::endl;
        return false;
    }

    // Allocate memory for RDMA
    if (!_server->allocateMemory(_config.memory_size)) {
        std::cerr << "Failed to allocate memory" << std::endl;
        return false;
    }

    // Initialize client
    if (!_client->initialize()) {
        std::cerr << "Failed to initialize client" << std::endl;
        return false;
    }

    return true;
}

void RDMABenchmark::run(benchmark::State& state) {
    // Get payload size from state
    size_t payload_size = state.range(0);

    // Run the benchmark
    for (auto _ : state) {
        // Setup phase - not measured
        state.PauseTiming();

        // Run the server and client
        auto latencies = benchmark_utils::run_app([this, payload_size]() {
            return runServer().then([this, payload_size] {
                return runClient(payload_size, _config.num_iterations);
            });
        });

        // Resume timing for benchmark framework
        state.ResumeTiming();

        // Calculate statistics
        if (!latencies.empty()) {
            auto stats = benchmark_utils::calculate_stats(latencies);

            // Report metrics to benchmark
            state.SetItemsProcessed(_config.num_iterations);
            state.SetBytesProcessed(_config.num_iterations * payload_size);
            state.counters["min_latency"] = stats.min;
            state.counters["max_latency"] = stats.max;
            state.counters["p50_latency"] = stats.p50;
            state.counters["p90_latency"] = stats.p90;
            state.counters["p99_latency"] = stats.p99;
            state.counters["avg_latency"] = stats.avg;

            // Save results to file
            if (!_config.output_file.empty()) {
                benchmark_utils::save_results(
                    _config.output_file,
                    "RDMA",
                    payload_size,
                    stats,
                    _config.append_output);

                // Only append for subsequent writes
                _config.append_output = true;
            }
        }
    }
}

seastar::future<> RDMABenchmark::runServer() {
    // Start the server
    return _server->start().then([] {
        if (seastar::engine().cpu_id() == 0) {
            std::cout << "Server started successfully" << std::endl;
        }
        return seastar::make_ready_future<>();
    });
}

seastar::future<std::vector<uint64_t>> RDMABenchmark::runClient(
    size_t payload_size, uint32_t iterations) {

    if (seastar::engine().cpu_id() == 0) {
        std::cout << "Starting RDMA benchmark with payload size: "
                  << payload_size << " bytes, iterations: " << iterations << std::endl;
    }

    return seastar::do_with(
        std::vector<uint64_t>{},
        std::vector<char>(payload_size, 'x'),
        std::vector<char>(payload_size),
        [this, iterations](auto& latencies, auto& write_buffer, auto& read_buffer) {
            // Reserve space for latencies
            latencies.reserve(iterations);

            // Connect to server
            return _client->connect(_config.server_addr, _config.rpc_port).then([&, this] {
                // Get remote memory info
                return _client->getRemoteMemoryInfo().then([&, this](bool success) {
                    if (!success) {
                        return seastar::make_exception_future<std::vector<uint64_t>>(
                            std::runtime_error("Failed to get remote memory info"));
                    }

                    // Create UCXX endpoint
                    return _client->createUCXXEndpoint().then([&, iterations, this](bool success) {
                        if (!success) {
                            return seastar::make_exception_future<std::vector<uint64_t>>(
                                std::runtime_error("Failed to create UCXX endpoint"));
                        }

                        if (seastar::engine().cpu_id() == 0 && _config.verbose) {
                            std::cout << "RDMA setup complete, starting benchmark..." << std::endl;
                        }

                        // Create semaphore to limit concurrency
                        return seastar::do_with(
                            seastar::semaphore(_config.concurrency),
                            [&, iterations, this](auto& limit) {
                                // Create a vector of futures
                                std::vector<seastar::future<>> futures;
                                futures.reserve(iterations);

                                // Launch all requests
                                for (uint32_t i = 0; i < iterations; i++) {
                                    auto fut = seastar::with_semaphore(limit, 1, [&, this]() {
                                        auto start = std::chrono::high_resolution_clock::now();

                                        // Perform RDMA write followed by read
                                        return _client->rdmaWrite(write_buffer.data(), write_buffer.size())
                                            .then([&, this](bool success) {
                                                if (!success) {
                                                    return seastar::make_exception_future<>(
                                                        std::runtime_error("RDMA write failed"));
                                                }

                                                // Read back the data to verify
                                                return _client->rdmaRead(read_buffer.data(), read_buffer.size())
                                                    .then([&latencies, start](bool success) {
                                                        auto end = std::chrono::high_resolution_clock::now();
                                                        auto latency = std::chrono::duration_cast<
                                                            std::chrono::microseconds>(end - start).count();
                                                        latencies.push_back(latency);
                                                    });
                                            });
                                    }).handle_exception([](std::exception_ptr ep) {
                                        try {
                                            std::rethrow_exception(ep);
                                        } catch (const std::exception& e) {
                                            std::cerr << "RDMA operation failed: " << e.what() << std::endl;
                                        }
                                    });

                                    futures.push_back(std::move(fut));
                                }

                                // Wait for all futures to complete
                                return seastar::when_all(futures.begin(), futures.end())
                                    .discard_result()
                                    .then([&latencies] {
                                        return std::move(latencies);
                                    });
                            });
                    });
                });
            }).finally([this] {
                return _client->stop();
            });
        });
}

seastar::future<> RDMABenchmark::stop() {
    return _server->stop();
}

// Benchmark utilities implementation
namespace benchmark_utils {

LatencyStats calculate_stats(const std::vector<uint64_t>& latencies) {
    if (latencies.empty()) {
        return {0, 0, 0, 0, 0, 0.0, 0};
    }

    // Make a copy for sorting
    std::vector<uint64_t> sorted_latencies = latencies;
    std::sort(sorted_latencies.begin(), sorted_latencies.end());

    size_t count = sorted_latencies.size();

    // Calculate percentiles
    LatencyStats stats;
    stats.min = sorted_latencies.front();
    stats.max = sorted_latencies.back();
    stats.p50 = sorted_latencies[count * 50 / 100];
    stats.p90 = sorted_latencies[count * 90 / 100];
    stats.p99 = sorted_latencies[count * 99 / 100];
    stats.count = count;

    // Calculate average
    uint64_t sum = std::accumulate(sorted_latencies.begin(), sorted_latencies.end(), 0ULL);
    stats.avg = static_cast<double>(sum) / count;

    return stats;
}

void save_results(const std::string& filename,
                 const std::string& benchmark_name,
                 size_t payload_size,
                 const LatencyStats& stats,
                 bool append) {
    std::ofstream file;
    if (append) {
        file.open(filename, std::ios::app);
    } else {
        file.open(filename);
        // Write header if creating a new file
        file << "Benchmark,Payload Size,Min Latency,P50 Latency,P90 Latency,"
             << "P99 Latency,Max Latency,Avg Latency,Iterations\n";
    }

    if (!file.is_open()) {
        std::cerr << "Failed to open output file: " << filename << std::endl;
        return;
    }

    // Write results
    file << benchmark_name << ","
         << payload_size << ","
         << stats.min << " us,"
         << stats.p50 << " us,"
         << stats.p90 << " us,"
         << stats.p99 << " us,"
         << stats.max << " us,"
         << stats.avg << " us,"
         << stats.count << "\n";

    file.close();
}

} // namespace benchmark_utils

} // namespace demo
