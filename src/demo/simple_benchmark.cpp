/*
 * Simple RDMA/RPC Benchmark
 *
 * This file contains a simplified benchmark for RDMA and RPC communication.
 */
#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <numeric>
#include <chrono>
#include <fmt/core.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/util/log.hh>

#include "demo/rdma_server.hpp"
#include "demo/rdma_client.hpp"

using namespace seastar;
using namespace std::chrono_literals;

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

// Latency statistics
struct LatencyStats {
    uint64_t min;
    uint64_t max;
    uint64_t p50;
    uint64_t p90;
    uint64_t p99;
    double avg;
    size_t count;
};

// Calculate statistics from latencies
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

// Save benchmark results to file
void save_results(const std::string& filename, 
                 const std::string& benchmark_name,
                 size_t payload_size, 
                 const LatencyStats& stats,
                 bool append = false) {
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

// Print benchmark results to console
void print_results(const std::string& benchmark_name, 
                  size_t payload_size,
                  const LatencyStats& stats) {
    fmt::print("\n{} benchmark with {} byte payload\n", benchmark_name, payload_size);
    fmt::print("Completed {} iterations\n", stats.count);
    fmt::print("Latency (microseconds):\n");
    fmt::print("min: {} us\n", stats.min);
    fmt::print("p50: {} us\n", stats.p50);
    fmt::print("p90: {} us\n", stats.p90);
    fmt::print("p99: {} us\n", stats.p99);
    fmt::print("max: {} us\n", stats.max);
    fmt::print("avg: {:.2f} us\n", stats.avg);
}

// Run RPC benchmark
future<> run_rpc_benchmark(BenchmarkConfig config) {
    fmt::print("Running RPC benchmark with payload size: {} bytes\n", config.payload_size);
    
    return do_with(
        RdmaServer(config.rpc_port, config.ucxx_port),
        RdmaClient{},
        std::vector<uint64_t>{},
        [config](auto& server, auto& client, auto& latencies) {
            // Initialize server
            if (!server.initialize()) {
                return make_exception_future<>(std::runtime_error("Failed to initialize server"));
            }
            
            // Allocate memory for RDMA
            if (!server.allocateMemory(config.memory_size)) {
                return make_exception_future<>(std::runtime_error("Failed to allocate memory"));
            }
            
            // Initialize client
            if (!client.initialize()) {
                return make_exception_future<>(std::runtime_error("Failed to initialize client"));
            }
            
            // Reserve space for latencies
            latencies.reserve(config.num_iterations);
            
            // Start the server
            return server.start().then([&client, &latencies, config] {
                fmt::print("Server started successfully\n");
                
                // Connect to server
                return client.connect(config.server_addr, config.rpc_port).then([&client, &latencies, config] {
                    fmt::print("Connected to server, starting benchmark...\n");
                    
                    // Create test payload
                    sstring test_payload(config.payload_size, 'x');
                    
                    // Create semaphore to limit concurrency
                    return do_with(
                        semaphore(config.concurrency),
                        [&client, &latencies, config, test_payload](auto& limit) {
                            std::vector<future<>> futures;
                            futures.reserve(config.num_iterations);
                            
                            // Launch all requests
                            for (uint32_t i = 0; i < config.num_iterations; i++) {
                                auto fut = with_semaphore(limit, 1, [&client, &latencies, test_payload]() {
                                    auto start = std::chrono::high_resolution_clock::now();
                                    
                                    return client.echo(test_payload)
                                        .then([&latencies, start](sstring) {
                                            auto end = std::chrono::high_resolution_clock::now();
                                            auto latency = std::chrono::duration_cast<
                                                std::chrono::microseconds>(end - start).count();
                                            latencies.push_back(latency);
                                        })
                                        .handle_exception([](std::exception_ptr ep) {
                                            try {
                                                std::rethrow_exception(ep);
                                            } catch (const std::exception& e) {
                                                fmt::print("Request failed: {}\n", e.what());
                                            }
                                        });
                                });
                                futures.push_back(std::move(fut));
                            }
                            
                            // Wait for all futures to complete
                            return when_all(futures.begin(), futures.end()).discard_result();
                        });
                });
            }).then([&latencies, config] {
                // Calculate statistics
                if (latencies.empty()) {
                    fmt::print("No successful requests completed\n");
                    return make_ready_future<>();
                }
                
                auto stats = calculate_stats(latencies);
                
                // Print results
                print_results("RPC", config.payload_size, stats);
                
                // Save results to file
                save_results(config.output_file, "RPC", config.payload_size, stats, config.append_output);
                
                return make_ready_future<>();
            }).finally([&server, &client] {
                return client.stop().then([&server] {
                    return server.stop();
                });
            });
        });
}

// Run RDMA benchmark
future<> run_rdma_benchmark(BenchmarkConfig config) {
    fmt::print("Running RDMA benchmark with payload size: {} bytes\n", config.payload_size);
    
    return do_with(
        RdmaServer(config.rpc_port, config.ucxx_port),
        RdmaClient{},
        std::vector<uint64_t>{},
        [config](auto& server, auto& client, auto& latencies) {
            // Initialize server
            if (!server.initialize()) {
                return make_exception_future<>(std::runtime_error("Failed to initialize server"));
            }
            
            // Allocate memory for RDMA
            if (!server.allocateMemory(config.memory_size)) {
                return make_exception_future<>(std::runtime_error("Failed to allocate memory"));
            }
            
            // Initialize client
            if (!client.initialize()) {
                return make_exception_future<>(std::runtime_error("Failed to initialize client"));
            }
            
            // Reserve space for latencies
            latencies.reserve(config.num_iterations);
            
            // Start the server
            return server.start().then([&client, &latencies, config] {
                fmt::print("Server started successfully\n");
                
                // Connect to server
                return client.connect(config.server_addr, config.rpc_port).then([&client, &latencies, config] {
                    // Get remote memory info
                    return client.getRemoteMemoryInfo().then([&client, &latencies, config](bool success) {
                        if (!success) {
                            return make_exception_future<>(
                                std::runtime_error("Failed to get remote memory info"));
                        }
                        
                        // Create UCXX endpoint
                        return client.createUCXXEndpoint().then([&client, &latencies, config](bool success) {
                            if (!success) {
                                return make_exception_future<>(
                                    std::runtime_error("Failed to create UCXX endpoint"));
                            }
                            
                            fmt::print("RDMA setup complete, starting benchmark...\n");
                            
                            // Create test data
                            std::vector<char> write_buffer(config.payload_size, 'x');
                            std::vector<char> read_buffer(config.payload_size);
                            
                            // Create semaphore to limit concurrency
                            return do_with(
                                semaphore(config.concurrency),
                                [&client, &latencies, config, &write_buffer, &read_buffer](auto& limit) {
                                    std::vector<future<>> futures;
                                    futures.reserve(config.num_iterations);
                                    
                                    // Launch all requests
                                    for (uint32_t i = 0; i < config.num_iterations; i++) {
                                        auto fut = with_semaphore(limit, 1, [&client, &latencies, &write_buffer, &read_buffer]() {
                                            auto start = std::chrono::high_resolution_clock::now();
                                            
                                            // Perform RDMA write followed by read
                                            return client.rdmaWrite(write_buffer.data(), write_buffer.size())
                                                .then([&client, &read_buffer, &latencies, start](bool success) {
                                                    if (!success) {
                                                        return make_exception_future<>(
                                                            std::runtime_error("RDMA write failed"));
                                                    }
                                                    
                                                    // Read back the data to verify
                                                    return client.rdmaRead(read_buffer.data(), read_buffer.size())
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
                                                fmt::print("RDMA operation failed: {}\n", e.what());
                                            }
                                        });
                                        
                                        futures.push_back(std::move(fut));
                                    }
                                    
                                    // Wait for all futures to complete
                                    return when_all(futures.begin(), futures.end()).discard_result();
                                });
                        });
                    });
                });
            }).then([&latencies, config] {
                // Calculate statistics
                if (latencies.empty()) {
                    fmt::print("No successful requests completed\n");
                    return make_ready_future<>();
                }
                
                auto stats = calculate_stats(latencies);
                
                // Print results
                print_results("RDMA", config.payload_size, stats);
                
                // Save results to file
                save_results(config.output_file, "RDMA", config.payload_size, stats, config.append_output);
                
                return make_ready_future<>();
            }).finally([&server, &client] {
                return client.stop().then([&server] {
                    return server.stop();
                });
            });
        });
}

} // namespace demo

int main(int ac, char** av) {
    namespace bpo = boost::program_options;
    app_template app;
    
    app.add_options()
        ("rpc-port", bpo::value<uint16_t>()->default_value(10000), "RPC server port")
        ("ucxx-port", bpo::value<uint16_t>()->default_value(10001), "UCXX server port")
        ("server-addr", bpo::value<std::string>()->default_value("127.0.0.1"), "Server address")
        ("memory-size", bpo::value<size_t>()->default_value(1024*1024), "Memory size for RDMA operations")
        ("payload-size", bpo::value<uint32_t>()->default_value(1024), "Payload size in bytes")
        ("iterations", bpo::value<uint32_t>()->default_value(1000), "Number of iterations")
        ("concurrency", bpo::value<uint32_t>()->default_value(32), "Concurrency level")
        ("mode", bpo::value<std::string>()->default_value("both"), "Benchmark mode: rpc, rdma, or both")
        ("output", bpo::value<std::string>()->default_value("logs/benchmark.txt"), "Output file");
    
    return app.run(ac, av, [&app] {
        auto& config = app.configuration();
        
        // Create benchmark configuration
        demo::BenchmarkConfig bench_config;
        bench_config.server_addr = config["server-addr"].as<std::string>();
        bench_config.rpc_port = config["rpc-port"].as<uint16_t>();
        bench_config.ucxx_port = config["ucxx-port"].as<uint16_t>();
        bench_config.memory_size = config["memory-size"].as<size_t>();
        bench_config.payload_size = config["payload-size"].as<uint32_t>();
        bench_config.num_iterations = config["iterations"].as<uint32_t>();
        bench_config.concurrency = config["concurrency"].as<uint32_t>();
        bench_config.output_file = config["output"].as<std::string>();
        
        // Create logs directory
        system("mkdir -p logs");
        
        // Get benchmark mode
        std::string mode = config["mode"].as<std::string>();
        
        // Run benchmarks
        if (mode == "rpc" || mode == "both") {
            return demo::run_rpc_benchmark(bench_config).then([&bench_config, mode] {
                if (mode == "both") {
                    bench_config.append_output = true;
                    return demo::run_rdma_benchmark(bench_config);
                }
                return make_ready_future<>();
            });
        } else if (mode == "rdma") {
            return demo::run_rdma_benchmark(bench_config);
        } else {
            fmt::print("Invalid mode: {}. Valid modes are: rpc, rdma, both\n", mode);
            return make_ready_future<>();
        }
    });
}
