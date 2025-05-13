/*
 * RDMA Seastar Demo
 *
 * This demo shows how to integrate Seastar with UCXX for RDMA communication.
 * It implements a server that maintains persistent connections between nodes
 * and uses UCXX endpoints for RDMA communication via RPC clients.
 */
#include <chrono>
#include <vector>
#include <fmt/core.h>
#include <signal.h>
#include <stdlib.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/util/defer.hh>
#include <seastar/rpc/rpc.hh>
#include <seastar/util/log.hh>

#include "demo/rdma_server.hpp"
#include "demo/rdma_client.hpp"

using namespace seastar;
using namespace std::chrono_literals;
using namespace demo;

static seastar::logger demo_logger("rdma_seastar_demo");

// Server function
future<> run_server(uint16_t rpc_port, uint16_t ucxx_port, size_t memory_size) {
    fmt::print("Starting RDMA RPC server on ports {} (RPC) and {} (UCXX)\n", rpc_port, ucxx_port);
    
    return do_with(RdmaServer(rpc_port, ucxx_port), [memory_size](auto& server) {
        // Initialize server
        if (!server.initialize()) {
            return make_exception_future<>(std::runtime_error("Failed to initialize server"));
        }
        
        // Allocate memory for RDMA
        if (!server.allocateMemory(memory_size)) {
            return make_exception_future<>(std::runtime_error("Failed to allocate memory"));
        }
        
        // Start the server
        return server.start().then([] {
            fmt::print("Server started successfully\n");
            fmt::print("Press Ctrl+C to stop the server\n");
            
            struct sigaction sa;
            sa.sa_handler = [](int) {
                static bool interrupted = false;
                if (!interrupted) {
                    interrupted = true;
                    fmt::print("\nReceived Ctrl+C, stopping server...\n");
                } else {
                    fmt::print("\nReceived second Ctrl+C, forcing exit...\n");
                    _exit(1);
                }
            };
            sigemptyset(&sa.sa_mask);
            sa.sa_flags = 0;
            sigaction(SIGINT, &sa, nullptr);
            
            // Keep the server running
            return sleep(std::chrono::hours(24));
        }).finally([&server] {
            fmt::print("Stopping server...\n");
            return server.stop();
        }).handle_exception([](std::exception_ptr ep) {
            try {
                std::rethrow_exception(ep);
            } catch (const std::exception& e) {
                fmt::print("Server error: {}\n", e.what());
            }
            return make_ready_future<>();
        });
    });
}

// Client function
future<> run_client(sstring server_addr, uint16_t rpc_port, uint32_t payload_size, 
                   uint32_t num_requests, bool use_rdma) {
    fmt::print("Starting RDMA RPC client, connecting to {}:{}\n", server_addr, rpc_port);
    fmt::print("Payload size: {} bytes, Requests: {}, Using RDMA: {}\n",
              payload_size, num_requests, use_rdma ? "yes" : "no");
    
    return do_with(
        RdmaClient{},
        std::vector<uint64_t>{},
        [=](auto& client, auto& latencies) {
            // Initialize client
            if (!client.initialize()) {
                return make_exception_future<>(std::runtime_error("Failed to initialize client"));
            }
            
            // Reserve space for latencies
            latencies.reserve(num_requests);
            
            fmt::print("Connecting to server at {}:{}\n", server_addr, rpc_port);
            
            // Connect to server
            return client.connect(server_addr, rpc_port).then([&] {
                fmt::print("Connected to RPC server\n");
                
                if (use_rdma) {
                    // Get remote memory info
                    return client.getRemoteMemoryInfo().then([&](bool success) {
                        if (!success) {
                            return make_exception_future<>(
                                std::runtime_error("Failed to get remote memory info"));
                        }
                        
                        // Create UCXX endpoint
                        return client.createUCXXEndpoint().then([&](bool success) {
                            if (!success) {
                                return make_exception_future<>(
                                    std::runtime_error("Failed to create UCXX endpoint"));
                            }
                            
                            fmt::print("RDMA setup complete\n");
                            return make_ready_future<>();
                        });
                    });
                } else {
                    return make_ready_future<>();
                }
            }).then([&, payload_size, num_requests, use_rdma] {
                fmt::print("Starting benchmark...\n");
                
                // Prepare test data
                std::vector<char> test_data(payload_size, 'x');
                std::vector<char> read_buffer(payload_size);
                
                // Run benchmark
                return do_with(semaphore(1), [&, num_requests, use_rdma, &test_data, &read_buffer](auto& limit) {
                    std::vector<future<>> futures;
                    futures.reserve(num_requests);
                    
                    for (uint32_t i = 0; i < num_requests; i++) {
                        auto fut = with_semaphore(limit, 1, [&, use_rdma, &test_data, &read_buffer]() {
                            auto start = std::chrono::high_resolution_clock::now();
                            
                            if (use_rdma) {
                                // Use RDMA write
                                return client.rdmaWrite(test_data.data(), test_data.size())
                                    .then([&, start](bool success) {
                                        if (!success) {
                                            return make_exception_future<>(
                                                std::runtime_error("RDMA write failed"));
                                        }
                                        
                                        // Read back the data to verify
                                        return client.rdmaRead(read_buffer.data(), read_buffer.size())
                                            .then([&latencies, start](bool success) {
                                                auto end = std::chrono::high_resolution_clock::now();
                                                auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
                                                    end - start).count();
                                                latencies.push_back(latency);
                                            });
                                    });
                            } else {
                                // Use RPC echo
                                return client.echo(seastar::sstring(test_data.data(), test_data.size()))
                                    .then([&latencies, start](seastar::sstring) {
                                        auto end = std::chrono::high_resolution_clock::now();
                                        auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
                                            end - start).count();
                                        latencies.push_back(latency);
                                    });
                            }
                        }).handle_exception([](std::exception_ptr ep) {
                            try {
                                std::rethrow_exception(ep);
                            } catch (const std::exception& e) {
                                fmt::print("Request failed: {}\n", e.what());
                            }
                        });
                        
                        futures.push_back(std::move(fut));
                    }
                    
                    // Wait for all futures to complete
                    return when_all(futures.begin(), futures.end()).discard_result();
                });
            }).then([&latencies, payload_size, use_rdma]() {
                if (latencies.empty()) {
                    fmt::print("No successful requests completed\n");
                    return make_ready_future<>();
                }
                
                // Calculate statistics
                std::sort(latencies.begin(), latencies.end());
                size_t count = latencies.size();
                
                uint64_t min = latencies.front();
                uint64_t max = latencies.back();
                uint64_t p50 = latencies[count * 50 / 100];
                uint64_t p90 = latencies[count * 90 / 100];
                uint64_t p99 = latencies[count * 99 / 100];
                
                // Calculate average
                uint64_t sum = 0;
                for (auto latency : latencies) {
                    sum += latency;
                }
                double avg = static_cast<double>(sum) / count;
                
                // Print results
                fmt::print("\n{} byte payload using {}\n", payload_size, use_rdma ? "RDMA" : "RPC");
                fmt::print("Completed {} requests\n", count);
                fmt::print("Latency (microseconds):\n");
                fmt::print("min: {} us\n", min);
                fmt::print("p50: {} us\n", p50);
                fmt::print("p90: {} us\n", p90);
                fmt::print("p99: {} us\n", p99);
                fmt::print("max: {} us\n", max);
                fmt::print("avg: {:.2f} us\n", avg);
                
                return make_ready_future<>();
            }).finally([&client] {
                return client.stop();
            });
        }
    ).handle_exception([](std::exception_ptr ep) {
        try {
            std::rethrow_exception(ep);
        } catch (const std::exception& e) {
            fmt::print("Client error: {}\n", e.what());
        }
        return make_ready_future<>();
    });
}

int main(int ac, char** av) {
    namespace bpo = boost::program_options;
    app_template app;
    
    app.add_options()
        ("rpc-port", bpo::value<uint16_t>()->default_value(10000), "RPC server port")
        ("ucxx-port", bpo::value<uint16_t>()->default_value(10001), "UCXX server port")
        ("server", bpo::value<std::string>(), "Server address (if not specified, run in server mode)")
        ("memory-size", bpo::value<size_t>()->default_value(1024*1024), "Memory size for RDMA operations")
        ("payload-size", bpo::value<uint32_t>()->default_value(1024), "Payload size in bytes")
        ("requests", bpo::value<uint32_t>()->default_value(1000), "Number of requests to send")
        ("use-rdma", bpo::value<bool>()->default_value(true), "Use RDMA for data transfer");
    
    return app.run(ac, av, [&app] {
        auto& config = app.configuration();
        uint16_t rpc_port = config["rpc-port"].as<uint16_t>();
        uint16_t ucxx_port = config["ucxx-port"].as<uint16_t>();
        size_t memory_size = config["memory-size"].as<size_t>();
        uint32_t payload_size = config["payload-size"].as<uint32_t>();
        uint32_t num_requests = config["requests"].as<uint32_t>();
        bool use_rdma = config["use-rdma"].as<bool>();
        
        if (config.count("server")) {
            // Client mode
            return run_client(config["server"].as<std::string>(), rpc_port, 
                             payload_size, num_requests, use_rdma);
        } else {
            // Server mode
            return run_server(rpc_port, ucxx_port, memory_size);
        }
    });
}
