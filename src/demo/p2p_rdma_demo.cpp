/*
 * P2P RDMA Communication Demo
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
#include <seastar/core/semaphore.hh>
#include <seastar/util/log.hh>

#include "demo/p2p_manager.hpp"

using namespace seastar;
using namespace std::chrono_literals;
using namespace demo;

// Server function
future<> run_server(const std::string& node_id, uint16_t rpc_port, uint16_t ucxx_port) {
    fmt::print("Starting P2P RDMA server '{}' on ports {} (RPC) and {} (UCXX)\n", 
              node_id, rpc_port, ucxx_port);
    
    // Get P2P manager instance
    auto p2p_manager = P2PManager::getInstance(node_id, rpc_port, ucxx_port);
    
    // Initialize P2P manager
    if (!p2p_manager->initialize()) {
        return make_exception_future<>(std::runtime_error("Failed to initialize P2P manager"));
    }
    
    // Start P2P manager
    return p2p_manager->start().then([] {
        fmt::print("Server started successfully\n");
        fmt::print("Press Ctrl+C to stop the server\n");
        
        struct sigaction sa;
        sa.sa_handler = [](int) {
            static bool interrupted = false;
            if (!interrupted) {
                interrupted = true;
                fmt::print("\nStopping server...\n");
            } else {
                _exit(1);
            }
        };
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = 0;
        sigaction(SIGINT, &sa, nullptr);
        
        // Keep the server running
        return sleep(std::chrono::hours(24));
    }).finally([] {
        return P2PManager::getInstance()->stop();
    }).handle_exception([](std::exception_ptr ep) {
        try {
            std::rethrow_exception(ep);
        } catch (const std::exception& e) {
            fmt::print("Server error: {}\n", e.what());
        }
        return make_ready_future<>();
    });
}

// Client function
future<> run_client(const std::string& node_id, 
                   const std::string& server_id,
                   const std::string& server_addr, 
                   uint16_t server_port,
                   uint32_t payload_size, 
                   uint32_t num_requests) {
    fmt::print("Starting P2P RDMA client '{}', connecting to {}:{}\n", 
              node_id, server_addr, server_port);
    
    // Get P2P manager instance
    auto p2p_manager = P2PManager::getInstance(node_id, 0, 0);
    
    // Initialize P2P manager
    if (!p2p_manager->initialize()) {
        return make_exception_future<>(std::runtime_error("Failed to initialize P2P manager"));
    }
    
    // Start P2P manager
    return p2p_manager->start().then([=] {
        // Connect to server
        return p2p_manager->connect(server_id, server_addr, server_port);
    }).then([=](bool connected) {
        if (!connected) {
            return make_exception_future<>(std::runtime_error("Failed to connect to server"));
        }
        
        fmt::print("Connected to server '{}'\n", server_id);
        
        // Prepare test data
        std::vector<char> write_data(payload_size, 'A');
        std::vector<char> read_data(payload_size);
        std::vector<uint64_t> latencies;
        latencies.reserve(num_requests);
        
        // Run benchmark
        return do_with(std::move(write_data), std::move(read_data), std::move(latencies),
                      [=, &p2p_manager](auto& write_data, auto& read_data, auto& latencies) {
            return do_with(semaphore(1), [=, &p2p_manager, &write_data, &read_data, &latencies](auto& limit) {
                std::vector<future<>> futures;
                futures.reserve(num_requests);
                
                for (uint32_t i = 0; i < num_requests; i++) {
                    auto fut = with_semaphore(limit, 1, [=, &p2p_manager, &write_data, &read_data, &latencies]() {
                        auto start = std::chrono::high_resolution_clock::now();
                        
                        // Write data to server
                        return p2p_manager->sendData(server_id, write_data.data(), write_data.size())
                            .then([=, &p2p_manager, &read_data](bool success) {
                                if (!success) {
                                    return make_exception_future<bool>(
                                        std::runtime_error("Failed to send data"));
                                }
                                
                                // Read data back from server
                                return p2p_manager->receiveData(server_id, read_data.data(), read_data.size());
                            }).then([&latencies, start](bool success) {
                                auto end = std::chrono::high_resolution_clock::now();
                                auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
                                    end - start).count();
                                latencies.push_back(latency);
                                
                                return make_ready_future<>();
                            });
                    }).handle_exception([i](std::exception_ptr ep) {
                        try {
                            std::rethrow_exception(ep);
                        } catch (const std::exception& e) {
                            fmt::print("Request {} failed: {}\n", i, e.what());
                        }
                    });
                    
                    futures.push_back(std::move(fut));
                }
                
                // Wait for all futures to complete
                return when_all(futures.begin(), futures.end()).discard_result();
            }).then([&latencies] {
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
                fmt::print("\nP2P RDMA Results:\n");
                fmt::print("Completed {} requests\n", count);
                fmt::print("Latency (microseconds):\n");
                fmt::print("min: {} us\n", min);
                fmt::print("p50: {} us\n", p50);
                fmt::print("p90: {} us\n", p90);
                fmt::print("p99: {} us\n", p99);
                fmt::print("max: {} us\n", max);
                fmt::print("avg: {:.2f} us\n", avg);
                
                return make_ready_future<>();
            });
        });
    }).finally([] {
        return P2PManager::getInstance()->stop();
    }).handle_exception([](std::exception_ptr ep) {
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
        ("node-id", bpo::value<std::string>()->default_value("node1"), "Node identifier")
        ("rpc-port", bpo::value<uint16_t>()->default_value(10000), "RPC server port")
        ("ucxx-port", bpo::value<uint16_t>()->default_value(10001), "UCXX server port")
        ("server", bpo::value<std::string>(), "Server address (if not specified, run in server mode)")
        ("server-id", bpo::value<std::string>()->default_value("server1"), "Server identifier (for client mode)")
        ("payload-size", bpo::value<uint32_t>()->default_value(1024), "Payload size in bytes")
        ("requests", bpo::value<uint32_t>()->default_value(1000), "Number of requests to send");
    
    return app.run(ac, av, [&app] {
        auto& config = app.configuration();
        std::string node_id = config["node-id"].as<std::string>();
        uint16_t rpc_port = config["rpc-port"].as<uint16_t>();
        uint16_t ucxx_port = config["ucxx-port"].as<uint16_t>();
        uint32_t payload_size = config["payload-size"].as<uint32_t>();
        uint32_t num_requests = config["requests"].as<uint32_t>();
        
        if (config.count("server")) {
            // Client mode
            std::string server_id = config["server-id"].as<std::string>();
            std::string server_addr = config["server"].as<std::string>();
            return run_client(node_id, server_id, server_addr, rpc_port, payload_size, num_requests);
        } else {
            // Server mode
            return run_server(node_id, rpc_port, ucxx_port);
        }
    });
}