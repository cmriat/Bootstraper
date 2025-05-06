/*
 * RPC Benchmark for Seastar based on official demo
 *
 * This benchmark measures the latency of RPC calls with different payload sizes
 * and reports percentile statistics (p50, p90, p99, p9999, p100).
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

#include "rpc/rpc_serializer.hpp"
#include "rpc/rpc_benchmark_utils.hpp"
#include "rpc/rpc_client.hpp"
#include "rpc/rpc_server.hpp"

using namespace seastar;
using namespace std::chrono_literals;
using namespace rpc_benchmark;

static seastar::logger rpc_logger("rpc_benchmark");

// Global server instance
static rpc_server server(10000);

// Server function
future<> run_server(uint16_t port) {
    fmt::print("Starting RPC benchmark server on port {}\n", port);

    // Update server port if needed
    if (port != 10000) {
        server = rpc_server(port);
    }

    // Start the server
    return server.start().then([] {
        fmt::print("Listening for connections...\n");
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
    }).finally([] {
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
}



// Client function
future<> run_client(sstring server_addr, uint16_t port, uint32_t payload_size, uint32_t num_requests, uint32_t concurrency) {
    fmt::print("Starting RPC benchmark client, connecting to {}:{}\n", server_addr, port);
    fmt::print("Payload size: {} bytes, Requests: {}, Concurrency: {}\n",
              payload_size, num_requests, concurrency);

    return do_with(
        rpc_client{},
        std::vector<uint64_t>{},
        sstring(uninitialized_string(payload_size)),
        [=](auto& client, auto& latencies, auto& test_payload) {
            // Fill payload with data
            std::fill(test_payload.begin(), test_payload.end(), 'x');

            // Reserve space for latencies
            latencies.reserve(num_requests);

            fmt::print("Attempting to connect to {}:{}\n", server_addr, port);

            // Connect to server
            return client.connect(server_addr, port).then([&] {
                fmt::print("Client created successfully\n");
                fmt::print("Test setup complete\n");
                fmt::print("Connected to server, starting benchmark...\n");

                // Create semaphore to limit concurrency
                return do_with(semaphore(concurrency), [&](auto& limit) {
                    // Create a vector of futures
                    std::vector<future<>> futures;
                    futures.reserve(num_requests);

                    // Launch all requests
                    for (uint32_t i = 0; i < num_requests; i++) {
                        auto fut = with_semaphore(limit, 1, [&client, &test_payload, &latencies]() {
                            auto start = std::chrono::high_resolution_clock::now();

                            return client.echo(test_payload)
                                .then([&latencies, start](sstring) {
                                    auto end = std::chrono::high_resolution_clock::now();
                                    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
                                        end - start).count();
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
                    return when_all(futures.begin(), futures.end()).discard_result()
                        .then([&latencies, payload_size]() {
                            if (latencies.empty()) {
                                fmt::print("No successful requests completed\n");
                                return make_ready_future<>();
                            }

                            // Calculate and print results
                            auto percentiles = calculate_percentiles(latencies);
                            print_benchmark_results(payload_size, percentiles);

                            return make_ready_future<>();
                        });
                });
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
        ("port", bpo::value<uint16_t>()->default_value(10000), "RPC server port")
        ("server", bpo::value<std::string>(), "Server address (if not specified, run in server mode)")
        ("payload-size,payload", bpo::value<uint32_t>()->default_value(60), "Payload size in bytes")
        ("requests", bpo::value<uint32_t>()->default_value(100000), "Number of requests to send")
        ("concurrency", bpo::value<uint32_t>()->default_value(100), "Number of concurrent requests");

    return app.run(ac, av, [&app] {
        auto& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();

        // Handle both --payload-size and --payload
        uint32_t payload_size;
        if (config.count("payload-size")) {
            payload_size = config["payload-size"].as<uint32_t>();
        } else if (config.count("payload")) {
            payload_size = config["payload"].as<uint32_t>();
        } else {
            payload_size = 60; // Default value
        }

        uint32_t num_requests = config["requests"].as<uint32_t>();
        uint32_t concurrency = config["concurrency"].as<uint32_t>();

        if (config.count("server")) {
            // Client mode
            return run_client(config["server"].as<std::string>(), port, payload_size, num_requests, concurrency);
        } else {
            // Server mode
            return run_server(port);
        }
    });
}
