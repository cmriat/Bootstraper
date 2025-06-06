#include <boost/program_options.hpp>
#include <cstdint>
#include <iostream>
#include <memory>
#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <fmt/core.h>
#include <vector>

#include "rpc/simple_rpc.hpp"
#include "fmt/base.h"
#include "rpc/coroutine_rdma_manager.hpp"

using namespace seastar;
namespace bpo = boost::program_options;

future<> run_server(uint16_t port) {
    // Use shared_ptr for proper lifetime management
    auto manager = std::make_shared<btsp::CoroutineRdmaManager>();

    try {
        if (!manager->initialize(true, 12346)) {
            std::cerr << "Failed to initialize RDMA manager" << std::endl;
            co_return;
        }

        auto&& shands_rpc = rpc_context::get_protocol();

        static auto keep_alive = std::make_shared<promise<>>();

        shands_rpc.register_handler(to_underlying(msg_type::PREPARE_TENSOR_TRANSFER),
            [](int32_t spec) -> future<> {
                fmt::print("Server: preparing tensor with shape: {}\n", spec);
                return make_ready_future<>();
            });

        fmt::print("RPC handlers registered.\n");

        // Set resource limits for RPC server to prevent memory issues
        rpc::resource_limits limits;
        limits.bloat_factor = 1;
        limits.basic_request_size = 128;
        limits.max_memory = 100'000'000;  // 100MB limit

        static std::unique_ptr<rpc::protocol<serializer>::server> server =
            std::make_unique<rpc::protocol<serializer>::server>(shands_rpc,
                                                                ipv4_addr{port},
                                                                limits);

        fmt::print("RPC server listening on port {}\n", port);

        // Wait for RDMA connection
        co_await manager->wait_for_connection();
        std::vector<int> buffer(5, 0);
        fmt::print("Receiver: Connected!\n");

        fmt::print("Server: Waiting for data (tag=42)...\n");
        btsp::RdmaOpResult result = co_await manager->tag_recv(
            buffer.data(),
            buffer.size() * sizeof(int),
            42
        );
        fmt::print("Server: Receive result: {}\n", result.success());

        if (result.success()) {
            fmt::print("Server: Successfully received data!\n");
            for (const auto& val : buffer) {
                fmt::print("{} ", val);
            }
            fmt::print("\n");
        } else {
            fmt::print("Server: Failed to receive data\n");
        }

        // Proper cleanup: shutdown RDMA manager before waiting indefinitely
        manager->shutdown();
        co_return co_await keep_alive->get_future();

    } catch (const std::bad_alloc& e) {
        std::cerr << "Server: Memory allocation failed: " << e.what() << std::endl;
        if (manager) {
            manager->shutdown();
        }
        throw;
    } catch (const std::exception& e) {
        std::cerr << "Server: Exception occurred: " << e.what() << std::endl;
        if (manager) {
            manager->shutdown();
        }
        throw;
    }
}

future<> run_client(const std::string& server_addr, uint16_t port) {
    // Use shared_ptr to ensure proper lifetime management
    auto manager = std::make_shared<btsp::CoroutineRdmaManager>();

    try {
        if (!manager->initialize(false, 0)) {
            std::cerr << "Failed to initialize RDMA manager" << std::endl;
            co_return;
        }

        auto&& shands_rpc = rpc_context::get_protocol();
        auto send_spec = shands_rpc.make_client<future<> (int32_t)>(
            to_underlying(msg_type::PREPARE_TENSOR_TRANSFER));

        // Set resource limits for RPC client to prevent memory issues
        rpc::client_options co;
        auto client = std::make_unique<rpc::protocol<serializer>::client>(
            shands_rpc, co, ipv4_addr{server_addr, port});

        // Send RPC message first
        co_await send_spec(*client, 128);
        fmt::print("Client: Tensor spec sent successfully\n");

        // Then establish RDMA connection
        fmt::print("Sender: Connecting to 127.0.0.1:12346...\n");
        btsp::RdmaOpResult connect_result;
        bool connect_failed = false;
        try {
            connect_result = co_await manager->connect(server_addr, 12346);
            fmt::print("Sender: Connect result: {}\n", connect_result.success());
            if (!connect_result.success()) {
                std::cerr << "Sender: Failed to connect" << std::endl;
                connect_failed = true;
            }
        } catch (const std::exception& e) {
            std::cerr << "Sender: Exception during connect: " << e.what() << std::endl;
            connect_failed = true;
        }

        if (connect_failed) {
            // Ensure proper cleanup before returning
            co_await client->stop();
            manager->shutdown();
            co_return;
        }
        

        fmt::print("Sender: Connected to server\n");

        // Small delay to ensure connection is stable
        // co_await seastar::sleep(std::chrono::seconds(1));

        // Prepare data buffer
        std::vector<int> buffer = {1, 2, 3, 4, 5};
        fmt::print("Client: Connected to server\n");

        // Send data via RDMA
        fmt::print("Client: Sending data\n");
        btsp::RdmaOpResult send_result = co_await manager->tag_send(
            buffer.data(),
            buffer.size() * sizeof(int),
            42
        );
        fmt::print("Client: Send result: {}\n", send_result.success());
        if (send_result.success()) {
            fmt::print("Client: Successfully sent data!\n");
            fmt::print("Client: Sent data: ");
            for (const auto& val : buffer) {
                fmt::print("{} ", val);
            }
            fmt::print("\n");
        } else {
            fmt::print("Client: Failed to send data\n");
        }

        // Proper cleanup sequence: RPC client first, then RDMA manager
        co_await client->stop().finally([client = std::move(client)] {
            fmt::print("Client: Disconnected from server\n");
        });

        // Shutdown RDMA manager after RPC client is stopped
        manager->shutdown();

    } catch (const std::bad_alloc& e) {
        std::cerr << "Client: Memory allocation failed: " << e.what() << std::endl;
        // Ensure cleanup even on allocation failure
        if (manager) {
            manager->shutdown();
        }
        throw;
    } catch (const std::exception& e) {
        std::cerr << "Client: Exception occurred: " << e.what() << std::endl;
        // Ensure cleanup on any exception
        if (manager) {
            manager->shutdown();
        }
        throw;
    }
}

int main(int ac, char** av) {
    app_template app;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(10000), "RPC server port")
        ("server", bpo::value<std::string>(), "Server address");

    static logger slp_logger("sleep rpc");
    rpc_context::get_protocol().set_logger(&slp_logger);


    app.run_deprecated(ac, av, [&app] {
        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();
        std::string server_addr;
        if (config.count("server")) {
            server_addr = config["server"].as<std::string>();
            return run_client(server_addr, port);
        } else {
            return run_server(port);

        }
    });
    fmt::print("Main function exiting\n");
    return 0;
}
