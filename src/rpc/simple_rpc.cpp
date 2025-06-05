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
#include "rpc/coroutine_rdma_manager.hpp"

using namespace seastar;
namespace bpo = boost::program_options;

future<> run_server(uint16_t port) {
    btsp::CoroutineRdmaManager manager;
    if (!manager.initialize(true, 12346)) {
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

    static std::unique_ptr<rpc::protocol<serializer>::server> server =
        std::make_unique<rpc::protocol<serializer>::server>(shands_rpc,
                                                            ipv4_addr{port});
    
    fmt::print("RPC server listening on port {}\n", port);
    
    co_await manager.wait_for_connection();
    std::vector<int> buffer(5, 0);
    fmt::print("Receiver: Connected!\n");

    fmt::print("Server: Waiting for data (tag=42)...\n");
    btsp::RdmaOpResult result = co_await manager.tag_recv(
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

    co_return co_await keep_alive->get_future();
}

future<> run_client(const std::string& server_addr, uint16_t port) {
    btsp::CoroutineRdmaManager manager;
    if (!manager.initialize(false, 12347)) {
        std::cerr << "Failed to initialize RDMA manager" << std::endl;
        co_return;
    }
    auto&& shands_rpc = rpc_context::get_protocol();
    auto send_spec = shands_rpc.make_client<future<> (int32_t)>(
        to_underlying(msg_type::PREPARE_TENSOR_TRANSFER));

    auto client = std::make_unique<rpc::protocol<serializer>::client>(shands_rpc, 
                                                                      ipv4_addr{server_addr, port});
    

    fmt::print("Sender: Connecting to 127.0.0.1:12346...\n");
    btsp::RdmaOpResult connect_result = co_await manager.connect(server_addr, 12346);
    if (!connect_result.success()) {
        std::cerr << "Sender: Failed to connect" << std::endl;
        co_return;
    }
    co_await seastar::sleep(std::chrono::seconds(1));
    
    std::vector<int> buffer = {1, 2, 3, 4, 5};
    fmt::print("Client: Connected to server\n");
    
    fmt::print("Client: Sending data\n");
    btsp::RdmaOpResult send_result = co_await manager.tag_send(
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

    co_await send_spec(*client, 128);
    fmt::print("Client: Tensor spec sent successfully\n");
    
    co_return co_await client->stop().finally([client = std::move(client)] { 
        fmt::print("Client: Disconnected from server\n");
    });

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
