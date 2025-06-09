#include <boost/program_options.hpp>
#include <cstdint>
#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/rpc/rpc.hh>
#include <fmt/core.h>
#include <seastar/core/sstring.hh>
#include <fmt/base.h>

#include "rpc/commom.hpp"
#include "rpc/coroutine_rdma_manager.hpp"
#include "rpc/simple_rpc.hpp"


using namespace seastar;
namespace bpo = boost::program_options;

static State server_state;

RdmaTask<void> connect_and_send(CoroutineRdmaManager& manager) {
    co_await manager.connect("127.0.0.1", 12346);
    fmt::print("Client: RDMA connection established!\n");

    std::vector<int> data = {1, 2, 3, 4, 5};
    RdmaOpResult result = co_await manager.tag_send(
        data.data(),
        data.size() * sizeof(int),
        42
    );

    if (result.success()) {
        fmt::print("Client: Successfully sent RDMA data!\n");
    } else {
        fmt::print("Client: Failed to send RDMA data\n");
    }
    co_return;
}

RdmaTask<void> connect_and_recv(CoroutineRdmaManager& manager) {
    fmt::print("Server: Waiting for RDMA connection...\n");
    co_await manager.wait_for_connection();
    fmt::print("Server: RDMA connection established!\n");

    std::vector<int> buffer(5, 0);
    RdmaOpResult result = co_await manager.tag_recv(
        buffer.data(),
        buffer.size() * sizeof(int),
        42
    );

    if (result.success()) {
        fmt::print("Server: Successfully received RDMA data!\n");
        fmt::print("Server: Received data: ");
        for (const auto& val : buffer) {
            fmt::print("{} ", val);
        }
        fmt::print("\n");
    } else {
        fmt::print("Server: Failed to receive RDMA data\n");
    }
    co_return;
}

future<> run_client(const std::string& server_addr, uint16_t rpc_port, uint16_t rdma_port) {
    auto manager = std::make_shared<btsp::CoroutineRdmaManager>();
    fmt::print("Client: Connecting to rpc server {}:{}\n", server_addr, rpc_port);
    fmt::print("Client: Connecting to rdma server {}:{}\n", server_addr, rdma_port);
    // Initialize RDMA manager as client
    if (!manager->initialize(false, 0)) {
        std::cerr << "Failed to initialize RDMA manager" << std::endl;
        co_return;
    }

    try {
        fmt::print("Client: Sending RPC metadata...\n");
        
        auto&& shands_rpc = rpc_context::get_protocol();
        auto send_spec = shands_rpc.make_client<future<> (int32_t)>(
            to_underlying(msg_type::PREPARE_TENSOR_TRANSFER));

        rpc::client_options co;
        auto& mc = rpc_context::get_compressor();
        co.compressor_factory = &mc;

        auto client = std::make_unique<rpc::protocol<serializer>::client>(
            shands_rpc, co, ipv4_addr{server_addr, rpc_port});

        fmt::print("Client: Setting up RPC client\n");

        fmt::print("Client: Sending tensor preparation RPC\n");
        co_await send_spec(*client, 128);
        fmt::print("Client: RPC response sent\n");
        co_await client->stop();
        
        fmt::print("Client: RPC metadata sent successfully\n");

        co_await sleep(std::chrono::milliseconds(500));

        fmt::print("Client: Starting RDMA operations...\n");
        auto task = connect_and_send(*manager);

        while (!task.done()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        task.get();

        fmt::print("Client: All operations completed successfully\n");

    } catch (const std::exception& e) {
        fmt::print("Client error: {}\n", e.what());
    }

    // Cleanup
    manager->shutdown();
    fmt::print("Client: RPC client stopped\n");
    co_return;
}

future<> run_server(uint16_t rpc_port, uint16_t rdma_port) {
    auto manager = std::make_shared<btsp::CoroutineRdmaManager>();

    // RDMA
    if (!manager->initialize(true, rdma_port)) {
        std::cerr << "Failed to initialize RDMA manager" << std::endl;
        co_return;
    }
    server_state.set_rdma_manager(manager);

    auto&& shands_rpc = rpc_context::get_protocol();

    shands_rpc.register_handler(to_underlying(msg_type::PREPARE_TENSOR_TRANSFER),
        [](int32_t spec) -> future<> {
            fmt::print("Server: preparing tensor with shape: {}\n", spec);
            return make_ready_future<>();
        });
    
    rpc::resource_limits limits;
    limits.bloat_factor = 1;
    limits.basic_request_size = 128;
    limits.max_memory = 100'000'000;  // 100MB limit

    auto server = std::make_unique<rpc::protocol<serializer>::server>(
        shands_rpc, ipv4_addr{rpc_port}, limits);

    fmt::print("Server: RPC server setup complete\n");

    // Start RDMA task in background without blocking RPC server
    auto rdma_task = connect_and_recv(*manager);

    // Keep server running and check RDMA task periodically
    while (!rdma_task.done()) {
        // Let Seastar event loop process RPC requests
        co_await sleep(std::chrono::milliseconds(100));
    }

    try {
        rdma_task.get();
        fmt::print("Server: RDMA operations completed successfully\n");
    } catch (const std::exception& e) {
        fmt::print("Server: RDMA error: {}\n", e.what());
    }

    co_return;
}

int main(int ac, char** av) {
    app_template app;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(10000), "RPC server port")
        ("server", bpo::value<sstring>(), "Server address");
    static logger slp_logger("sleep rpc");
    rpc_context::get_protocol().set_logger(&slp_logger);
    static uint16_t rpc_port = 10000;
    static uint16_t rdma_port = 12346;
    app.run_deprecated(ac, av, [&app] {
        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();
        if (config.count("server")) {
            return run_client(config["server"].as<sstring>(), port, rdma_port);
        } else {
            return run_server(port, rdma_port);
        }
    });
    return 0;
}
