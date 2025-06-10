#include <boost/program_options.hpp>
#include <cstdint>
#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/rpc/rpc.hh>
#include <fmt/core.h>
#include <seastar/core/sstring.hh>
#include <fmt/base.h>

#include "commom.hpp"
#include "log_utils.hpp"
#include "fast_channel_manager.hpp"
#include "simple_rpc.hpp"


using namespace seastar;
namespace bpo = boost::program_options;

static State server_state;

btsp::FCTask<void> connect_and_send(btsp::FastChannelManager& manager) {
    co_await manager.connect("127.0.0.1", 12346);
    RDMA_LOG_INFO("Connection established!");

    std::vector<int> data = {1, 2, 3, 4, 5};
    btsp::FCOpResult result = co_await manager.tag_send(
        data.data(),
        data.size() * sizeof(int),
        42
    );

    if (result.success()) {
        for (const auto& val : data) {
            RDMA_LOG_INFO("Sent data: {}", val);
        }
    } else {
        RDMA_LOG_ERROR("Failed to send RDMA data");
    }
    co_return;
}

btsp::FCTask<void> connect_and_recv(btsp::FastChannelManager& manager) {
    co_await manager.wait_for_connection();
    RDMA_LOG_INFO("Connection established!");

    std::vector<int> buffer(5, 0);
    btsp::FCOpResult result = co_await manager.tag_recv(
        buffer.data(),
        buffer.size() * sizeof(int),
        42
    );

    if (result.success()) {
        for (const auto& val : buffer) {
            RDMA_LOG_INFO("Received data: {}", val);
        }
    } else {
        RDMA_LOG_ERROR("Failed to receive RDMA data");
    }
    co_return;
}

future<> run_client(const std::string& server_addr, uint16_t rpc_port, uint16_t rdma_port) {
    auto manager = std::make_shared<btsp::FastChannelManager>();
    RPC_LOG_INFO("Connecting to RPC server {}:{}", server_addr, rpc_port);
    RDMA_LOG_INFO("Connecting to RDMA server {}:{}", server_addr, rdma_port);
    // Initialize RDMA manager as client
    if (!manager->initialize(false, 0)) {
        BTSP_LOG_ERROR("Failed to initialize RDMA manager");
        co_return;
    }

    try {
        auto&& shands_rpc = rpc_context::get_protocol();
        auto send_spec = shands_rpc.make_client<future<> (int32_t)>(
            to_underlying(msg_type::PREPARE_TENSOR_TRANSFER));

        rpc::client_options co;
        auto& mc = rpc_context::get_compressor();
        co.compressor_factory = &mc;

        auto client = std::make_unique<rpc::protocol<serializer>::client>(
            shands_rpc, co, ipv4_addr{server_addr, rpc_port});

        RPC_LOG_DEBUG(1, "Setting up RPC client");
        co_await send_spec(*client, 128);
        co_await client->stop();

        RPC_LOG_INFO("RPC metadata sent successfully");

        co_await sleep(std::chrono::milliseconds(500));

        RDMA_LOG_INFO("Starting RDMA operations...");
        auto task = connect_and_send(*manager);

        while (!task.done()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        task.get();

        BTSP_LOG_INFO("All operations completed successfully");

    } catch (const std::exception& e) {
        BTSP_LOG_ERROR("Client error: {}", e.what());
    }

    // Cleanup
    manager->shutdown();
    BTSP_LOG_INFO("Client stopped");
    co_return;
}

future<> run_server(uint16_t rpc_port, uint16_t rdma_port) {
    auto manager = std::make_shared<btsp::FastChannelManager>();

    if (!manager->initialize(true, rdma_port)) {
        BTSP_LOG_ERROR("Failed to initialize RDMA manager");
        co_return;
    }
    server_state.set_rdma_manager(manager);

    auto&& shands_rpc = rpc_context::get_protocol();

    shands_rpc.register_handler(to_underlying(msg_type::PREPARE_TENSOR_TRANSFER),
        [](int32_t spec) -> future<> {
            RPC_LOG_INFO("Preparing tensor with shape: {}", spec);
            return make_ready_future<>();
        });
    
    rpc::resource_limits limits;
    limits.bloat_factor = 1;
    limits.basic_request_size = 128;
    limits.max_memory = 100'000'000;

    auto server = std::make_unique<rpc::protocol<serializer>::server>(
        shands_rpc, ipv4_addr{rpc_port}, limits);

    RPC_LOG_INFO("RPC server setup complete on port {}", rpc_port);

    // Start RDMA task in background without blocking RPC server
    auto rdma_task = connect_and_recv(*manager);

    // Keep server running and check RDMA task periodically
    while (!rdma_task.done()) {
        // Let Seastar event loop process RPC requests
        co_await sleep(std::chrono::milliseconds(100));
    }

    try {
        rdma_task.get();
        RDMA_LOG_INFO("RDMA operations completed successfully");
    } catch (const std::exception& e) {
        RDMA_LOG_ERROR("RDMA error: {}", e.what());
    }

    co_return;
}

int main(int ac, char** av) {
    btsp::LogWrapper::init(av[0]);

    app_template app;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(10000), "RPC server port")
        ("server", bpo::value<sstring>(), "Server address")
        ("v", bpo::value<int>()->default_value(0), "Verbose logging level")
        ("log-dir", bpo::value<std::string>(), "Log directory");
    
    static logger slp_logger("sleep rpc");
    rpc_context::get_protocol().set_logger(&slp_logger);
    static uint16_t rdma_port = 12346;
    app.run(ac, av, [&app] {
        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();

        if (config.count("v")) {
            btsp::LogWrapper::set_verbose_level(config["v"].as<int>());
        }

        if (config.count("log-dir")) {
            btsp::LogWrapper::set_log_dir(config["log-dir"].as<std::string>());
        }

        if (config.count("server")) {
            BTSP_LOG_INFO("Starting client mode, connecting to {}:{}",
                         config["server"].as<sstring>(), port);
            return run_client(config["server"].as<sstring>(), port, rdma_port);
        } else {
            BTSP_LOG_INFO("Starting server mode on port {}", port);
            return run_server(port, rdma_port);
        }
    });
    return 0;
}
