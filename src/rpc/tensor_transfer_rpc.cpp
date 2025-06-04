#include <boost/program_options.hpp>
#include <cstdint>
#include <iostream>
#include <memory>
#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <fmt/core.h>

#include "rpc/rpc_common.hpp"

using namespace seastar;
namespace bpo = boost::program_options;

future<> run_server(uint16_t port) {
    fmt::print("Starting tensor transfer server on port {}\n", port);
    auto&& shands_rpc = rpc_context::get_protocol();

    static auto keep_alive = std::make_shared<promise<>>();

    // register the handler
    shands_rpc.register_handler(to_underlying(msg_type::PREPARE_TENSOR_TRANSFER), 
        [](int32_t spec) -> future<> {
            fmt::print("Server: preparing tensor with shape: {}\n", spec);
            return make_ready_future<>();
        });
    
    // register shutdown handler
    shands_rpc.register_handler(to_underlying(msg_type::GOODBYE),
        [](sstring msg) -> sstring {
            std::cout << "Server: received shutdown request: " << msg << "\n";
            // Note: In a real implementation, you'd want to properly handle shutdown
            return "Server shutting down";
        });
    
    // register sleep handler
    shands_rpc.register_handler(to_underlying(msg_type::SLEEP_MS), [](int ms) -> future<> {
        return sleep(std::chrono::milliseconds(ms)).then([ms] {
            fmt::print("Server: sleep for {}ms\n", ms);
            return make_ready_future<>();
        });
    });
    
    fmt::print("RPC handlers registered.\n");

    rpc::server_options so;
    auto& mc = rpc_context::get_compressor();
    so.compressor_factory = &mc;

    rpc::resource_limits limits;
    limits.bloat_factor = 1;
    limits.basic_request_size = 128;
    limits.max_memory = 10'000'000;

    static std::unique_ptr<rpc::protocol<serializer>::server> server =
        std::make_unique<rpc::protocol<serializer>::server>(shands_rpc,
                                                            so,
                                                            ipv4_addr{port},
                                                            limits);
    
    fmt::print("RPC server listening on port {}\n", port);
    
    return keep_alive->get_future();
}

future<> run_client(const std::string& server_addr, uint16_t port) {
    fmt::print("Starting tensor transfer client, connecting to {}: {}\n", server_addr, port);
    auto&& shands_rpc = rpc_context::get_protocol();
    auto send_spec = shands_rpc.make_client<future<> (int32_t)>(
        to_underlying(msg_type::PREPARE_TENSOR_TRANSFER));
    
    auto goodbye = shands_rpc.make_client<sstring (sstring)>(
        to_underlying(msg_type::GOODBYE));
    
    auto slp = shands_rpc.make_client<future<> (int)>(to_underlying(msg_type::SLEEP_MS));

    rpc::client_options co;
    auto& mc = rpc_context::get_compressor();
    co.compressor_factory = &mc;

    auto client = std::make_unique<rpc::protocol<serializer>::client>(shands_rpc, 
                                                                      co, 
                                                                      ipv4_addr{server_addr, port});
    
    
    co_await send_spec(*client, 128);
    fmt::print("Client: Tensor spec sent successfully\n");

    co_await goodbye(*client, "goodbye");
    fmt::print("Client: Received response to goodbye\n");

    co_await slp(*client, 1000).then([]() {
        fmt::print("Client: Sleep completed\n");
    });
    
    co_return co_await client->stop().finally([client = std::move(client)] { 
        fmt::print("Client: Disconnected from server\n");
    });

}

int main(int ac, char** av) {
    app_template app;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(12345), "RPC server port")
        ("server", bpo::value<std::string>(), "Server address");

    static logger slp_logger("sleep rpc");
    rpc_context::get_protocol().set_logger(&slp_logger);

    return app.run_deprecated(ac, av, [&app] {
        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();
        std::string server_addr;
        if (config.count("server")) {
            server_addr = config["server"].as<std::string>();
            return run_client(server_addr, port);
        } else {
            fmt::print("Running in server mode on port {}\n", port);
            return run_server(port);
        }
    });
}
