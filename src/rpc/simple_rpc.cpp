#include <boost/program_options.hpp>
#include <cstdint>
#include <iostream>
#include <memory>
#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <fmt/core.h>

#include "rpc/simple_rpc.hpp"

using namespace seastar;
namespace bpo = boost::program_options;

future<> run_server(uint16_t port) {
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
    
    return keep_alive->get_future();
}

future<> run_client(const std::string& server_addr, uint16_t port) {
    auto&& shands_rpc = rpc_context::get_protocol();
    auto send_spec = shands_rpc.make_client<future<> (int32_t)>(
        to_underlying(msg_type::PREPARE_TENSOR_TRANSFER));

    auto client = std::make_unique<rpc::protocol<serializer>::client>(shands_rpc, 
                                                                      ipv4_addr{server_addr, port});
    
    co_await send_spec(*client, 128);
    fmt::print("Client: Tensor spec sent successfully\n");
    
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
            return run_server(port);
        }
    });
}
