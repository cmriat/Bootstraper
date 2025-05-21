#include "rpc/rpc_common.hpp"
#include <boost/program_options.hpp>
#include <seastar/core/app-template.hh>
#include <iostream>
#include <fmt/core.h>

using namespace seastar;
namespace bpo = boost::program_options;

future<> run_server(bpo::variables_map& config) {
    uint16_t port = config["port"].as<uint16_t>();
    bool compress = config["compress"].as<bool>();
    auto& slp_rpc = rpc_context::get_protocol();

    std::cout << "Starting server on port " << port << std::endl;

    slp_rpc.register_handler(to_underlying(msg_type::SLEEP_MS), [](int ms) {
        return sleep(std::chrono::milliseconds(ms)).then([ms] {
            fmt::print("Server: sleep for {}ms\n", ms);
            return make_ready_future<>();
        });
    });

    slp_rpc.register_handler(to_underlying(msg_type::ECHO), [](sstring msg) {
        std::cout << "Server: echo '" << msg << "'\n";
        return msg;
    });

    rpc::server_options so;
    if (compress) {
        auto& mc = rpc_context::get_compressor();
        so.compressor_factory = &mc;
    }

    rpc::resource_limits limits;
    limits.bloat_factor = 1;
    limits.basic_request_size = 0;
    limits.max_memory = 10'000'000;

    static std::unique_ptr<rpc::protocol<serializer>::server> server =
        std::make_unique<rpc::protocol<serializer>::server>(slp_rpc, so, ipv4_addr{port}, limits);

    std::cout << "RPC server listening on port " << port << "...\n";

    // create a promise that will never be fulfilled to keep the server running
    static auto keep_alive = std::make_shared<promise<>>();
    return keep_alive->get_future();
}
