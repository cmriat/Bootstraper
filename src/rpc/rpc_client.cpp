#include "rpc/rpc_common.hpp"
#include "seastar/core/loop.hh"
#include "seastar/core/sstring.hh"
#include "seastar/rpc/rpc_types.hh"
#include <boost/program_options.hpp>
#include <chrono>
#include <seastar/core/app-template.hh>
#include <iostream>
#include <fmt/core.h>
#include <vector>
#include <ucxx/buffer.h>
using namespace seastar;
namespace bpo = boost::program_options;

future<> run_client(bpo::variables_map& config) {
    std::string server_addr = config["server"].as<std::string>();
    bool compress = config["compress"].as<bool>();

    auto& slp_rpc = rpc_context::get_protocol();

    std::cout << "Starting client, connecting to " << server_addr << "\n";

    auto sleep_ms = slp_rpc.make_client<void (int sleep_ms)>(to_underlying(msg_type::SLEEP_MS));
    auto echo = slp_rpc.make_client<future<rpc::tuple<sstring, rpc::optional<int>>> (sstring msg)>(to_underlying(msg_type::ECHO));
    auto goodbye = slp_rpc.make_client<future<sstring> (sstring msg)>(to_underlying(msg_type::GOODBYE));

    // Client function to create a tensor on the server
    auto create_tensor = slp_rpc.make_client<future<sstring> (TensorSpec spec)>(to_underlying(msg_type::CREATE_TENSOR));

    rpc::client_options co;
    if (compress) {
        auto& mc = rpc_context::get_compressor();
        co.compressor_factory = &mc;
    }


    static std::unique_ptr<rpc::protocol<serializer>::client> client =
        std::make_unique<rpc::protocol<serializer>::client>(slp_rpc, co, ipv4_addr{server_addr});

    for (int i = 0; i < 5; ++i) {
        fmt::print("Iteration {:d}\n", i);

        (void)echo(*client, "Hello RPC World!").then([] (rpc::tuple<sstring, rpc::optional<int>> response) {
            fmt::print("Client: echo response: '{}', {}\n", std::get<0>(response), std::get<1>(response));
        });
    }


    (void)sleep(500ms).then([sleep_ms] () mutable {
        auto now = rpc::rpc_clock_type::now();
        return parallel_for_each(std::views::iota(0, 25), [sleep_ms, now](int i) mutable {
            return sleep_ms(*client, 100).then([now, i] {
                auto later = rpc::rpc_clock_type::now();
                auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(later - now);
                fmt::print("Client: sleep {:d} diff {:d}us\n", i, diff.count());
            });
        }).then([now] {
            auto later = rpc::rpc_clock_type::now();
            auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(later - now);
            fmt::print("sleep_ms completed after {:d}ms\n", diff.count());
        });
    });

    // create a tensor on the server
    (void)sleep(1s).then([create_tensor] () mutable {
        auto now = rpc::rpc_clock_type::now();

        TensorSpec spec;
        spec.buffer_type = BufferType::RMM;
        spec.data_type = DataType::FLOAT32;
        spec.shape = {128, 128, 128};

        std::cout << "Client: sending tensor creation request...\n";
        std::cout << "  Shape: [";
        for (size_t i = 0; i < spec.shape.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << spec.shape[i];
        }
        std::cout << "]\n";
        std::cout << "  Total size: " << spec.total_bytes() << " bytes\n";

        return create_tensor(*client, spec).then([now] (sstring response) {
            auto later = rpc::rpc_clock_type::now();
            auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(later - now);
            fmt::print("Client: tensor creation response: '{}', completed after {:d}ms\n",
                       response, diff.count());
        });
    });


    return sleep(1s).then([goodbye] () mutable {
        std::cout << "Client: sending goodbye message to server...\n";
        return goodbye(*client, "goodbye").then([] (sstring response) {
            std::cout << "Client: received response to goodbye: " << response << "\n";
            return make_ready_future<>();
        });
    });
}
