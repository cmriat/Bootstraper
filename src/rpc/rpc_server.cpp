#include "rpc/rpc_common.hpp"
#include "coucxx/co_tag.hpp"
#include <boost/program_options.hpp>
#include <seastar/core/app-template.hh>
#include <iostream>
#include <fmt/core.h>
#include <memory>
#include <sstream>
#include <vector>
#include <ucxx/api.h>
#include <ucxx/worker.h>
#include <ucxx/buffer.h>

coucxx::coroutine::task<std::shared_ptr<ucxx::Buffer>> create_tensor_coroutine(
    const TensorSpec& spec, std::shared_ptr<ucxx::Worker> worker);

using namespace seastar;
namespace bpo = boost::program_options;

future<> run_server(bpo::variables_map& config) {
    uint16_t port = config["port"].as<uint16_t>();
    bool compress = config["compress"].as<bool>();
    auto& slp_rpc = rpc_context::get_protocol();

    // create a promise that will be fulfilled when the server should shut down
    static auto keep_alive = std::make_shared<promise<>>();

    std::cout << "Starting server on port " << port << std::endl;

    // SLEEP_MS
    slp_rpc.register_handler(to_underlying(msg_type::SLEEP_MS), [](int ms) {
        return sleep(std::chrono::milliseconds(ms)).then([ms] {
            fmt::print("Server: sleep for {}ms\n", ms);
            return make_ready_future<>();
        });
    });

    // ECHO
    slp_rpc.register_handler(to_underlying(msg_type::ECHO), [](sstring msg) {
        std::cout << "Server: echo '" << msg << "'\n";
        return msg;
    });

    // GOODBYE
    slp_rpc.register_handler(to_underlying(msg_type::GOODBYE), [](sstring msg) {
        std::cout << "Server: received goodbye message: '" << msg << "'\n";
        std::cout << "Server: shutting down...\n";

        keep_alive->set_value();

        return make_ready_future<sstring>("Goodbye acknowledged, server shutting down");
    });

    // CREATE_TENSOR
    slp_rpc.register_handler(to_underlying(msg_type::CREATE_TENSOR), [](TensorSpec spec) {
        std::cout << "Server: received tensor creation request\n";

        // create a string representation of the tensor shape
        std::stringstream shape_str;
        shape_str << "[";
        for (size_t i = 0; i < spec.shape.size(); ++i) {
            if (i > 0) shape_str << ", ";
            shape_str << spec.shape[i];
        }
        shape_str << "]";

        std::cout << "  Buffer type: RMM (CUDA memory)" << "\n";
        std::cout << "  Data type: " << static_cast<int>(spec.data_type) << "\n";
        std::cout << "  Shape: " << shape_str.str() << "\n";
        std::cout << "  Total size: " << spec.total_bytes() << " bytes\n";

        try {
            auto buffer = ucxx::allocateBuffer(BufferType::RMM, spec.total_bytes());

            std::string result = "Tensor allocated successfully, size: " +
                                 std::to_string(buffer->getSize()) + " bytes";
            return make_ready_future<sstring>(result);
        } catch (const std::exception& e) {
            std::string error = "Failed to allocate tensor: " + std::string(e.what());
            return make_ready_future<sstring>(error);
        }
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

    return keep_alive->get_future();
}
