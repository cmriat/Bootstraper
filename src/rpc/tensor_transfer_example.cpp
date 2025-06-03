#include <boost/program_options.hpp>
#include <iostream>
#include <memory>
#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <string>
#include <ucxx/api.h>
#include <ucxx/worker.h>
#include <ucxx/endpoint.h>
#include <ucxx/buffer.h>
#include <ucxx/request.h>
#include <vector>

#include "rpc/tensor_transfer_backend.hpp"
#include "coucxx/pcq.hpp"
#include "rpc/rpc_common.hpp"

using namespace seastar;
namespace bpo = boost::program_options;

// Server function
future<> run_server(uint16_t port, btsp::TensorTransferManager& mgr,
                                   std::string& server_addr) {
    std::cout << "Starting tensor transfer server on port " << port << "\n";
    co_await mgr.initialize_listener(listener_port);

    auto listener_ctx = mgr.get_listener_context();
    auto worker = mgr.get_worker();

    auto&& shands_rpc = rpc_context::get_protocol();
    shands_rpc.register_handler(to_underlying(msg_type::PREPARE_TENSOR_TRANSFER), 
        [](TensorSpec spec) -> future<btsp::TensorTransferResponse> {
            std::stringstream shape_str;
            shape_str << "[";
            for (size_t i = 0; i < spec.shape.size(); ++i) {
                if (i > 0) shape_str << ", ";
                shape_str << spec.shape[i];
            }
            shape_str << "]";

            std::cout << "Server: preparing tensor with shape: " << shape_str.str() << "\n"
                      << "  Buffer type: " << static_cast<int>(spec.buffer_type) << "\n"
                      << "  Data type: " << static_cast<int>(spec.data_type) << "\n"
                      << "  Total size: " << spec.total_bytes() << " bytes\n";
            
            btsp::TensorTransferResponse response;
            response.tag = 347;
            return make_ready_future<btsp::TensorTransferResponse>(response);
        });

    std::cout << "RPC handlers registered.\n";

    rpc::server_options so;
    auto& mc = rpc_context::get_compressor();
    so.compressor_factory = &mc;

    rpc::resource_limits limits;
    limits.bloat_factor = 1;
    limits.basic_request_size = 1024;
    limits.max_memory = 10'000'000;
    std::cout << "Creating RPC server on port " << port << "\n";
    std::unique_ptr<rpc::protocol<serializer>::server> server =
    std::make_unique<rpc::protocol<serializer>::server>(shands_rpc, 
                                                        so, 
                                                        ipv4_addr{port},
                                                        limits);
    
    while (listener_ctx->isAvailable()) {
        worker->progress();
    }

    std::cout << "Server is now ready, waiting for client connection...\n";

    auto endpoint = listener_ctx->getEndpoint();

    std::vector<int> recv_buf(5, 0);
    auto recv_req = endpoint->tagRecv(recv_buf.data(), recv_buf.size() * sizeof(int), ucxx::Tag{0}, ucxx::TagMaskFull);
    // TODO: isolate polling task
    // Prevent blocking seastar event loop
    while(!recv_req->isCompleted()) {
        worker->progress();
    }
    recv_req->checkError();
    std::cout << "Received data: ";
    for (const auto& val : recv_buf) {
        std::cout << val << " ";
    }
    std::cout << "\n";

    auto keep_alive = std::make_shared<promise<>>();
    std::cout << "Press Enter to shut down the server...\n";
    std::thread([keep_alive] {
        std::cin.get();
        std::cout << "Shutting down...\n";
        keep_alive->set_value();
    }).detach();

    co_return co_await keep_alive->get_future().then([worker] {
        worker->stopProgressThread();
        return make_ready_future<>();
    });
}

// Client function
future<> run_client(const std::string& server_addr, uint16_t port,
                    btsp::TensorTransferManager& mgr) {
    std::cout << "Starting tensor transfer client, connecting to " << server_addr << ":" << port << "\n";
    auto worker = mgr.get_worker();
    auto endpoint = worker->createEndpointFromHostname(server_addr.c_str(), listener_port, true);

    auto&& shands_rpc = rpc_context::get_protocol();
    auto send_spec = shands_rpc.make_client<future<btsp::TensorTransferResponse>(TensorSpec)>(
        to_underlying(msg_type::PREPARE_TENSOR_TRANSFER));

    rpc::client_options co;
    auto& mc = rpc_context::get_compressor();
    co.compressor_factory = &mc;

    std::unique_ptr<rpc::protocol<serializer>::client> client =
        std::make_unique<rpc::protocol<serializer>::client>(shands_rpc, 
                                                            co, 
                                                            ipv4_addr{server_addr});

    (void)send_spec(*client, TensorSpec{ucxx::BufferType::RMM,DataType::FLOAT32, {2, 3, 4}})
        .then([&](btsp::TensorTransferResponse response) {
            std::cout << "Server assigned tag: " << response.tag << "\n";
        });

    std::cout << "Connected to server!\n";

    std::vector<int> send_buf = {1, 2, 3, 4, 5};
    auto send_req = endpoint->tagSend(send_buf.data(), send_buf.size() * sizeof(int), ucxx::Tag{0});
    while (!send_req->isCompleted()) {
        worker->progress();
    }
    send_req->checkError();
    std::cout << "Sent data: ";
    for (const auto& val : send_buf) {
        std::cout << val << " ";
    }
    std::cout << "\n";

    return make_ready_future<>();
}


int main(int ac, char** av) {
    app_template app;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(12345), "UCXX port")
        ("server", bpo::value<std::string>(), "Server address (client mode only)");

    return app.run_deprecated(ac, av, [&app] {
        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();
        std::string server_addr;
        btsp::initialize_tensor_transfer().then([] {
            std::cout << "Tensor transfer manager initialized.\n";
        }).get();
        auto&& mgr = btsp::get_tensor_transfer_manager();
        if (config.count("server")) {
            server_addr = config["server"].as<std::string>();
            std::cout << "Running in client mode, connecting to " << server_addr << ":" << port << "\n";
            return run_client(server_addr, port, mgr);
        } else {
            std::cout << "Running in server mode on port " << port << "\n";
            return run_server(port, mgr, server_addr);
        }
    });
}
