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
// #include "coucxx/pcq.hpp"
#include "rpc/simple_rpc.hpp"

using namespace seastar;
namespace bpo = boost::program_options;

// Server function
future<> run_server(uint16_t port, btsp::TensorTransferManager& mgr,
                                   std::string& server_addr) {
    
    co_await mgr.initialize_listener(listener_port);

    auto listener_ctx = mgr.get_listener_context();
    auto worker = mgr.get_worker();

    auto&& shands_rpc = rpc_context::get_protocol();

    static auto keep_alive = std::make_shared<promise<>>();
    
    shands_rpc.register_handler(to_underlying(msg_type::PREPARE_TENSOR_TRANSFER), 
        [](int32_t spec) -> future<> {
            fmt::print("Server: preparing tensor with shape: {}\n", spec);
            return make_ready_future<>();
        });

    fmt::print("RPC handlers registered.\n");

    rpc::server_options so;
    auto& mc = rpc_context::get_compressor();
    so.compressor_factory = &mc;

    rpc::resource_limits limits;
    limits.bloat_factor = 1;
    limits.basic_request_size = 128;
    limits.max_memory = 10'000'000;
    fmt::print("Creating RPC server on port {}\n", port);

    static std::unique_ptr<rpc::protocol<serializer>::server> server =
        std::make_unique<rpc::protocol<serializer>::server>(shands_rpc, 
                                                            so, 
                                                            ipv4_addr{port},
                                                            limits);
    
    fmt::print("Server is now ready, waiting for client connection...\n");
    
    std::thread listener_checking_thread([&worker, &listener_ctx] {
        while (listener_ctx->isAvailable()) {
            worker->waitProgress();
        }
    });
    listener_checking_thread.detach();
    // while (listener_ctx->isAvailable()) {
    //     worker->progress();
    // }
    auto endpoint = listener_ctx->getEndpoint();
    fmt::print("Server got connection from client\n");

    std::vector<int> recv_buf(5, 0);
    fmt::print("Server: Creating receive request\n");

    auto recv_req = endpoint->tagRecv(recv_buf.data(), recv_buf.size() * sizeof(int), ucxx::Tag{0}, ucxx::TagMaskFull);
    fmt::print("Server: Waiting for data from client\n");

    std::thread req_checking_thread([&worker, &recv_req] {
        while(!recv_req->isCompleted()) {
            worker->waitProgress();
        }
    });
    req_checking_thread.detach();
    // while(!recv_req->isCompleted()) {
    //     worker->progress();
    // }
    recv_req->checkError();
    fmt::print("Received data: ");
    for (const auto& val : recv_buf) {
        fmt::print("{} ", val);
    }
    fmt::print("\n");

    std::cout << "Press Enter to shut down the server...\n";
    // return keep_alive->get_future();
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
    auto send_spec = shands_rpc.make_client<future<> (int32_t)>(
        to_underlying(msg_type::PREPARE_TENSOR_TRANSFER));

    rpc::client_options co;
    auto& mc = rpc_context::get_compressor();
    co.compressor_factory = &mc;
    
    fmt::print("Creating RPC client\n");
    auto client =
        std::make_unique<rpc::protocol<serializer>::client>(shands_rpc, 
                                                            co, 
                                                            ipv4_addr{server_addr, port});
    fmt::print("RPC client created\n");

    co_await send_spec(*client, 128);
    fmt::print("Client: Tensor spec sent successfully\n");

    std::vector<int> send_buf = {1, 2, 3, 4, 5};
    auto send_req = endpoint->tagSend(send_buf.data(), send_buf.size() * sizeof(int), ucxx::Tag{0});
    while (!send_req->isCompleted()) {
        worker->progress();
    }
    send_req->checkError();
    fmt::print("Sent data: ");
    for (const auto& val : send_buf) {
        fmt::print("{} ", val);
    }
    fmt::print("\n");

    co_return co_await client->stop().finally([client = std::move(client)] { 
        fmt::print("Client: Disconnected from server\n");
    });
}


int main(int ac, char** av) {
    app_template app;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(12345), "UCXX port")
        ("server", bpo::value<std::string>(), "Server address (client mode only)");

    return app.run_deprecated(ac, av, [&app] {
        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();

        static logger btsp_logger("tensor transfer rpc");
        rpc_context::get_protocol().set_logger(&btsp_logger);
        std::string server_addr;
        btsp::initialize_tensor_transfer().then([] {
            fmt::print("Tensor transfer manager initialized.\n");
        }).get();
        auto&& mgr = btsp::get_tensor_transfer_manager();
        // btsp::TensorTransferManager mgr{};
        if (config.count("server")) {
            server_addr = config["server"].as<std::string>();
            fmt::print("Running in client mode, connecting to {}: {}\n", server_addr, port);
            return run_client(server_addr, port, mgr);
        } else {
            fmt::print("Running in server mode on port {}\n", port);
            return run_server(port, mgr, server_addr);
        }
    });
}
