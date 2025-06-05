#include <boost/program_options.hpp>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "rpc/tensor_transfer_backend.hpp"
#include "rpc/simple_rpc.hpp"

using namespace seastar;
namespace bpo = boost::program_options;

// Server function using RDMA Thread Manager
future<> run_server(uint16_t port, btsp::RdmaThreadManager& mgr) {

    // Initialize listener using RDMA Thread Manager
    co_await mgr.initialize_listener(listener_port);

    auto rdma_manager = &mgr;

    static auto keep_alive = std::make_shared<promise<>>();
    std::vector<int> recv_buf(5, 0);
    co_await mgr.submit_tag_recv(recv_buf.data(), recv_buf.size() * sizeof(int), 0);

    co_return co_await keep_alive->get_future();
}

// Client function using RDMA Thread Manager
future<> run_client(const std::string& server_addr, uint16_t port,
                    btsp::RdmaThreadManager& mgr) {
    auto rdma_manager = &mgr;

    // Connect to RDMA server
    bool connected = co_await rdma_manager->submit_connect(server_addr, listener_port);
    if (!connected) {
        std::cerr << "Failed to connect to RDMA server" << std::endl;
        co_return;
    }
    
    fmt::print("Client: Connected to RDMA server\n");

    // Send actual data via RDMA
    std::vector<int> send_buf = {1, 2, 3, 4, 5};
    bool sent = co_await mgr.submit_tag_send(send_buf.data(), send_buf.size() * sizeof(int), 0);
    
    if (sent) {
        fmt::print("Client: RDMA send submitted successfully\n");
        fmt::print("Client: Sent RDMA data: ");
        for (const auto& val : send_buf) {
            fmt::print("{} ", val);
        }
        fmt::print("\n");
        
        // Wait a bit for the send to complete
        co_await sleep(std::chrono::milliseconds(1000));
    } else {
        fmt::print("Client: Failed to submit RDMA send request\n");
    }
    
}

int main(int ac, char** av) {
    
    bpo::options_description desc("Allowed options");
    desc.add_options()
        ("port", bpo::value<uint16_t>()->default_value(12345), "RPC server port")
        ("server", bpo::value<std::string>(), "Server address (client mode only)");

    bpo::variables_map vm;
    bpo::store(bpo::parse_command_line(ac, av, desc), vm);
    bpo::notify(vm);

    uint16_t port = vm["port"].as<uint16_t>();
    std::string server_addr;
    btsp::initialize_rdma_manager().then([] {
        fmt::print("RDMA thread manager initialized.\n");
    }).get();
    auto&& mgr = btsp::get_rdma_thread_manager();
    if (vm.count("server")) {
        server_addr = vm["server"].as<std::string>();
        fmt::print("Running in client mode, connecting to {}: {}\n", server_addr, port);
        run_client(server_addr, port, mgr);
    }



}
