#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/do_with.hh>
#include <seastar/net/api.hh>
#include <iostream>
#include "bootstrap/unique_id.hpp"

using namespace seastar;

future<> tcp_client(std::string server_address, uint16_t port) {
    return do_with(socket_address(make_ipv4_address({server_address, port})), [] (auto& server_addr) {
        return connect(server_addr).then([] (connected_socket s) {
            return do_with(std::move(s), [] (auto& socket) {
                auto in = socket.input();
                
                // Read the unique ID from the server
                return in.read_exactly(bootstrap::unique_id::kIdSize).then([&in] (temporary_buffer<char> buf) {
                    // Create a unique_id from the received bytes
                    bootstrap::unique_id id = bootstrap::unique_id::from_bytes(buf.get());
                    std::cout << "Received UniqueID: " << id.to_string() << std::endl;
                    
                    // Close the input stream
                    return in.close();
                });
            });
        });
    });
}

int main(int argc, char** argv) {
    app_template app;
    
    app.add_options()
        ("server-address", boost::program_options::value<std::string>()->default_value("127.0.0.1"), "Server address")
        ("server-port", boost::program_options::value<uint16_t>()->default_value(10000), "Server port");
    
    return app.run(argc, argv, [&app] {
        auto& config = app.configuration();
        std::string server_address = config["server-address"].as<std::string>();
        uint16_t server_port = config["server-port"].as<uint16_t>();
        
        std::cout << "Connecting to server at " << server_address << ":" << server_port << std::endl;
        
        return tcp_client(server_address, server_port).then([] {
            return make_ready_future<int>(0);
        });
    });
}
