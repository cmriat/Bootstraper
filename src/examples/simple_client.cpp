#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/future.hh>
#include <seastar/net/api.hh>
#include <iostream>
#include "bootstrap/unique_id.hpp"

int main(int argc, char** argv) {
    seastar::app_template app;
    
    app.add_options()
        ("server-address", boost::program_options::value<std::string>()->default_value("127.0.0.1"), "Server address")
        ("server-port", boost::program_options::value<uint16_t>()->default_value(10000), "Server port");
    
    return app.run(argc, argv, [&app] () -> seastar::future<int> {
        auto& config = app.configuration();
        std::string server_address = config["server-address"].as<std::string>();
        uint16_t server_port = config["server-port"].as<uint16_t>();
        
        // Create socket address for the server
        seastar::ipv4_addr ip_addr(server_address, server_port);
        seastar::socket_address server_socket_addr(ip_addr);
        
        std::cout << "Connecting to server at " << server_socket_addr << std::endl;
        
        // Connect to the server
        return seastar::connect(server_socket_addr).then([] (seastar::connected_socket socket) {
            std::cout << "Connected to server" << std::endl;
            
            // Read the unique ID from the server
            auto in = socket.input();
            return in.read_exactly(bootstrap::unique_id::kIdSize).then([in = std::move(in), socket = std::move(socket)]
                                                                      (seastar::temporary_buffer<char> buf) mutable {
                // Create a unique_id from the received bytes
                bootstrap::unique_id id = bootstrap::unique_id::from_bytes(buf.get());
                std::cout << "Received UniqueID: " << id.to_string() << std::endl;
                
                // Close the input stream
                return in.close().then([socket = std::move(socket)] () mutable {
                    return seastar::make_ready_future<int>(0);
                });
            });
        });
    });
}
