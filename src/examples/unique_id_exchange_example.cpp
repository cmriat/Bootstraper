#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <iostream>
#include "bootstrap/unique_id_exchange.hpp"

int main(int argc, char** argv) {
    seastar::app_template app;

    // Add command line options
    app.add_options()
        ("rank", boost::program_options::value<int>()->default_value(0), "Node rank (0 for server, >0 for clients)")
        ("server-address", boost::program_options::value<std::string>()->default_value("127.0.0.1"), "Server address")
        ("server-port", boost::program_options::value<uint16_t>()->default_value(10000), "Server port")
        ("wait-time", boost::program_options::value<int>()->default_value(5), "Time to wait before exiting (seconds)");

    return app.run(argc, argv, [&app] () -> seastar::future<int> {
        auto& config = app.configuration();
        int rank = config["rank"].as<int>();
        std::string server_address = config["server-address"].as<std::string>();
        uint16_t server_port = config["server-port"].as<uint16_t>();
        int wait_time = config["wait-time"].as<int>();

        std::cout << "Starting node with rank " << rank << std::endl;

        // Create socket address for the server
        seastar::ipv4_addr ip_addr(server_address, server_port);
        seastar::socket_address server_socket_addr(ip_addr);

        // Create a vector with the server address
        std::vector<seastar::socket_address> node_addresses = {server_socket_addr};

        // Create the UniqueID exchange service and use do_with to manage its lifetime
        return seastar::do_with(
            bootstrap::unique_id_exchange(rank, node_addresses),
            [wait_time](bootstrap::unique_id_exchange& exchange) {
                // Start the exchange process
                return exchange.start().then([&exchange, wait_time] {
                    // Wait for a while to ensure all nodes have time to connect
                    using namespace std::chrono_literals;
                    return seastar::sleep(std::chrono::seconds(wait_time)).then([&exchange] {
                        // Print the final UniqueID
                        std::cout << "Final UniqueID: " << exchange.get_unique_id().to_string() << std::endl;

                        // Stop the exchange service
                        return exchange.stop().then([] {
                            return 0; // Return success code
                        });
                    });
                });
            });
    });
}
