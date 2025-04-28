#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/net/api.hh>
#include <iostream>
#include "bootstrap/unique_id.hpp"

int main(int argc, char** argv) {
    seastar::app_template app;

    app.add_options()
        ("port", boost::program_options::value<uint16_t>()->default_value(10000), "Server port");

    return app.run(argc, argv, [&app] () -> seastar::future<int> {
        auto& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();

        // Create a unique ID
        bootstrap::unique_id id = bootstrap::unique_id::create();
        std::cout << "Generated UniqueID: " << id.to_string() << std::endl;

        // Create server socket
        seastar::listen_options lo;
        lo.reuse_address = true;
        auto server_socket = seastar::listen(seastar::make_ipv4_address({port}), lo);

        std::cout << "Server listening on port " << port << std::endl;

        // Accept one connection
        return server_socket.accept().then([id] (seastar::accept_result ar) {
            std::cout << "Client connected from " << ar.remote_address << std::endl;

            // Send the unique ID to the client
            auto out = ar.connection.output();
            return out.write(reinterpret_cast<const char*>(id.get_id().data()), bootstrap::unique_id::kIdSize)
                .then([out = std::move(out)] () mutable {
                    return out.flush();
                }).then([connection = std::move(ar.connection)] () mutable {
                    std::cout << "UniqueID sent to client" << std::endl;
                    return seastar::make_ready_future<int>(0);
                });
        });
    });
}
