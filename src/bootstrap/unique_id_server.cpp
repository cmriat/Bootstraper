#include "bootstrap/unique_id_server.hpp"
#include <iostream>

namespace bootstrap {

unique_id_server::unique_id_server(seastar::socket_address listen_address)
    : _listen_address(listen_address) {
    // Generate a unique ID when server is created
    _id = unique_id::create();
}

seastar::future<> unique_id_server::start() {
    // Set up the server socket to listen for connections
    seastar::listen_options lo;
    lo.reuse_address = true;
    _server_socket = seastar::listen(_listen_address, lo);

    std::cout << "Server listening on " << _listen_address << std::endl;

    // Start accepting connections in a background task
    (void)seastar::do_with(true, [this] (bool& running) {
        return seastar::do_until(
            [&running] { return !running; },
            [this, &running] {
                return _server_socket->accept().then(
                    [this] (seastar::accept_result ar) {
                        std::cout << "Client connected from " << ar.remote_address << std::endl;

                        // Handle the connection in a separate task
                        (void)seastar::with_gate(_gate, [this, socket = std::move(ar.connection), addr = ar.remote_address] () mutable {
                            return handle_connection(std::move(socket), addr);
                        });

                        return seastar::make_ready_future<>();
                    }
                ).handle_exception([&running] (std::exception_ptr ep) {
                    try {
                        std::rethrow_exception(ep);
                    } catch (const seastar::gate_closed_exception& e) {
                        // Expected during shutdown
                        running = false;
                    } catch (const std::exception& e) {
                        std::cerr << "Error in accept: " << e.what() << std::endl;
                    }
                    return seastar::make_ready_future<>();
                });
            }
        );
    });

    return seastar::make_ready_future<>();
}

seastar::future<> unique_id_server::stop() {
    // Close the server socket if it's open
    if (_server_socket) {
        _server_socket->abort_accept();
        _server_socket = std::nullopt;
    }

    // Wait for all connections to complete
    return _gate.close().then([] {
        return seastar::make_ready_future<>();
    });
}

const unique_id& unique_id_server::get_unique_id() const {
    return _id;
}

seastar::future<> unique_id_server::handle_connection(seastar::connected_socket socket,
                                                     seastar::socket_address addr) {
    auto out = socket.output();

    // Send the unique ID to the client
    return out.write(reinterpret_cast<const char*>(_id.get_id().data()), unique_id::kIdSize)
        .then([out = std::move(out)] () mutable {
            // Flush and close the output stream
            return out.flush();
        }).then([socket = std::move(socket)] () mutable {
            // Let the socket be destroyed when this lambda completes
            return seastar::make_ready_future<>();
        });
}

} // namespace bootstrap
