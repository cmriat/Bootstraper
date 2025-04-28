#include "bootstrap/unique_id_client.hpp"
#include <iostream>

namespace bootstrap {

unique_id_client::unique_id_client(seastar::socket_address server_address)
    : _server_address(server_address) {
}

seastar::future<> unique_id_client::connect() {
    std::cout << "Client connecting to " << _server_address << std::endl;

    // Connect to the server
    return seastar::connect(_server_address).then([this](seastar::connected_socket socket) {
        auto in = socket.input();

        // Read the unique ID from the server
        return in.read_exactly(unique_id::kIdSize).then([this, in = std::move(in), socket = std::move(socket)]
                                                       (seastar::temporary_buffer<char> buf) mutable {
            // Create a unique_id from the received bytes
            _id = unique_id::from_bytes(buf.get());
            std::cout << "Received UniqueID: " << _id.to_string() << std::endl;

            // Close the input stream and let the socket be destroyed
            return in.close().finally([socket = std::move(socket)] () mutable {
                // Let the socket be destroyed when this lambda completes
                return seastar::make_ready_future<>();
            });
        });
    });
}

const unique_id& unique_id_client::get_unique_id() const {
    return _id;
}

} // namespace bootstrap
