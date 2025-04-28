#include "bootstrap/unique_id_exchange.hpp"
#include "bootstrap/unique_id_server.hpp"
#include "bootstrap/unique_id_client.hpp"
#include <iostream>

namespace bootstrap {

// Implementation of unique_id_exchange

unique_id_exchange::unique_id_exchange(int rank, std::vector<seastar::socket_address> node_addresses)
    : _rank(rank), _node_addresses(std::move(node_addresses)) {
}

seastar::future<> unique_id_exchange::start() {
    if (_rank == 0) {
        // This node is the server (rank 0)
        _server = std::make_unique<unique_id_server>(_node_addresses[0]);
        return _server->start().then([this] {
            // After server starts, get the generated ID
            _id = _server->get_unique_id();
            std::cout << "Server generated UniqueID: " << _id.to_string() << std::endl;
            return seastar::make_ready_future<>();
        });
    } else {
        // This node is a client
        _client = std::make_unique<unique_id_client>(_node_addresses[0]);
        return _client->connect().then([this] {
            // After client connects and receives ID, store it
            _id = _client->get_unique_id();
            std::cout << "Client received UniqueID: " << _id.to_string() << std::endl;
            return seastar::make_ready_future<>();
        });
    }
}

seastar::future<> unique_id_exchange::stop() {
    if (_server) {
        return _server->stop();
    }
    return seastar::make_ready_future<>();
}

const unique_id& unique_id_exchange::get_unique_id() const {
    return _id;
}

} // namespace bootstrap
