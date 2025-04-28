#pragma once

#include <vector>
#include <seastar/core/future.hh>
#include <seastar/net/api.hh>
#include "bootstrap/unique_id.hpp"
#include "bootstrap/unique_id_server.hpp"
#include "bootstrap/unique_id_client.hpp"

namespace bootstrap {

/**
 * @brief Main class for UniqueID exchange between nodes
 *
 * This class coordinates the exchange of UniqueID between nodes.
 * It delegates to either a server or client implementation based on the node's rank.
 */
class unique_id_exchange {
public:
    /**
     * @brief Construct a new unique id exchange object
     *
     * @param rank The rank of this node (0 is the server)
     * @param node_addresses Vector of socket addresses for all nodes
     */
    unique_id_exchange(int rank, std::vector<seastar::socket_address> node_addresses);

    /**
     * @brief Start the UniqueID exchange process
     *
     * @return seastar::future<> Future that resolves when exchange is complete
     */
    seastar::future<> start();

    /**
     * @brief Stop the exchange service
     *
     * @return seastar::future<> Future that resolves when service is stopped
     */
    seastar::future<> stop();

    /**
     * @brief Get the unique ID
     *
     * @return const unique_id& Reference to the unique ID
     */
    const unique_id& get_unique_id() const;

private:
    int _rank;
    std::vector<seastar::socket_address> _node_addresses;
    unique_id _id;

    // Implementation objects (either server or client)
    std::unique_ptr<unique_id_server> _server;
    std::unique_ptr<unique_id_client> _client;
};

} // namespace bootstrap
