#pragma once

#include <seastar/core/future.hh>
#include <seastar/net/api.hh>
#include "bootstrap/unique_id.hpp"

namespace bootstrap {

/**
 * @brief Client implementation for UniqueID exchange
 * 
 * This class implements the client-side logic for UniqueID exchange.
 * It connects to a server and receives a unique ID.
 */
class unique_id_client {
public:
    /**
     * @brief Construct a new unique id client object
     * 
     * @param server_address The address of the server to connect to
     */
    explicit unique_id_client(seastar::socket_address server_address);
    
    /**
     * @brief Connect to the server and receive the unique ID
     * 
     * @return seastar::future<> Future that resolves when ID is received
     */
    seastar::future<> connect();
    
    /**
     * @brief Get the unique ID received from the server
     * 
     * @return const unique_id& Reference to the unique ID
     */
    const unique_id& get_unique_id() const;

private:
    seastar::socket_address _server_address;
    unique_id _id;
};

} // namespace bootstrap
