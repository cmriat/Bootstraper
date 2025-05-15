/*
 * P2P Communication Manager
 *
 * Manages persistent RPC servers for P2P communication handshakes
 * and UCXX endpoints for RDMA communication.
 */
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include "demo/endpoint_manager.hpp"
#include "demo/rdma_client.hpp"
#include "demo/rdma_server.hpp"
#include "rpc/rpc_serializer.hpp"

namespace demo {

/**
 * @brief P2P Communication Manager
 *
 * Manages persistent RPC servers and RDMA communication between nodes.
 */
class P2PManager {
private:
    // RPC server for handling connection requests
    std::unique_ptr<RdmaServer> _server;

    // Map of connected clients by node ID
    std::unordered_map<std::string, std::unique_ptr<RdmaClient>> _clients;
    std::mutex _clients_mutex;

    // Node information
    std::string _node_id;
    uint16_t _rpc_port;
    uint16_t _ucxx_port;

    // Singleton instance
    static std::unique_ptr<P2PManager> _instance;
    static std::mutex _instance_mutex;

    // Private constructor for singleton
    P2PManager(const std::string& node_id, uint16_t rpc_port, uint16_t ucxx_port);

public:
    /**
     * @brief Get the singleton instance
     */
    static P2PManager* getInstance(const std::string& node_id = "",
                                  uint16_t rpc_port = 0,
                                  uint16_t ucxx_port = 0);

    /**
     * @brief Destroy the singleton instance
     */
    static void destroyInstance();

    /**
     * @brief Initialize the P2P manager
     */
    bool initialize();

    /**
     * @brief Start the P2P manager
     */
    seastar::future<> start();

    /**
     * @brief Stop the P2P manager
     */
    seastar::future<> stop();

    /**
     * @brief Connect to a remote node
     */
    seastar::future<bool> connect(const std::string& node_id,
                                 const std::string& remote_addr,
                                 uint16_t remote_port);

    /**
     * @brief Disconnect from a remote node
     */
    seastar::future<bool> disconnect(const std::string& node_id);

    /**
     * @brief Send data to a remote node using RDMA
     */
    seastar::future<bool> sendData(const std::string& node_id,
                                  const void* data,
                                  size_t size,
                                  uint64_t remote_offset = 0);

    /**
     * @brief Receive data from a remote node using RDMA
     */
    seastar::future<bool> receiveData(const std::string& node_id,
                                     void* data,
                                     size_t size,
                                     uint64_t remote_offset = 0);

    /**
     * @brief Get the RPC server
     */
    RdmaServer* getServer() {
        return _server.get();
    }

    /**
     * @brief Get a client by node ID
     */
    RdmaClient* getClient(const std::string& node_id);

    /**
     * @brief Check connection health for a specific node
     *
     * @param node_id Node identifier
     * @return seastar::future<bool> Future that resolves to true if connection is healthy
     */
    seastar::future<bool> checkConnectionHealth(const std::string& node_id);

    /**
     * @brief Monitor all connections and attempt to reconnect unhealthy ones
     *
     * @return seastar::future<> Future that resolves when monitoring is complete
     */
    seastar::future<> monitorConnections();

    /**
     * @brief Reconnect to a node
     *
     * @param node_id Node identifier
     * @return seastar::future<bool> Future that resolves to true if reconnection was successful
     */
    seastar::future<bool> reconnect(const std::string& node_id);
};

} // namespace demo