/*
 * P2P Communication Manager Implementation
 */
#include "demo/p2p_manager.hpp"
#include <iostream>
#include <chrono>
#include <seastar/core/sleep.hh>
#include <seastar/core/loop.hh>

namespace demo {

// Initialize static members
std::unique_ptr<P2PManager> P2PManager::_instance = nullptr;
std::mutex P2PManager::_instance_mutex;

P2PManager::P2PManager(const std::string& node_id, uint16_t rpc_port, uint16_t ucxx_port)
    : _node_id(node_id), _rpc_port(rpc_port), _ucxx_port(ucxx_port) {
    _server = std::make_unique<RdmaServer>(rpc_port, ucxx_port);
}

P2PManager* P2PManager::getInstance(const std::string& node_id,
                                   uint16_t rpc_port,
                                   uint16_t ucxx_port) {
    std::lock_guard<std::mutex> lock(_instance_mutex);

    if (!_instance) {
        if (node_id.empty() || rpc_port == 0) {
            throw std::runtime_error("Node ID and ports must be provided for first getInstance call");
        }
        _instance = std::unique_ptr<P2PManager>(new P2PManager(node_id, rpc_port, ucxx_port));
    }
    return _instance.get();
}

void P2PManager::destroyInstance() {
    std::lock_guard<std::mutex> lock(_instance_mutex);
    _instance.reset();
}

bool P2PManager::initialize() {
    if (!_server->initialize()) {
        std::cerr << "Failed to initialize RPC server" << std::endl;
        return false;
    }

    if (!_server->allocateMemory(10 * 1024 * 1024)) { // 10MB default memory
        std::cerr << "Failed to allocate memory for RDMA operations" << std::endl;
        return false;
    }

    return true;
}

seastar::future<> P2PManager::start() {
    return _server->start().then([this] {
        std::cout << "P2P Manager started successfully" << std::endl;

        // Schedule periodic connection monitoring
        // We'll use a simpler approach with a timer function
        auto monitor_connections = [this]() -> seastar::future<> {
            return monitorConnections()
                .handle_exception([](std::exception_ptr ep) {
                    try {
                        std::rethrow_exception(ep);
                    } catch (const std::exception& e) {
                        std::cerr << "Error in connection monitoring: " << e.what() << std::endl;
                    }
                    return seastar::make_ready_future<>();
                })
                .then([] {
                    // Schedule the next run after 30 seconds
                    return seastar::sleep(std::chrono::seconds(30));
                });
        };

        // Start the monitoring loop as a background task
        (void)seastar::keep_doing(monitor_connections);

        return seastar::make_ready_future<>();
    });
}

seastar::future<> P2PManager::stop() {
    std::vector<seastar::future<bool>> disconnect_futures;

    {
        std::lock_guard<std::mutex> lock(_clients_mutex);
        for (auto& client_pair : _clients) {
            disconnect_futures.push_back(disconnect(client_pair.first));
        }
    }

    return seastar::when_all(disconnect_futures.begin(), disconnect_futures.end())
        .discard_result()
        .then([this] {
            return _server->stop();
        });
}

seastar::future<bool> P2PManager::connect(const std::string& node_id,
                                         const std::string& remote_addr,
                                         uint16_t remote_port) {
    {
        std::lock_guard<std::mutex> lock(_clients_mutex);
        if (_clients.find(node_id) != _clients.end()) {
            return seastar::make_ready_future<bool>(true); // Already connected
        }
    }

    auto client = std::make_unique<RdmaClient>(_node_id);

    if (!client->initialize()) {
        std::cerr << "Failed to initialize client for node " << node_id << std::endl;
        return seastar::make_ready_future<bool>(false);
    }

    return client->connect(remote_addr, remote_port)
        .then([this, node_id, client = std::move(client)] () mutable {
            return client->getRemoteMemoryInfo()
                .then([this, node_id, client = std::move(client)] (bool success) mutable {
                    if (!success) {
                        return seastar::make_ready_future<bool>(false);
                    }

                    return client->createUCXXEndpoint()
                        .then([this, node_id, client = std::move(client)] (bool success) mutable {
                            if (!success) {
                                return seastar::make_ready_future<bool>(false);
                            }

                            {
                                std::lock_guard<std::mutex> lock(_clients_mutex);
                                _clients[node_id] = std::move(client);
                            }

                            std::cout << "Connected to node " << node_id << std::endl;
                            return seastar::make_ready_future<bool>(true);
                        });
                });
        });
}

seastar::future<bool> P2PManager::disconnect(const std::string& node_id) {
    std::unique_ptr<RdmaClient> client;

    {
        std::lock_guard<std::mutex> lock(_clients_mutex);
        auto it = _clients.find(node_id);
        if (it == _clients.end()) {
            return seastar::make_ready_future<bool>(false);
        }

        client = std::move(it->second);
        _clients.erase(it);
    }

    return client->stop().then([] {
        return seastar::make_ready_future<bool>(true);
    });
}

seastar::future<bool> P2PManager::sendData(const std::string& node_id,
                                          const void* data,
                                          size_t size,
                                          uint64_t remote_offset) {
    RdmaClient* client = getClient(node_id);
    if (!client) {
        return seastar::make_ready_future<bool>(false);
    }

    return client->rdmaWrite(data, size, remote_offset);
}

seastar::future<bool> P2PManager::receiveData(const std::string& node_id,
                                             void* data,
                                             size_t size,
                                             uint64_t remote_offset) {
    RdmaClient* client = getClient(node_id);
    if (!client) {
        return seastar::make_ready_future<bool>(false);
    }

    return client->rdmaRead(data, size, remote_offset);
}

RdmaClient* P2PManager::getClient(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(_clients_mutex);
    auto it = _clients.find(node_id);
    if (it == _clients.end()) {
        return nullptr;
    }

    return it->second.get();
}

seastar::future<bool> P2PManager::checkConnectionHealth(const std::string& node_id) {
    RdmaClient* client = getClient(node_id);
    if (!client) {
        return seastar::make_ready_future<bool>(false);
    }

    // Send a ping message to check connection health
    return client->echo("ping")
        .then([](seastar::sstring response) {
            return seastar::make_ready_future<bool>(response == "ping");
        })
        .handle_exception([node_id](std::exception_ptr ep) {
            try {
                std::rethrow_exception(ep);
            } catch (const std::exception& e) {
                std::cerr << "Connection health check failed for node " << node_id
                          << ": " << e.what() << std::endl;
            }
            return seastar::make_ready_future<bool>(false);
        });
}

seastar::future<> P2PManager::monitorConnections() {
    std::vector<std::string> node_ids;

    // Get a copy of all node IDs to avoid holding the lock during async operations
    {
        std::lock_guard<std::mutex> lock(_clients_mutex);
        for (const auto& client_pair : _clients) {
            node_ids.push_back(client_pair.first);
        }
    }

    // Check each connection and reconnect if needed
    return seastar::do_for_each(node_ids, [this](const std::string& node_id) {
        return checkConnectionHealth(node_id)
            .then([this, node_id](bool healthy) {
                if (!healthy) {
                    std::cout << "Connection to node " << node_id
                              << " is unhealthy, attempting to reconnect" << std::endl;
                    return reconnect(node_id);
                }
                return seastar::make_ready_future<bool>(true);
            })
            .then([node_id](bool success) {
                if (success) {
                    std::cout << "Connection to node " << node_id << " is healthy" << std::endl;
                } else {
                    std::cerr << "Failed to reconnect to node " << node_id << std::endl;
                }
                return seastar::make_ready_future<>();
            });
    });
}

seastar::future<bool> P2PManager::reconnect(const std::string& node_id) {
    RdmaClient* client = getClient(node_id);
    if (!client) {
        return seastar::make_ready_future<bool>(false);
    }

    // Get connection information before disconnecting
    std::string server_addr = client->getServerAddr();
    uint16_t server_port = client->getServerPort();

    // Disconnect and then connect again
    return disconnect(node_id)
        .then([this, node_id, server_addr, server_port](bool) {
            std::cout << "Reconnecting to node " << node_id
                      << " at " << server_addr << ":" << server_port << std::endl;
            return connect(node_id, server_addr, server_port);
        });
}

} // namespace demo