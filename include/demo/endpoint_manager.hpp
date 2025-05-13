/*
 * RDMA Endpoint Manager
 *
 * This file contains the RDMA Endpoint Manager class for managing UCXX endpoints.
 */
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <ucxx/api.h>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

namespace demo {

/**
 * @brief Manager for UCXX endpoints
 * 
 * This class manages UCXX endpoints for RDMA communication.
 * It provides methods to create, get, and release endpoints.
 */
class EndpointManager {
private:
    std::shared_ptr<ucxx::Context> _context;
    std::shared_ptr<ucxx::Worker> _worker;
    std::unordered_map<std::string, std::shared_ptr<ucxx::Endpoint>> _endpoints;
    std::mutex _mutex;  // Protects _endpoints
    bool _progress_thread_running = false;

public:
    /**
     * @brief Construct a new Endpoint Manager
     */
    EndpointManager();

    /**
     * @brief Destroy the Endpoint Manager
     */
    ~EndpointManager();

    /**
     * @brief Initialize the UCXX context and worker
     * 
     * @return true if initialization was successful
     * @return false if initialization failed
     */
    bool initialize();

    /**
     * @brief Start the progress thread
     * 
     * @param polling_mode Whether to use polling mode (true) or blocking mode (false)
     * @return true if the thread was started successfully
     * @return false if the thread failed to start
     */
    bool startProgressThread(bool polling_mode = true);

    /**
     * @brief Stop the progress thread
     */
    void stopProgressThread();

    /**
     * @brief Create an endpoint to a remote node
     * 
     * @param remote_addr Remote address
     * @param port Remote port
     * @return std::shared_ptr<ucxx::Endpoint> The created endpoint
     */
    std::shared_ptr<ucxx::Endpoint> createEndpoint(const std::string& remote_addr, uint16_t port);

    /**
     * @brief Get an existing endpoint
     * 
     * @param remote_addr Remote address
     * @param port Remote port
     * @return std::shared_ptr<ucxx::Endpoint> The endpoint, or nullptr if not found
     */
    std::shared_ptr<ucxx::Endpoint> getEndpoint(const std::string& remote_addr, uint16_t port);

    /**
     * @brief Release an endpoint
     * 
     * @param remote_addr Remote address
     * @param port Remote port
     */
    void releaseEndpoint(const std::string& remote_addr, uint16_t port);

    /**
     * @brief Get the UCXX Worker
     * 
     * @return std::shared_ptr<ucxx::Worker> The worker
     */
    std::shared_ptr<ucxx::Worker> getWorker() {
        return _worker;
    }

    /**
     * @brief Get the UCXX Context
     * 
     * @return std::shared_ptr<ucxx::Context> The context
     */
    std::shared_ptr<ucxx::Context> getContext() {
        return _context;
    }

    /**
     * @brief Create a listener for incoming connections
     * 
     * @param port Port to listen on
     * @param callback Callback function for connection requests
     * @param arg Argument to pass to the callback
     * @return std::shared_ptr<ucxx::Listener> The created listener
     */
    std::shared_ptr<ucxx::Listener> createListener(uint16_t port, 
                                                  ucp_listener_conn_callback_t callback,
                                                  void* arg);

    /**
     * @brief Perform an RDMA write operation
     * 
     * @param endpoint The endpoint to use
     * @param local_data Local data buffer
     * @param local_size Size of local data
     * @param remote_addr Remote memory address
     * @param rkey Remote memory key
     * @return std::shared_ptr<ucxx::Request> The request object
     */
    std::shared_ptr<ucxx::Request> rdmaWrite(std::shared_ptr<ucxx::Endpoint> endpoint,
                                            const void* local_data,
                                            size_t local_size,
                                            uint64_t remote_addr,
                                            ucp_rkey_h rkey);

    /**
     * @brief Perform an RDMA read operation
     * 
     * @param endpoint The endpoint to use
     * @param local_data Local data buffer
     * @param local_size Size of local data
     * @param remote_addr Remote memory address
     * @param rkey Remote memory key
     * @return std::shared_ptr<ucxx::Request> The request object
     */
    std::shared_ptr<ucxx::Request> rdmaRead(std::shared_ptr<ucxx::Endpoint> endpoint,
                                           void* local_data,
                                           size_t local_size,
                                           uint64_t remote_addr,
                                           ucp_rkey_h rkey);

    /**
     * @brief Wait for a request to complete
     * 
     * @param request The request to wait for
     */
    void waitRequest(std::shared_ptr<ucxx::Request> request);
};

} // namespace demo
