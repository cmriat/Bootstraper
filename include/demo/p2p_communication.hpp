/*
 * P2P Communication Interface
 *
 * This file defines the interface for P2P communication between nodes.
 */
#pragma once

#include <memory>
#include <string>
#include <seastar/core/future.hh>

namespace demo {

/**
 * @brief Interface for P2P communication
 * 
 * This interface defines the methods for P2P communication between nodes.
 */
class P2PCommunication {
public:
    virtual ~P2PCommunication() = default;
    
    /**
     * @brief Initialize the communication
     * 
     * @return true if initialization was successful
     * @return false if initialization failed
     */
    virtual bool initialize() = 0;
    
    /**
     * @brief Start the communication
     * 
     * @return seastar::future<> Future that resolves when the communication is started
     */
    virtual seastar::future<> start() = 0;
    
    /**
     * @brief Stop the communication
     * 
     * @return seastar::future<> Future that resolves when the communication is stopped
     */
    virtual seastar::future<> stop() = 0;
    
    /**
     * @brief Connect to a remote node
     * 
     * @param node_id Remote node identifier
     * @param remote_addr Remote address
     * @param remote_port Remote port
     * @return seastar::future<bool> Future that resolves to true if connection was successful
     */
    virtual seastar::future<bool> connect(const std::string& node_id, 
                                         const std::string& remote_addr, 
                                         uint16_t remote_port) = 0;
    
    /**
     * @brief Disconnect from a remote node
     * 
     * @param node_id Remote node identifier
     * @return seastar::future<bool> Future that resolves to true if disconnection was successful
     */
    virtual seastar::future<bool> disconnect(const std::string& node_id) = 0;
    
    /**
     * @brief Send data to a remote node
     * 
     * @param node_id Remote node identifier
     * @param data Data to send
     * @param size Size of data
     * @param offset Offset in remote memory
     * @return seastar::future<bool> Future that resolves to true if send was successful
     */
    virtual seastar::future<bool> sendData(const std::string& node_id, 
                                          const void* data, 
                                          size_t size, 
                                          uint64_t offset = 0) = 0;
    
    /**
     * @brief Receive data from a remote node
     * 
     * @param node_id Remote node identifier
     * @param data Buffer to receive data
     * @param size Size of data
     * @param offset Offset in remote memory
     * @return seastar::future<bool> Future that resolves to true if receive was successful
     */
    virtual seastar::future<bool> receiveData(const std::string& node_id, 
                                             void* data, 
                                             size_t size, 
                                             uint64_t offset = 0) = 0;
};

} // namespace demo