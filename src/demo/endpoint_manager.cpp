/*
 * RDMA Endpoint Manager Implementation
 */
#include "demo/endpoint_manager.hpp"
#include <iostream>

namespace demo {

EndpointManager::EndpointManager() {}

EndpointManager::~EndpointManager() {
    stopProgressThread();
    
    // Clear all endpoints
    std::lock_guard<std::mutex> lock(_mutex);
    _endpoints.clear();
    
    // Reset worker and context
    _worker.reset();
    _context.reset();
}

bool EndpointManager::initialize() {
    try {
        // Create UCXX context with default feature flags
        _context = ucxx::createContext({}, ucxx::Context::defaultFeatureFlags);
        
        // Create worker
        _worker = _context->createWorker();
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Failed to initialize UCXX: " << e.what() << std::endl;
        return false;
    }
}

bool EndpointManager::startProgressThread(bool polling_mode) {
    if (!_worker) {
        std::cerr << "Cannot start progress thread: worker not initialized" << std::endl;
        return false;
    }
    
    if (_progress_thread_running) {
        return true; // Already running
    }
    
    try {
        _worker->startProgressThread(polling_mode);
        _progress_thread_running = true;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Failed to start progress thread: " << e.what() << std::endl;
        return false;
    }
}

void EndpointManager::stopProgressThread() {
    if (_worker && _progress_thread_running) {
        try {
            _worker->stopProgressThread();
            _progress_thread_running = false;
        } catch (const std::exception& e) {
            std::cerr << "Error stopping progress thread: " << e.what() << std::endl;
        }
    }
}

std::shared_ptr<ucxx::Endpoint> EndpointManager::createEndpoint(
    const std::string& remote_addr, uint16_t port) {
    
    if (!_worker) {
        std::cerr << "Cannot create endpoint: worker not initialized" << std::endl;
        return nullptr;
    }
    
    std::string key = remote_addr + ":" + std::to_string(port);
    
    // Check if endpoint already exists
    {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _endpoints.find(key);
        if (it != _endpoints.end()) {
            return it->second;
        }
    }
    
    try {
        // Create new endpoint
        auto endpoint = _worker->createEndpointFromHostname(remote_addr.c_str(), port, true);
        
        // Store endpoint
        std::lock_guard<std::mutex> lock(_mutex);
        _endpoints[key] = endpoint;
        
        std::cout << "Created UCXX endpoint to " << remote_addr << ":" << port << std::endl;
        return endpoint;
    } catch (const std::exception& e) {
        std::cerr << "Failed to create endpoint to " << remote_addr << ":" << port 
                  << ": " << e.what() << std::endl;
        return nullptr;
    }
}

std::shared_ptr<ucxx::Endpoint> EndpointManager::getEndpoint(
    const std::string& remote_addr, uint16_t port) {
    
    std::string key = remote_addr + ":" + std::to_string(port);
    
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _endpoints.find(key);
    if (it != _endpoints.end()) {
        return it->second;
    }
    
    return nullptr;
}

void EndpointManager::releaseEndpoint(const std::string& remote_addr, uint16_t port) {
    std::string key = remote_addr + ":" + std::to_string(port);
    
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _endpoints.find(key);
    if (it != _endpoints.end()) {
        std::cout << "Releasing UCXX endpoint to " << remote_addr << ":" << port << std::endl;
        _endpoints.erase(it);
    }
}

std::shared_ptr<ucxx::Listener> EndpointManager::createListener(
    uint16_t port, ucp_listener_conn_callback_t callback, void* arg) {
    
    if (!_worker) {
        std::cerr << "Cannot create listener: worker not initialized" << std::endl;
        return nullptr;
    }
    
    try {
        auto listener = _worker->createListener(port, callback, arg);
        std::cout << "Created UCXX listener on port " << port << std::endl;
        return listener;
    } catch (const std::exception& e) {
        std::cerr << "Failed to create listener on port " << port << ": " << e.what() << std::endl;
        return nullptr;
    }
}

std::shared_ptr<ucxx::Request> EndpointManager::rdmaWrite(
    std::shared_ptr<ucxx::Endpoint> endpoint, const void* local_data, 
    size_t local_size, uint64_t remote_addr, ucp_rkey_h rkey) {
    
    if (!endpoint) {
        throw std::runtime_error("Cannot perform RDMA write: endpoint is null");
    }
    
    return endpoint->putBuffer(local_data, local_size, remote_addr, rkey);
}

std::shared_ptr<ucxx::Request> EndpointManager::rdmaRead(
    std::shared_ptr<ucxx::Endpoint> endpoint, void* local_data, 
    size_t local_size, uint64_t remote_addr, ucp_rkey_h rkey) {
    
    if (!endpoint) {
        throw std::runtime_error("Cannot perform RDMA read: endpoint is null");
    }
    
    return endpoint->getBuffer(local_data, local_size, remote_addr, rkey);
}

void EndpointManager::waitRequest(std::shared_ptr<ucxx::Request> request) {
    if (!request) {
        return;
    }
    
    if (!_worker) {
        throw std::runtime_error("Cannot wait for request: worker not initialized");
    }
    
    // Wait for request to complete
    while (!request->isCompleted()) {
        _worker->progress();
    }
    
    // Check for errors
    request->checkError();
}

} // namespace demo
