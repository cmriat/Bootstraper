/*
 * RDMA Server for Seastar
 *
 * This file contains the RPC server class that integrates with UCXX for RDMA communication.
 */
#pragma once

#include <memory>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/rpc/rpc.hh>
#include <seastar/net/api.hh>
#include <fmt/core.h>
#include "rpc/rpc_serializer.hpp"
#include "demo/endpoint_manager.hpp"

namespace demo {

// Memory region information for RDMA operations
struct MemoryRegion {
    void* addr;
    size_t size;
    ucp_mem_h mem_handle;
    ucp_rkey_buffer_h rkey_buffer;
    std::string rkey_str;
    
    MemoryRegion() : addr(nullptr), size(0), mem_handle(nullptr), rkey_buffer(nullptr) {}
    
    ~MemoryRegion() {
        if (rkey_buffer) {
            ucp_rkey_buffer_release(rkey_buffer);
        }
        if (mem_handle) {
            ucp_mem_unmap(nullptr, mem_handle);
        }
        if (addr) {
            free(addr);
        }
    }
};

// RDMA RPC Server class
class RdmaServer {
private:
    seastar::rpc::protocol<rpc_benchmark::rpc_serializer> _proto;
    std::unique_ptr<seastar::rpc::protocol<rpc_benchmark::rpc_serializer>::server> _server;
    uint16_t _rpc_port;
    uint16_t _ucxx_port;
    EndpointManager _endpoint_manager;
    std::shared_ptr<ucxx::Listener> _listener;
    std::unique_ptr<MemoryRegion> _memory_region;
    
    // Listener context for handling connection requests
    class ListenerContext {
    private:
        RdmaServer* _server;
        std::shared_ptr<ucxx::Endpoint> _endpoint;
        
    public:
        explicit ListenerContext(RdmaServer* server) : _server(server), _endpoint(nullptr) {}
        
        ~ListenerContext() {
            releaseEndpoint();
        }
        
        bool isAvailable() const {
            return _endpoint == nullptr;
        }
        
        void createEndpointFromConnRequest(ucp_conn_request_h conn_request);
        
        void releaseEndpoint() {
            _endpoint.reset();
        }
        
        std::shared_ptr<ucxx::Endpoint> getEndpoint() {
            return _endpoint;
        }
    };
    
    std::unique_ptr<ListenerContext> _listener_ctx;
    
    // Static callback for UCXX listener
    static void listenerCallback(ucp_conn_request_h conn_request, void* arg);

public:
    RdmaServer(uint16_t rpc_port, uint16_t ucxx_port)
        : _proto(rpc_benchmark::rpc_serializer{}), 
          _rpc_port(rpc_port), 
          _ucxx_port(ucxx_port),
          _listener_ctx(std::make_unique<ListenerContext>(this)) {}
    
    ~RdmaServer() {
        stop().get();
    }
    
    // Initialize the server
    bool initialize();
    
    // Allocate memory for RDMA operations
    bool allocateMemory(size_t size);
    
    // Start the server
    seastar::future<> start();
    
    // Stop the server
    seastar::future<> stop();
    
    // Get the memory region
    MemoryRegion* getMemoryRegion() {
        return _memory_region.get();
    }
    
    // Get the endpoint manager
    EndpointManager& getEndpointManager() {
        return _endpoint_manager;
    }
};

} // namespace demo
