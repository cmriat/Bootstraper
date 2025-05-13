/*
 * RDMA Client for Seastar
 *
 * This file contains the RPC client class that integrates with UCXX for RDMA communication.
 */
#pragma once

#include <memory>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/rpc/rpc.hh>
#include <seastar/net/api.hh>
#include "rpc/rpc_serializer.hpp"
#include "demo/endpoint_manager.hpp"

namespace demo {

// Remote memory region information
struct RemoteMemoryRegion {
    uint64_t addr;
    size_t size;
    std::vector<char> rkey_buffer;
    ucp_rkey_h rkey;
    
    RemoteMemoryRegion() : addr(0), size(0), rkey(nullptr) {}
    
    ~RemoteMemoryRegion() {
        if (rkey) {
            ucp_rkey_destroy(rkey);
        }
    }
};

// RDMA RPC Client class
class RdmaClient {
private:
    seastar::rpc::protocol<rpc_benchmark::rpc_serializer> _proto;
    std::unique_ptr<seastar::rpc::protocol<rpc_benchmark::rpc_serializer>::client> _client;
    seastar::sstring _server_addr;
    uint16_t _server_rpc_port;
    uint16_t _server_ucxx_port;
    EndpointManager _endpoint_manager;
    std::shared_ptr<ucxx::Endpoint> _ucxx_endpoint;
    std::unique_ptr<RemoteMemoryRegion> _remote_memory;

public:
    RdmaClient() 
        : _proto(rpc_benchmark::rpc_serializer{}), 
          _server_rpc_port(0), 
          _server_ucxx_port(0) {}
    
    ~RdmaClient() {
        stop().get();
    }
    
    // Initialize the client
    bool initialize();
    
    // Connect to the server
    seastar::future<> connect(seastar::sstring server_addr, uint16_t server_port);
    
    // Get remote memory region information
    seastar::future<bool> getRemoteMemoryInfo();
    
    // Create UCXX endpoint to the server
    seastar::future<bool> createUCXXEndpoint();
    
    // Perform RDMA write operation
    seastar::future<bool> rdmaWrite(const void* local_data, size_t size, uint64_t remote_offset = 0);
    
    // Perform RDMA read operation
    seastar::future<bool> rdmaRead(void* local_data, size_t size, uint64_t remote_offset = 0);
    
    // Echo test (for testing RPC)
    seastar::future<seastar::sstring> echo(seastar::sstring payload);
    
    // Stop the client
    seastar::future<> stop();
    
    // Get the remote memory region
    RemoteMemoryRegion* getRemoteMemory() {
        return _remote_memory.get();
    }
    
    // Get the endpoint manager
    EndpointManager& getEndpointManager() {
        return _endpoint_manager;
    }
};

} // namespace demo
