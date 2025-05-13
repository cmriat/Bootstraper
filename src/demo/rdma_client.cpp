/*
 * RDMA Client Implementation
 */
#include "demo/rdma_client.hpp"
#include <iostream>

namespace demo {

bool RdmaClient::initialize() {
    // Initialize UCXX endpoint manager
    if (!_endpoint_manager.initialize()) {
        std::cerr << "Failed to initialize UCXX endpoint manager" << std::endl;
        return false;
    }
    
    // Start progress thread
    if (!_endpoint_manager.startProgressThread(true)) {
        std::cerr << "Failed to start UCXX progress thread" << std::endl;
        return false;
    }
    
    _remote_memory = std::make_unique<RemoteMemoryRegion>();
    
    return true;
}

seastar::future<> RdmaClient::connect(seastar::sstring server_addr, uint16_t server_port) {
    _server_addr = server_addr;
    _server_rpc_port = server_port;
    
    try {
        // Create RPC client
        _client = std::make_unique<seastar::rpc::protocol<rpc_benchmark::rpc_serializer>::client>(
            _proto, seastar::rpc::client_options{}, 
            seastar::ipv4_addr{_server_addr, _server_rpc_port});
        
        std::cout << "Connected to RPC server at " << _server_addr << ":" 
                  << _server_rpc_port << std::endl;
        
        return seastar::make_ready_future<>();
    } catch (std::exception& e) {
        std::cerr << "Failed to connect to RPC server: " << e.what() << std::endl;
        return seastar::make_exception_future<>(std::current_exception());
    }
}

seastar::future<bool> RdmaClient::getRemoteMemoryInfo() {
    if (!_client) {
        return seastar::make_exception_future<bool>(
            std::runtime_error("Not connected to RPC server"));
    }
    
    // Get memory region info from server
    auto get_mem_info = _proto.make_client<seastar::future<seastar::sstring> (seastar::sstring)>(1);
    
    return get_mem_info(*_client, "client").then([this](seastar::sstring result) {
        // Parse result: "addr:size:rkey_buffer"
        auto first_colon = result.find(':');
        if (first_colon == seastar::sstring::npos) {
            return seastar::make_exception_future<bool>(
                std::runtime_error("Invalid memory info format"));
        }
        
        auto second_colon = result.find(':', first_colon + 1);
        if (second_colon == seastar::sstring::npos) {
            return seastar::make_exception_future<bool>(
                std::runtime_error("Invalid memory info format"));
        }
        
        try {
            // Parse address and size
            _remote_memory->addr = std::stoull(result.substr(0, first_colon));
            _remote_memory->size = std::stoull(
                result.substr(first_colon + 1, second_colon - first_colon - 1));
            
            // Get rkey buffer
            std::string rkey_str = result.substr(second_colon + 1);
            _remote_memory->rkey_buffer.assign(rkey_str.begin(), rkey_str.end());
            
            std::cout << "Received remote memory info: addr=" << _remote_memory->addr
                      << ", size=" << _remote_memory->size
                      << ", rkey_buffer_size=" << _remote_memory->rkey_buffer.size() << std::endl;
            
            return seastar::make_ready_future<bool>(true);
        } catch (const std::exception& e) {
            std::cerr << "Error parsing memory info: " << e.what() << std::endl;
            return seastar::make_exception_future<bool>(std::current_exception());
        }
    });
}

seastar::future<bool> RdmaClient::createUCXXEndpoint() {
    if (!_client) {
        return seastar::make_exception_future<bool>(
            std::runtime_error("Not connected to RPC server"));
    }
    
    // Get UCXX port from server
    auto get_ucxx_port = _proto.make_client<seastar::future<uint16_t> (
        seastar::sstring, uint16_t)>(2);
    
    return get_ucxx_port(*_client, _server_addr, 0).then([this](uint16_t ucxx_port) {
        _server_ucxx_port = ucxx_port;
        
        std::cout << "Server UCXX port: " << _server_ucxx_port << std::endl;
        
        // Create UCXX endpoint
        _ucxx_endpoint = _endpoint_manager.createEndpoint(_server_addr, _server_ucxx_port);
        if (!_ucxx_endpoint) {
            return seastar::make_exception_future<bool>(
                std::runtime_error("Failed to create UCXX endpoint"));
        }
        
        // Unpack remote key
        if (_remote_memory && !_remote_memory->rkey_buffer.empty()) {
            ucs_status_t status = ucp_ep_rkey_unpack(
                _ucxx_endpoint->getHandle(),
                _remote_memory->rkey_buffer.data(),
                &_remote_memory->rkey);
            
            if (status != UCS_OK) {
                return seastar::make_exception_future<bool>(std::runtime_error(
                    std::string("Failed to unpack remote key: ") + ucs_status_string(status)));
            }
            
            std::cout << "Unpacked remote key successfully" << std::endl;
        } else {
            return seastar::make_exception_future<bool>(
                std::runtime_error("Remote memory info not available"));
        }
        
        return seastar::make_ready_future<bool>(true);
    });
}

seastar::future<bool> RdmaClient::rdmaWrite(
    const void* local_data, size_t size, uint64_t remote_offset) {
    
    if (!_ucxx_endpoint) {
        return seastar::make_exception_future<bool>(
            std::runtime_error("UCXX endpoint not created"));
    }
    
    if (!_remote_memory || !_remote_memory->rkey) {
        return seastar::make_exception_future<bool>(
            std::runtime_error("Remote memory info not available"));
    }
    
    if (remote_offset + size > _remote_memory->size) {
        return seastar::make_exception_future<bool>(
            std::runtime_error("RDMA write would exceed remote memory bounds"));
    }
    
    try {
        // Perform RDMA write
        auto request = _endpoint_manager.rdmaWrite(
            _ucxx_endpoint, 
            local_data, 
            size, 
            _remote_memory->addr + remote_offset, 
            _remote_memory->rkey);
        
        // Wait for completion
        _endpoint_manager.waitRequest(request);
        
        std::cout << "RDMA write completed successfully: " << size << " bytes" << std::endl;
        return seastar::make_ready_future<bool>(true);
    } catch (const std::exception& e) {
        std::cerr << "RDMA write failed: " << e.what() << std::endl;
        return seastar::make_exception_future<bool>(std::current_exception());
    }
}

seastar::future<bool> RdmaClient::rdmaRead(
    void* local_data, size_t size, uint64_t remote_offset) {
    
    if (!_ucxx_endpoint) {
        return seastar::make_exception_future<bool>(
            std::runtime_error("UCXX endpoint not created"));
    }
    
    if (!_remote_memory || !_remote_memory->rkey) {
        return seastar::make_exception_future<bool>(
            std::runtime_error("Remote memory info not available"));
    }
    
    if (remote_offset + size > _remote_memory->size) {
        return seastar::make_exception_future<bool>(
            std::runtime_error("RDMA read would exceed remote memory bounds"));
    }
    
    try {
        // Perform RDMA read
        auto request = _endpoint_manager.rdmaRead(
            _ucxx_endpoint, 
            local_data, 
            size, 
            _remote_memory->addr + remote_offset, 
            _remote_memory->rkey);
        
        // Wait for completion
        _endpoint_manager.waitRequest(request);
        
        std::cout << "RDMA read completed successfully: " << size << " bytes" << std::endl;
        return seastar::make_ready_future<bool>(true);
    } catch (const std::exception& e) {
        std::cerr << "RDMA read failed: " << e.what() << std::endl;
        return seastar::make_exception_future<bool>(std::current_exception());
    }
}

seastar::future<seastar::sstring> RdmaClient::echo(seastar::sstring payload) {
    if (!_client) {
        return seastar::make_exception_future<seastar::sstring>(
            std::runtime_error("Not connected to RPC server"));
    }
    
    auto echo_func = _proto.make_client<seastar::future<seastar::sstring> (seastar::sstring)>(4);
    return echo_func(*_client, payload);
}

seastar::future<> RdmaClient::stop() {
    seastar::future<> client_stop = seastar::make_ready_future<>();
    
    if (_client) {
        client_stop = _client->stop();
    }
    
    return client_stop.then([this] {
        _ucxx_endpoint.reset();
        _remote_memory.reset();
        _endpoint_manager.stopProgressThread();
        return seastar::make_ready_future<>();
    });
}

} // namespace demo
