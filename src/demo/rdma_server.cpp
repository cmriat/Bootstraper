/*
 * RDMA Server Implementation
 */
#include "demo/rdma_server.hpp"
#include <iostream>
#include <ucxx/utils/sockaddr.h>

namespace demo {

void RdmaServer::ListenerContext::createEndpointFromConnRequest(ucp_conn_request_h conn_request) {
    if (!isAvailable()) {
        throw std::runtime_error("Listener context already has an endpoint");
    }

    static bool endpoint_error_handling = true;
    auto listener = _server->_listener;
    if (!listener) {
        throw std::runtime_error("Listener is not initialized");
    }

    _endpoint = listener->createEndpointFromConnRequest(conn_request, endpoint_error_handling);
}

void RdmaServer::listenerCallback(ucp_conn_request_h conn_request, void* arg) {
    char ip_str[INET6_ADDRSTRLEN];
    char port_str[INET6_ADDRSTRLEN];
    ucp_conn_request_attr_t attr{};
    ListenerContext* listener_ctx = reinterpret_cast<ListenerContext*>(arg);

    attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
    ucxx::utils::ucsErrorThrow(ucp_conn_request_query(conn_request, &attr));
    ucxx::utils::sockaddr_get_ip_port_str(&attr.client_address, ip_str, port_str, INET6_ADDRSTRLEN);
    std::cout << "Server received a connection request from client at address " << ip_str << ":"
              << port_str << std::endl;

    if (listener_ctx->isAvailable()) {
        listener_ctx->createEndpointFromConnRequest(conn_request);
    } else {
        // The server is already handling a connection request from a client,
        // reject this new one
        std::cout << "Rejecting a connection request from " << ip_str << ":" << port_str << "."
                  << std::endl
                  << "Only one client at a time is supported." << std::endl;
        ucxx::utils::ucsErrorThrow(
            ucp_listener_reject(listener_ctx->getEndpoint()->getWorker()->getHandle(), conn_request));
    }
}

bool RdmaServer::initialize() {
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

    // Create UCXX listener
    _listener = _endpoint_manager.createListener(_ucxx_port, listenerCallback, _listener_ctx.get());
    if (!_listener) {
        std::cerr << "Failed to create UCXX listener" << std::endl;
        return false;
    }

    return true;
}

bool RdmaServer::allocateMemory(size_t size) {
    if (!_endpoint_manager.getContext()) {
        std::cerr << "Cannot allocate memory: UCXX context not initialized" << std::endl;
        return false;
    }

    try {
        _memory_region = std::make_unique<MemoryRegion>();

        // Allocate memory
        _memory_region->addr = malloc(size);
        if (!_memory_region->addr) {
            std::cerr << "Failed to allocate memory of size " << size << std::endl;
            return false;
        }
        _memory_region->size = size;

        // Register memory with UCX
        ucp_mem_map_params_t params;
        params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH;
        params.address = _memory_region->addr;
        params.length = size;

        ucs_status_t status = ucp_mem_map(_endpoint_manager.getContext()->getHandle(), &params,
                                          &_memory_region->mem_handle);
        if (status != UCS_OK) {
            std::cerr << "Failed to register memory with UCX: "
                      << ucs_status_string(status) << std::endl;
            return false;
        }

        // Pack remote key
        status = ucp_rkey_pack(_endpoint_manager.getContext()->getHandle(),
                              _memory_region->mem_handle,
                              &_memory_region->packed_rkey);
        if (status != UCS_OK) {
            std::cerr << "Failed to pack remote key: "
                      << ucs_status_string(status) << std::endl;
            return false;
        }

        // Convert rkey buffer to string for RPC transfer
        size_t rkey_size = ucp_rkey_packed_size(_endpoint_manager.getContext()->getHandle());
        _memory_region->rkey_str = std::string(
            static_cast<char*>(_memory_region->packed_rkey), rkey_size);

        std::cout << "Allocated and registered memory region of size "
                  << size << " bytes at address "
                  << _memory_region->addr << std::endl;

        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error allocating memory: " << e.what() << std::endl;
        _memory_region.reset();
        return false;
    }
}

seastar::future<> RdmaServer::start() {
    // Register RPC handlers

    // 1. Get memory region info
    _proto.register_handler(1, [this](seastar::sstring client_id) {
        // Check if client has a dedicated memory region
        MemoryRegion* memory_region = getClientMemoryRegion(client_id);

        // If no memory region found, allocate one for this client
        if (!memory_region) {
            // Default size: 1MB per client
            size_t default_size = 1024 * 1024;
            if (!allocateClientMemory(client_id, default_size)) {
                return seastar::make_exception_future<seastar::sstring>(
                    std::runtime_error("Failed to allocate memory region for client"));
            }
            memory_region = getClientMemoryRegion(client_id);
        }

        if (!memory_region) {
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("Memory region not available"));
        }

        std::string addr_str = std::to_string(reinterpret_cast<uint64_t>(memory_region->addr));
        std::string size_str = std::to_string(memory_region->size);

        return seastar::make_ready_future<seastar::sstring>(
            addr_str + ":" + size_str + ":" + memory_region->rkey_str);
    });

    // 2. Create UCXX endpoint
    _proto.register_handler(2, [this](seastar::sstring client_addr, uint16_t client_ucxx_port) {
        return seastar::make_ready_future<uint16_t>(_ucxx_port);
    });

    // 3. Release UCXX endpoint
    _proto.register_handler(3, [this](seastar::sstring client_addr, uint16_t client_ucxx_port) {
        _endpoint_manager.releaseEndpoint(client_addr, client_ucxx_port);
        return seastar::make_ready_future<bool>(true);
    });

    // 6. Release client memory
    _proto.register_handler(6, [this](seastar::sstring client_id) {
        bool success = releaseClientMemory(client_id);
        return seastar::make_ready_future<bool>(success);
    });

    // 4. Echo handler (for testing)
    _proto.register_handler(4, [](seastar::sstring payload) {
        return seastar::make_ready_future<seastar::sstring>(payload);
    });

    // 5. Handshake handler - establishes P2P communication
    _proto.register_handler(5, [this](seastar::sstring client_id, seastar::sstring client_addr, uint16_t client_rpc_port) {
        return seastar::make_ready_future<seastar::sstring>(
            "OK:" + std::to_string(_ucxx_port));
    });

    // Set up resource limits
    seastar::rpc::resource_limits limits;
    limits.bloat_factor = 1;
    limits.basic_request_size = 0;
    limits.max_memory = 10'000'000;

    // Create server
    _server = std::make_unique<seastar::rpc::protocol<rpc_benchmark::rpc_serializer>::server>(
        _proto, seastar::rpc::server_options{}, seastar::ipv4_addr{_rpc_port}, limits);

    fmt::print("RDMA RPC server started on port {} (UCXX port: {})\n", _rpc_port, _ucxx_port);
    return seastar::make_ready_future<>();
}

bool RdmaServer::allocateClientMemory(const std::string& client_id, size_t size) {
    if (!_endpoint_manager.getContext()) {
        std::cerr << "Cannot allocate memory: UCXX context not initialized" << std::endl;
        return false;
    }

    // Check if client already has a memory region
    auto it = _client_memory_regions.find(client_id);
    if (it != _client_memory_regions.end()) {
        std::cout << "Client " << client_id << " already has a memory region. Releasing it first." << std::endl;
        releaseClientMemory(client_id);
    }

    try {
        auto memory_region = std::make_unique<MemoryRegion>();

        // Allocate memory with page alignment for better performance
        memory_region->addr = aligned_alloc(4096, size);
        if (!memory_region->addr) {
            std::cerr << "Failed to allocate memory of size " << size << " for client " << client_id << std::endl;
            return false;
        }
        memory_region->size = size;

        // Register memory with UCX
        ucp_mem_map_params_t params;
        params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH;
        params.address = memory_region->addr;
        params.length = size;

        ucs_status_t status = ucp_mem_map(_endpoint_manager.getContext()->getHandle(), &params,
                                         &memory_region->mem_handle);
        if (status != UCS_OK) {
            std::cerr << "Failed to register memory with UCX for client " << client_id << ": "
                      << ucs_status_string(status) << std::endl;
            return false;
        }

        // Pack remote key
        status = ucp_rkey_pack(_endpoint_manager.getContext()->getHandle(),
                              memory_region->mem_handle,
                              &memory_region->packed_rkey);
        if (status != UCS_OK) {
            std::cerr << "Failed to pack remote key for client " << client_id << ": "
                      << ucs_status_string(status) << std::endl;
            return false;
        }

        // Convert rkey buffer to string for RPC transfer
        size_t rkey_size = ucp_rkey_packed_size(_endpoint_manager.getContext()->getHandle());
        memory_region->rkey_str = std::string(
            static_cast<char*>(memory_region->packed_rkey), rkey_size);

        // Store memory region
        _client_memory_regions[client_id] = std::move(memory_region);

        std::cout << "Allocated and registered memory region of size "
                  << size << " bytes for client " << client_id << std::endl;

        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error allocating memory for client " << client_id << ": " << e.what() << std::endl;
        return false;
    }
}

MemoryRegion* RdmaServer::getClientMemoryRegion(const std::string& client_id) {
    auto it = _client_memory_regions.find(client_id);
    if (it != _client_memory_regions.end()) {
        return it->second.get();
    }

    // If client-specific memory region not found, return the default one
    return _memory_region.get();
}

bool RdmaServer::releaseClientMemory(const std::string& client_id) {
    auto it = _client_memory_regions.find(client_id);
    if (it != _client_memory_regions.end()) {
        std::cout << "Releasing memory region for client " << client_id << std::endl;
        _client_memory_regions.erase(it);
        return true;
    }
    return false;
}

seastar::future<> RdmaServer::stop() {
    seastar::future<> server_stop = seastar::make_ready_future<>();

    if (_server) {
        server_stop = _server->stop();
    }

    return server_stop.then([this] {
        _listener.reset();
        _memory_region.reset();

        // Clear all client memory regions
        _client_memory_regions.clear();

        _endpoint_manager.stopProgressThread();
        return seastar::make_ready_future<>();
    });
}

} // namespace demo
