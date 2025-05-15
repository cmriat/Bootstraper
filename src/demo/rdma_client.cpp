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

        // Perform handshake with server
        auto handshake = _proto.make_client<seastar::future<seastar::sstring> (
            seastar::sstring, seastar::sstring, uint16_t)>(5);

        return handshake(*_client, _client_id, _server_addr, 0).then([this](seastar::sstring result) {
            // Parse handshake result: "OK:ucxx_port"
            auto colon_pos = result.find(':');
            if (colon_pos == seastar::sstring::npos || result.substr(0, colon_pos) != "OK") {
                return seastar::make_exception_future<>(
                    std::runtime_error("Handshake failed: " + std::string(result)));
            }

            // Store UCXX port for later use
            try {
                _server_ucxx_port = std::stoi(result.substr(colon_pos + 1));
            } catch (const std::exception& e) {
                return seastar::make_exception_future<>(
                    std::runtime_error("Invalid UCXX port in handshake response"));
            }

            return seastar::make_ready_future<>();
        });
    } catch (std::exception& e) {
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

    return get_mem_info(*_client, _client_id).then([this](seastar::sstring result) {
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

            return seastar::make_ready_future<bool>(true);
        } catch (const std::exception& e) {
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

        return seastar::make_ready_future<bool>(true);
    } catch (const std::exception& e) {
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

        return seastar::make_ready_future<bool>(true);
    } catch (const std::exception& e) {
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
        // Release client memory on server
        auto release_memory = _proto.make_client<seastar::future<bool> (seastar::sstring)>(6);

        // Try to release memory, but don't fail if it doesn't work
        try {
            // Capture the future to avoid warning about discarding nodiscard value
            auto release_future = release_memory(*_client, _client_id).then([](bool success) {
                if (success) {
                    std::cout << "Successfully released client memory on server" << std::endl;
                }
                return seastar::make_ready_future<>();
            }).handle_exception([](std::exception_ptr ep) {
                try {
                    std::rethrow_exception(ep);
                } catch (const std::exception& e) {
                    std::cerr << "Failed to release client memory: " << e.what() << std::endl;
                }
                return seastar::make_ready_future<>();
            });

            // We don't wait for this future to complete
            (void)release_future;
        } catch (...) {
            // Ignore errors when releasing memory
        }

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
