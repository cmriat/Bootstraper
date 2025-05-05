/*
 * RPC Server for Seastar
 *
 * This file contains the RPC server class for benchmarking.
 */
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/rpc/rpc.hh>
#include <seastar/net/api.hh>
#include <fmt/core.h>
#include "rpc/rpc_serializer.hpp"

namespace rpc_benchmark {

// Server class
class rpc_server {
private:
    seastar::rpc::protocol<rpc_serializer> _proto;
    std::unique_ptr<seastar::rpc::protocol<rpc_serializer>::server> _server;
    uint16_t _port;

public:
    rpc_server(uint16_t port)
        : _proto(rpc_serializer{}), _port(port) {}

    seastar::future<> start() {
        // Register echo handler
        _proto.register_handler(1, [](seastar::sstring payload) {
            return payload;
        });

        // Set up resource limits
        seastar::rpc::resource_limits limits;
        limits.bloat_factor = 1;
        limits.basic_request_size = 0;
        limits.max_memory = 10'000'000;

        // Create server
        _server = std::make_unique<seastar::rpc::protocol<rpc_serializer>::server>(
            _proto, seastar::rpc::server_options{}, seastar::ipv4_addr{_port}, limits);

        fmt::print("RPC server started on port {}\n", _port);
        return seastar::make_ready_future<>();
    }

    seastar::future<> stop() {
        if (_server) {
            return _server->stop();
        }
        return seastar::make_ready_future<>();
    }
};

} // namespace rpc_benchmark
