/*
 * RPC Client for Seastar
 *
 * This file contains the RPC client class for benchmarking.
 */
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/rpc/rpc.hh>
#include <seastar/net/api.hh>
#include "rpc/rpc_serializer.hpp"

namespace rpc_benchmark {

// Client class
class rpc_client {
private:
    seastar::rpc::protocol<rpc_serializer> _proto;
    std::unique_ptr<seastar::rpc::protocol<rpc_serializer>::client> _client;

public:
    rpc_client() : _proto(rpc_serializer{}) {}

    seastar::future<> connect(seastar::sstring addr, uint16_t port) {
        try {
            _client = std::make_unique<seastar::rpc::protocol<rpc_serializer>::client>(
                _proto, seastar::rpc::client_options{}, seastar::ipv4_addr{addr, port});
            return seastar::make_ready_future<>();
        } catch (std::exception& e) {
            return seastar::make_exception_future<>(std::current_exception());
        }
    }

    seastar::future<seastar::sstring> echo(seastar::sstring payload) {
        auto echo_func = _proto.make_client<seastar::future<seastar::sstring> (seastar::sstring)>(1);
        return echo_func(*_client, payload);
    }

    seastar::future<> stop() {
        if (_client) {
            return _client->stop();
        }
        return seastar::make_ready_future<>();
    }
};

} // namespace rpc_benchmark
