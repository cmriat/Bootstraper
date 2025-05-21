#pragma once
#include "seastar/net/socket_defs.hh"
#include <cmath>
#include <memory>
#include <seastar/core/app-template.hh>
#include <seastar/rpc/rpc.hh>
#include <seastar/core/sleep.hh>
#include <seastar/rpc/lz4_compressor.hh>
#include <seastar/util/log.hh>
#include <seastar/core/reactor.hh>
#include <seastar/rpc/rpc_types.hh>

using namespace seastar;
using namespace std::chrono_literals;


struct serializer {};

template <typename T, typename Output>
inline void write_arithmetic_type(Output& out, T v) {
    static_assert(std::is_arithmetic_v<T>, "must be arithmetic type");
    return out.write(reinterpret_cast<const char*>(&v), sizeof(T));
}

template <typename T, typename Input>
inline T read_arithmetic_type(Input& in) {
    static_assert(std::is_arithmetic_v<T>, "must be arithmetic type");
    T v;
    in.read(reinterpret_cast<char*>(&v), sizeof(T));
    return v;
}

// int32_t read and write
template <typename Output>
inline void write(serializer, Output& output, int32_t v) { return write_arithmetic_type(output, v); }

template <typename Input>
inline int32_t read(serializer, Input& input, rpc::type<int32_t>) { return read_arithmetic_type<int32_t>(input); }

// sstring read and write
template <typename Output>
inline void write(serializer, Output& out, const sstring& v) {
    write_arithmetic_type(out, uint32_t(v.size()));
    out.write(v.c_str(), v.size());
}

template <typename Input>
inline sstring read(serializer, Input& in, rpc::type<sstring>) {
    auto size = read_arithmetic_type<uint32_t>(in);
    sstring ret = uninitialized_string(size);
    in.read(ret.data(), size);
    return ret;
}

// 0 for sleep, 1 for echo
enum class msg_type : uint32_t {
    SLEEP_MS = 0,
    ECHO = 1,
};

template <typename Enum>
constexpr auto to_underlying(Enum e) -> std::underlying_type_t<Enum> {
    return static_cast<std::underlying_type_t<Enum>>(e);
}

namespace bpo = boost::program_options;
using protocol_type = rpc::protocol<serializer>;
using server_type = protocol_type::server;
using client_type = protocol_type::client;

future<> run_server(bpo::variables_map& config);
future<> run_client(bpo::variables_map& config);
future<> runner(bpo::variables_map& config);

class rpc_factory {
public:
    static std::unique_ptr<server_type> create_server(
        protocol_type& proto,
        rpc::server_options so,
        uint16_t port,
        const rpc::resource_limits& limits
    ) { return std::make_unique<server_type>(proto, so, ipv4_addr{port}, limits); }

    static std::unique_ptr<client_type> create_client(
        protocol_type& proto,
        rpc::client_options co,
        const std::string& server_addr
    ) { return std::make_unique<client_type>(proto, co, ipv4_addr{server_addr}); }
};



// compressor using lz4
class slp_comp : public rpc::compressor::factory {
    const sstring _name = "LZ4";
public:
    const sstring& supported() const override { return _name; }
    std::unique_ptr<rpc::compressor> negotiate(sstring feature, bool is_server) const override {
        return feature == _name ? std::make_unique<rpc::lz4_compressor>() : nullptr;
    }
};

class rpc_context {
public:
    static protocol_type& get_protocol() { 
        static protocol_type slp_rpc(serializer{});
        return slp_rpc;
    }
    static slp_comp& get_compressor() { 
        static slp_comp mc;
        return mc;
    }
};