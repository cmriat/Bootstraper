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
#include <ucxx/buffer.h>

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

// Use UCXX's BufferType
using BufferType = ucxx::BufferType;

enum class DataType : uint32_t {
    INT8 = 0,
    INT16 = 1,
    INT32 = 2,
    INT64 = 3,
    UINT8 = 4,
    UINT16 = 5,
    UINT32 = 6,
    UINT64 = 7,
    FLOAT16 = 8,
    FLOAT32 = 9,
    FLOAT64 = 10,
};

struct TensorSpec {
    BufferType buffer_type;
    DataType data_type;
    std::vector<size_t> shape;

    size_t total_bytes() const {
        size_t element_count = 1;
        for (auto dim : shape) {
            element_count *= dim;
        }

        size_t element_size = 0;
        switch (data_type) {
            case DataType::INT8:
            case DataType::UINT8:
                element_size = 1;
                break;
            case DataType::INT16:
            case DataType::UINT16:
            case DataType::FLOAT16:
                element_size = 2;
                break;
            case DataType::INT32:
            case DataType::UINT32:
            case DataType::FLOAT32:
                element_size = 4;
                break;
            case DataType::INT64:
            case DataType::UINT64:
            case DataType::FLOAT64:
                element_size = 8;
                break;
        }

        return element_count * element_size;
    }
};

// serialization for BufferType
template <typename Output>
inline void write(serializer, Output& out, const BufferType& v) {
    write_arithmetic_type(out, static_cast<uint32_t>(v));
}

template <typename Input>
inline BufferType read(serializer, Input& in, rpc::type<BufferType>) {
    return static_cast<BufferType>(read_arithmetic_type<uint32_t>(in));
}

// serialization for DataType
template <typename Output>
inline void write(serializer, Output& out, const DataType& v) {
    write_arithmetic_type(out, static_cast<uint32_t>(v));
}

template <typename Input>
inline DataType read(serializer, Input& in, rpc::type<DataType>) {
    return static_cast<DataType>(read_arithmetic_type<uint32_t>(in));
}

// serialization for std::vector<size_t>
template <typename Output>
inline void write(serializer, Output& out, const std::vector<size_t>& v) {
    write_arithmetic_type(out, uint32_t(v.size()));
    for (auto& item : v) {
        write_arithmetic_type(out, item);
    }
}

template <typename Input>
inline std::vector<size_t> read(serializer, Input& in, rpc::type<std::vector<size_t>>) {
    auto size = read_arithmetic_type<uint32_t>(in);
    std::vector<size_t> ret;
    ret.reserve(size);
    for (uint32_t i = 0; i < size; ++i) {
        ret.push_back(read_arithmetic_type<size_t>(in));
    }
    return ret;
}

// serialization for TensorSpec
template <typename Output>
inline void write(serializer, Output& out, const TensorSpec& v) {
    write(serializer{}, out, v.buffer_type);
    write(serializer{}, out, v.data_type);
    write(serializer{}, out, v.shape);
}

template <typename Input>
inline TensorSpec read(serializer, Input& in, rpc::type<TensorSpec>) {
    TensorSpec ret;
    ret.buffer_type = read(serializer{}, in, rpc::type<BufferType>{});
    ret.data_type = read(serializer{}, in, rpc::type<DataType>{});
    ret.shape = read(serializer{}, in, rpc::type<std::vector<size_t>>{});
    return ret;
}

// 0 for sleep, 1 for echo, 2 for goodbye, 3 for tensor creation
enum class msg_type : uint32_t {
    SLEEP_MS = 0,
    ECHO = 1,
    GOODBYE = 2,
    CREATE_TENSOR = 3,
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