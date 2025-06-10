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
#include <vector>
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

// 0 for sleep, 1 for echo, 2 for goodbye, 3 for tensor creation, 4 for tensor transfer, 5 for tensor data transfer
enum class msg_type : uint32_t {
    SLEEP_MS = 0,
    ECHO = 1,
    GOODBYE = 2,
    CREATE_TENSOR = 3,
    PREPARE_TENSOR_TRANSFER = 4,
    TENSOR_DATA_TRANSFER = 5,
};

template <typename Enum>
constexpr auto to_underlying(Enum e) -> std::underlying_type_t<Enum> {
    return static_cast<std::underlying_type_t<Enum>>(e);
}

// Data types for tensors
enum class DataType : uint32_t {
    FLOAT32 = 0,
    FLOAT64 = 1,
    INT32 = 2,
    INT64 = 3,
    UINT8 = 4,
    UINT16 = 5,
    UINT32 = 6,
    UINT64 = 7,
};

// Buffer types for memory allocation
enum class BufferType : uint32_t {
    HOST = 0,
    RMM = 1,  // RAPIDS Memory Manager for CUDA
    CUDA = 2,
};

// Tensor specification structure
struct TensorSpec {
    DataType data_type;
    std::vector<uint64_t> shape;

    // Calculate total size in bytes
    size_t total_bytes() const {
        if (shape.empty()) return 0;

        size_t total_elements = 1;
        for (auto dim : shape) {
            total_elements *= dim;
        }

        size_t element_size = 0;
        switch (data_type) {
            case DataType::FLOAT32:
            case DataType::INT32:
            case DataType::UINT32:
                element_size = 4;
                break;
            case DataType::FLOAT64:
            case DataType::INT64:
            case DataType::UINT64:
                element_size = 8;
                break;
            case DataType::UINT8:
                element_size = 1;
                break;
            case DataType::UINT16:
                element_size = 2;
                break;
        }

        return total_elements * element_size;
    }
};

// Serialization for DataType enum
template <typename Output>
inline void write(serializer, Output& out, DataType data_type) {
    write_arithmetic_type(out, static_cast<uint32_t>(data_type));
}

template <typename Input>
inline DataType read(serializer, Input& in, rpc::type<DataType>) {
    return static_cast<DataType>(read_arithmetic_type<uint32_t>(in));
}

// Serialization for std::vector<uint64_t> (for tensor shape)
template <typename Output>
inline void write(serializer, Output& out, const std::vector<uint64_t>& vec) {
    write_arithmetic_type(out, static_cast<uint32_t>(vec.size()));
    for (const auto& item : vec) {
        write_arithmetic_type(out, item);
    }
}

template <typename Input>
inline std::vector<uint64_t> read(serializer, Input& in, rpc::type<std::vector<uint64_t>>) {
    auto size = read_arithmetic_type<uint32_t>(in);
    std::vector<uint64_t> vec;
    vec.reserve(size);
    for (uint32_t i = 0; i < size; ++i) {
        vec.push_back(read_arithmetic_type<uint64_t>(in));
    }
    return vec;
}

// Serialization for TensorSpec
template <typename Output>
inline void write(serializer, Output& out, const TensorSpec& spec) {
    write(serializer{}, out, spec.data_type);
    write(serializer{}, out, spec.shape);
}

template <typename Input>
inline TensorSpec read(serializer, Input& in, rpc::type<TensorSpec>) {
    TensorSpec spec;
    spec.data_type = read(serializer{}, in, rpc::type<DataType>{});
    spec.shape = read(serializer{}, in, rpc::type<std::vector<uint64_t>>{});
    return spec;
}

using protocol_type = rpc::protocol<serializer>;
using server_type = protocol_type::server;
using client_type = protocol_type::client;

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