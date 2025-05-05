/*
 * RPC Serializer for Seastar
 *
 * This file contains serialization functions for RPC communication.
 */
#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/rpc/rpc.hh>

namespace rpc_benchmark {

// Simple serializer for our benchmark
struct rpc_serializer {
};

// Serialization functions for basic types
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

// Serialization for integers
template <typename Output>
inline void write(rpc_serializer, Output& output, int32_t v) { return write_arithmetic_type(output, v); }

template <typename Output>
inline void write(rpc_serializer, Output& output, uint32_t v) { return write_arithmetic_type(output, v); }

template <typename Input>
inline int32_t read(rpc_serializer, Input& input, seastar::rpc::type<int32_t>) { 
    return read_arithmetic_type<int32_t>(input); 
}

template <typename Input>
inline uint32_t read(rpc_serializer, Input& input, seastar::rpc::type<uint32_t>) { 
    return read_arithmetic_type<uint32_t>(input); 
}

// Serialization for byte arrays (our payload)
template <typename Output>
inline void write(rpc_serializer, Output& out, const seastar::sstring& v) {
    write_arithmetic_type(out, uint32_t(v.size()));
    out.write(v.c_str(), v.size());
}

template <typename Input>
inline seastar::sstring read(rpc_serializer, Input& in, seastar::rpc::type<seastar::sstring>) {
    auto size = read_arithmetic_type<uint32_t>(in);
    seastar::sstring ret = seastar::uninitialized_string(size);
    in.read(ret.data(), size);
    return ret;
}

} // namespace rpc_benchmark
