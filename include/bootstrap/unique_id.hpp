#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <random>
#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/api.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/rpc/rpc.hh>

namespace bootstrap {

class unique_id {
public:
    static constexpr size_t kIdSize = 16;
    using id_type = std::array<uint8_t, kIdSize>;

    unique_id() {
        _id.fill(0);
    }

    static unique_id create() {
        unique_id id;
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<uint64_t> dis;

        uint64_t* data = reinterpret_cast<uint64_t*>(id._id.data());
        data[0] = dis(gen);
        data[1] = dis(gen);
        return id;
    }

    static unique_id from_bytes(const char* bytes) {
        unique_id id;
        std::copy(bytes, bytes + kIdSize, id._id.begin());
        return id;
    }

    const id_type& get_id() const {
        return _id;
    }

    std::string to_string() const {
        std::stringstream ss;
        ss << std::hex;
        for (auto b : _id) {
            ss << std::setw(2) << std::setfill('0') << static_cast<int>(b);
        }
        return ss.str();
    }

    bool operator==(const unique_id& other) const {
        return _id == other._id;
    }

    bool operator!=(const unique_id& other) const {
        return !(*this == other);
    }

private:
    id_type _id;
};

template <typename Output>
void write_to(Output& out, const unique_id& id) {
    out.write(reinterpret_cast<const char*>(id.get_id().data()), unique_id::kIdSize);
}

template <typename Input>
unique_id read_from(Input& in) {
    char buffer[unique_id::kIdSize];
    in.read(buffer, unique_id::kIdSize);
    return unique_id::from_bytes(buffer);
}

} // namespace bootstrap
