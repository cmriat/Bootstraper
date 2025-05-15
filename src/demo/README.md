# RDMA RPC Demo

A high-performance RDMA communication system based on Seastar RPC and UCXX. This system combines Seastar's asynchronous programming model with UCXX's RDMA capabilities to create an efficient distributed communication framework.

## System Architecture

The system employs a layered architecture that separates the control plane from the data plane. Each node maintains a persistent RPC server for P2P communication handshakes, then uses RPC clients to take over UCXX endpoints for RDMA communication.

The system consists of the following main components:

**P2PManager** (Top Layer): Implemented as a singleton, manages the entire P2P communication system, provides high-level APIs, and coordinates RPC servers and clients.

**RdmaServer/RdmaClient** (Middle Layer): RdmaServer handles RPC requests and manages memory regions; RdmaClient connects to servers and performs RDMA operations.

**EndpointManager** (Bottom Layer): Manages UCXX endpoints and listeners, handles low-level RDMA operations.

## Communication Flow

The system communication flow is divided into initialization, connection establishment, and data transfer phases. During initialization, the server allocates and registers memory with UCXX, starts the RPC service and UCXX listener. During connection establishment, the client connects to the server via RPC, performs handshakes to get the server's UCXX port, obtains remote memory region information, and creates a UCXX endpoint to connect to the server. During data transfer, the client directly performs RDMA read/write operations on the server's memory, eliminating server CPU involvement in data transfers, significantly improving performance.

## Building and Running

```bash
mkdir build
cd build
cmake ..
make
```

### Server Mode

```bash
./p2p_rdma_demo --node-id=server1 --rpc-port=10000 --ucxx-port=10001
```

### Client Mode

```bash
./p2p_rdma_demo --node-id=client1 --server=127.0.0.1 --server-id=server1 --payload-size=1024 --requests=1000
```

## Performance Testing

The project includes tools for comparing RPC and RDMA performance:

```bash
./benchmark_main
```

Or use a simpler benchmark:

```bash
./simple_benchmark
```

## Optimization Strategies

The following optimization strategies aim to improve the system's performance, reliability, and usability by making the code more concise, efficient, and removing redundancies.

### 1. Memory Management Optimization

The current implementation allocates a fixed 10MB memory region for all clients, which can lead to inefficient memory usage and resource contention. We can optimize memory management through:

- **Dynamic Memory Allocation**: Allocate memory regions on-demand based on client needs
- **Memory Pooling**: Implement a memory pool system for efficient memory region management
- **Client-Specific Memory Regions**: Allocate separate memory regions for each client to prevent contention
- **Zero-Copy Integration**: Better integrate with Seastar's zero-copy buffers

Implementation approach:
```cpp
// Client-specific memory regions
class RdmaServer {
private:
    // One memory region per client
    std::unordered_map<std::string, std::unique_ptr<MemoryRegion>> _client_memory_regions;

public:
    // Allocate memory for a specific client
    bool allocateClientMemory(const std::string& client_id, size_t size) {
        auto memory_region = std::make_unique<MemoryRegion>();

        // Allocate memory with page alignment for better performance
        memory_region->addr = aligned_alloc(4096, size);
        if (!memory_region->addr) {
            return false;
        }
        memory_region->size = size;

        // Register memory with UCX
        ucp_mem_map_params_t params;
        params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH;
        params.address = memory_region->addr;
        params.length = size;

        ucs_status_t status = ucp_mem_map(_endpoint_manager.getContext()->getHandle(),
                                         &params, &memory_region->mem_handle);
        if (status != UCS_OK) {
            return false;
        }

        // Pack remote key
        status = ucp_rkey_pack(_endpoint_manager.getContext()->getHandle(),
                              memory_region->mem_handle, &memory_region->packed_rkey);
        if (status != UCS_OK) {
            return false;
        }

        // Store memory region
        _client_memory_regions[client_id] = std::move(memory_region);
        return true;
    }

    // Get client memory region
    MemoryRegion* getClientMemoryRegion(const std::string& client_id) {
        auto it = _client_memory_regions.find(client_id);
        if (it != _client_memory_regions.end()) {
            return it->second.get();
        }
        return nullptr;
    }
};
```

### 2. Connection Management Improvements

The current implementation has basic connection management with limited error handling. We can improve connection management by:

- **Connection Pooling**: Maintain a pool of pre-established connections
- **Automatic Reconnection**: Implement automatic reconnection logic for failed connections
- **Connection Health Monitoring**: Periodically check connection health
- **Graceful Degradation**: Fall back to RPC if RDMA connection fails

Implementation approach:
```cpp
// Connection health monitoring and automatic reconnection
class P2PManager {
public:
    // Monitor all connection health
    seastar::future<> monitorConnections() {
        return seastar::do_for_each(_clients, [this](auto& client_pair) {
            return checkConnectionHealth(client_pair.first)
                .then([this, &client_pair](bool healthy) {
                    if (!healthy) {
                        std::cout << "Connection to " << client_pair.first
                                  << " unhealthy, attempting reconnect" << std::endl;
                        return reconnect(client_pair.first);
                    }
                    return seastar::make_ready_future<>();
                });
        });
    }

    // Check connection health
    seastar::future<bool> checkConnectionHealth(const std::string& node_id) {
        RdmaClient* client = getClient(node_id);
        if (!client) {
            return seastar::make_ready_future<bool>(false);
        }

        // Send heartbeat message to check connection
        return client->echo("ping")
            .then([](seastar::sstring response) {
                return seastar::make_ready_future<bool>(response == "ping");
            })
            .handle_exception([](std::exception_ptr ep) {
                return seastar::make_ready_future<bool>(false);
            });
    }

    // Reconnect to node
    seastar::future<bool> reconnect(const std::string& node_id) {
        RdmaClient* client = getClient(node_id);
        if (!client) {
            return seastar::make_ready_future<bool>(false);
        }

        std::string server_addr = client->getServerAddr();
        uint16_t server_port = client->getServerPort();

        // Disconnect old connection
        return disconnect(node_id)
            .then([this, node_id, server_addr, server_port](bool) {
                // Establish new connection
                return connect(node_id, server_addr, server_port);
            });
    }
};
```

### 3. Performance Optimization

The current implementation uses synchronous waiting for RDMA operations. We can further optimize performance by:

- **Asynchronous RDMA Operations**: Make RDMA operations fully asynchronous
- **Batch Operations**: Support batching multiple RDMA operations
- **Pipelining**: Implement pipelining for multiple operations
- **Zero-Copy Integration**: Better integration with Seastar's zero-copy buffers
- **RDMA Queue Pair Optimization**: Fine-tune UCXX parameters for optimal performance

Implementation approach:
```cpp
// Batch asynchronous RDMA operations
class RdmaClient {
public:
    // Batch RDMA write operation
    seastar::future<std::vector<bool>> batchRdmaWrite(
        const std::vector<std::pair<const void*, size_t>>& data_items,
        const std::vector<uint64_t>& offsets) {

        if (data_items.size() != offsets.size()) {
            return seastar::make_exception_future<std::vector<bool>>(
                std::runtime_error("Data items and offsets size mismatch"));
        }

        if (!_ucxx_endpoint || !_remote_memory || !_remote_memory->rkey) {
            return seastar::make_exception_future<std::vector<bool>>(
                std::runtime_error("RDMA endpoint not ready"));
        }

        std::vector<std::shared_ptr<ucxx::Request>> requests;
        requests.reserve(data_items.size());

        // Issue all RDMA write requests
        for (size_t i = 0; i < data_items.size(); i++) {
            if (offsets[i] + data_items[i].second > _remote_memory->size) {
                return seastar::make_exception_future<std::vector<bool>>(
                    std::runtime_error("RDMA write would exceed remote memory bounds"));
            }

            try {
                auto request = _endpoint_manager.rdmaWrite(
                    _ucxx_endpoint,
                    data_items[i].first,
                    data_items[i].second,
                    _remote_memory->addr + offsets[i],
                    _remote_memory->rkey);
                requests.push_back(request);
            } catch (const std::exception& e) {
                return seastar::make_exception_future<std::vector<bool>>(std::current_exception());
            }
        }

        // Asynchronously wait for all requests to complete
        return seastar::do_with(std::move(requests), [this](auto& requests) {
            return seastar::map_reduce(
                seastar::iter_from_range(requests),
                [this](auto& request) {
                    return seastar::do_until(
                        [&request] { return request->isCompleted(); },
                        [this] { return seastar::yield(); }
                    ).then([&request] {
                        try {
                            request->checkError();
                            return seastar::make_ready_future<bool>(true);
                        } catch (...) {
                            return seastar::make_ready_future<bool>(false);
                        }
                    });
                },
                std::vector<bool>(),
                [](std::vector<bool> results, bool result) {
                    results.push_back(result);
                    return results;
                }
            );
        });
    }

    // Zero-copy RDMA write using Seastar's temporary_buffer
    seastar::future<bool> zeroCopyRdmaWrite(
        seastar::temporary_buffer<char>&& buffer,
        uint64_t remote_offset = 0) {

        if (!_ucxx_endpoint || !_remote_memory || !_remote_memory->rkey) {
            return seastar::make_exception_future<bool>(
                std::runtime_error("RDMA endpoint not ready"));
        }

        if (remote_offset + buffer.size() > _remote_memory->size) {
            return seastar::make_exception_future<bool>(
                std::runtime_error("RDMA write would exceed remote memory bounds"));
        }

        return seastar::do_with(std::move(buffer), [this, remote_offset](auto& buf) {
            try {
                auto request = _endpoint_manager.rdmaWrite(
                    _ucxx_endpoint,
                    buf.get(),
                    buf.size(),
                    _remote_memory->addr + remote_offset,
                    _remote_memory->rkey);

                return seastar::do_until(
                    [&request] { return request->isCompleted(); },
                    [] { return seastar::yield(); }
                ).then([&request] {
                    request->checkError();
                    return seastar::make_ready_future<bool>(true);
                });
            } catch (const std::exception& e) {
                return seastar::make_exception_future<bool>(std::current_exception());
            }
        });
    }
};
```

### 4. Scalability Improvements

The current implementation uses a single-threaded design with limited scalability. We can improve scalability by:

- **Sharded Design**: Leverage Seastar's sharded architecture for multi-core scaling
- **Load Balancing**: Implement load balancing across shards
- **Resource Limits**: Add configurable resource limits to prevent overload
- **Backpressure Mechanisms**: Implement backpressure to handle high load

Implementation approach:
```cpp
// Sharded P2P manager design
class ShardedP2PManager {
private:
    seastar::sharded<P2PManager> _managers;

public:
    // Start all P2P manager shards
    seastar::future<> start(const std::string& node_id, uint16_t base_rpc_port, uint16_t base_ucxx_port) {
        return _managers.start(
            [node_id, base_rpc_port, base_ucxx_port](unsigned id) {
                // Each shard uses different ports
                uint16_t rpc_port = base_rpc_port + id;
                uint16_t ucxx_port = base_ucxx_port + id;
                std::string shard_node_id = fmt::format("{}_{}", node_id, id);

                return std::make_unique<P2PManager>(shard_node_id, rpc_port, ucxx_port);
            }
        ).then([this] {
            // Initialize all shards
            return _managers.invoke_on_all([](P2PManager& manager) {
                return manager.initialize();
            });
        }).then([this] {
            // Start all shards
            return _managers.invoke_on_all([](P2PManager& manager) {
                return manager.start();
            });
        });
    }

    // Select appropriate shard for node
    unsigned selectShardForNode(const std::string& node_id) {
        // Simple hash-based shard selection
        return std::hash<std::string>{}(node_id) % seastar::smp::count;
    }

    // Connect to remote node
    seastar::future<bool> connect(const std::string& node_id,
                                 const std::string& remote_addr,
                                 uint16_t remote_port) {
        unsigned shard = selectShardForNode(node_id);
        return _managers.invoke_on(shard, [=](P2PManager& manager) {
            return manager.connect(node_id, remote_addr, remote_port);
        });
    }

    // Send data to remote node
    seastar::future<bool> sendData(const std::string& node_id,
                                  const void* data,
                                  size_t size,
                                  uint64_t remote_offset = 0) {
        unsigned shard = selectShardForNode(node_id);
        // Need to copy data because cross-shard calls can't pass pointers
        return seastar::do_with(std::vector<char>(static_cast<const char*>(data),
                                                static_cast<const char*>(data) + size),
                              [=, this](auto& data_copy) {
            return _managers.invoke_on(shard, [=, &data_copy](P2PManager& manager) {
                return manager.sendData(node_id, data_copy.data(), data_copy.size(), remote_offset);
            });
        });
    }

    // Stop all shards
    seastar::future<> stop() {
        return _managers.stop();
    }
};
```

### 5. Error Handling and Reliability

The current implementation has basic error handling with limited recovery mechanisms. We can improve reliability by:

- **Comprehensive Error Handling**: Improve error detection and handling
- **Retry Mechanisms**: Implement automatic retries for failed operations
- **Timeout Handling**: Add configurable timeouts for all operations
- **Circuit Breakers**: Implement circuit breakers to prevent cascading failures

Implementation approach:
```cpp
// Retry mechanism with exponential backoff
template<typename Func>
seastar::future<bool> retryWithBackoff(Func func, int max_retries = 3,
                                      std::chrono::milliseconds initial_delay = std::chrono::milliseconds(100)) {
    return seastar::do_with(0, initial_delay, [=](int& retry_count, auto& delay) {
        return seastar::repeat([&, func]() {
            return func().then_wrapped([&](seastar::future<bool> f) {
                try {
                    bool result = f.get();
                    if (result) {
                        return seastar::stop_iteration::yes;
                    }
                } catch (...) {
                    // Operation failed, log exception
                    std::exception_ptr ep = std::current_exception();
                    try {
                        std::rethrow_exception(ep);
                    } catch (const std::exception& e) {
                        std::cerr << "Operation failed: " << e.what()
                                  << ", retry " << (retry_count + 1)
                                  << " of " << max_retries << std::endl;
                    }
                }

                if (++retry_count >= max_retries) {
                    return seastar::stop_iteration::yes;
                }

                // Exponential backoff
                delay *= 2;
                return seastar::sleep(delay).then([] {
                    return seastar::stop_iteration::no;
                });
            });
        }).then([&retry_count, max_retries]() {
            return retry_count < max_retries;
        });
    });
}

// RDMA operation with retry
seastar::future<bool> P2PManager::reliableSendData(const std::string& node_id,
                                                 const void* data,
                                                 size_t size,
                                                 uint64_t remote_offset = 0) {
    return retryWithBackoff([=, this]() {
        return this->sendData(node_id, data, size, remote_offset);
    });
}

// Circuit breaker pattern
class CircuitBreaker {
private:
    enum class State { CLOSED, OPEN, HALF_OPEN };
    State _state = State::CLOSED;
    int _failure_threshold;
    int _failure_count = 0;
    std::chrono::steady_clock::time_point _reset_time;
    std::chrono::milliseconds _reset_timeout;

public:
    CircuitBreaker(int failure_threshold = 5,
                  std::chrono::milliseconds reset_timeout = std::chrono::milliseconds(5000))
        : _failure_threshold(failure_threshold), _reset_timeout(reset_timeout) {}

    template<typename Func>
    seastar::future<typename std::result_of<Func()>::type> execute(Func func) {
        using return_type = typename std::result_of<Func()>::type;

        auto now = std::chrono::steady_clock::now();

        if (_state == State::OPEN) {
            if (now >= _reset_time) {
                _state = State::HALF_OPEN;
            } else {
                return seastar::make_exception_future<return_type>(
                    std::runtime_error("Circuit breaker is open"));
            }
        }

        return func().then_wrapped([this, func](seastar::future<return_type> f) {
            try {
                auto result = f.get();
                // Success, close the circuit
                _state = State::CLOSED;
                _failure_count = 0;
                return seastar::make_ready_future<return_type>(std::move(result));
            } catch (...) {
                // Failure, increment counter
                _failure_count++;

                if (_state == State::HALF_OPEN || _failure_count >= _failure_threshold) {
                    // Open the circuit
                    _state = State::OPEN;
                    _reset_time = std::chrono::steady_clock::now() + _reset_timeout;
                }

                return seastar::make_exception_future<return_type>(std::current_exception());
            }
        });
    }
};
```

## Implementation Plan

To implement these optimizations, we'll follow this step-by-step approach:

1. **Memory Management Optimization**
   - Refactor `RdmaServer` to support client-specific memory regions
   - Implement memory allocation on-demand
   - Add memory region cleanup when clients disconnect

2. **Connection Management Improvements**
   - Add connection health monitoring to `P2PManager`
   - Implement automatic reconnection logic
   - Add graceful degradation to RPC when RDMA fails

3. **Performance Optimization**
   - Implement batch RDMA operations
   - Add zero-copy integration with Seastar buffers
   - Optimize UCXX parameters for better performance

4. **Scalability Improvements**
   - Create a sharded version of `P2PManager`
   - Implement load balancing across shards
   - Add resource limits and backpressure mechanisms

5. **Error Handling and Reliability**
   - Implement retry mechanisms with exponential backoff
   - Add circuit breaker pattern
   - Improve error reporting and handling

## Conclusion

The RDMA RPC Demo provides a solid foundation for high-performance distributed communication using RDMA technology. By implementing the suggested optimizations, we can significantly improve the system's performance, reliability, and scalability.

The key strengths of the current implementation are:
- Clean separation of control plane (RPC) and data plane (RDMA)
- Efficient use of Seastar's asynchronous programming model
- Integration with UCXX for RDMA operations

The proposed optimizations address the main areas for improvement:
- Memory management and allocation strategies
- Connection management and reliability
- Performance optimizations for RDMA operations
- Scalability across multiple cores
- Error handling and recovery mechanisms

These optimizations will make the code more concise, efficient, and performant while maintaining the core architecture of the system.