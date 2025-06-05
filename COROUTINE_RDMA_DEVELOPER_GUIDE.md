# 协程 RDMA 管理器开发文档

## 概述

协程 RDMA 管理器 (CoroutineRdmaManager) 是一个基于 C++20 协程的高性能 RDMA 通信框架。它提供了简洁的异步编程接口，让复杂的 RDMA 操作看起来像同步代码，同时保持高性能和完全独立性。

## 架构概览

### 分层架构

1. **应用层**：用户应用程序、Seastar RPC 服务等
2. **协程接口层**：提供 co_await 风格的异步 API
3. **协程类型系统**：RdmaTask、RdmaAwaitable 等协程类型
4. **协程管理层**：协程生命周期管理和调度
5. **多线程执行层**：Progress、Request、State Machine 线程
6. **RDMA 资源层**：UCXX 资源封装
7. **网络传输层**：UCX 和 RDMA 硬件

### 核心组件

- **CoroutineRdmaManager**：主要管理类
- **RdmaTask<T>**：协程任务类型
- **RdmaAwaitable**：RDMA 操作的可等待对象
- **StateChangeAwaitable**：状态变化的可等待对象

## 快速开始

### 1. 基本设置

```cpp
#include "rpc/coroutine_rdma_manager.hpp"
using namespace btsp;

// 创建管理器实例
CoroutineRdmaManager manager;

// 初始化（服务器模式）
if (!manager.initialize(true, 12345)) {
    std::cerr << "初始化失败" << std::endl;
    return;
}
```

### 2. 简单的服务器协程

```cpp
RdmaTask<void> server_coroutine() {
    CoroutineRdmaManager manager;
    manager.initialize(true, 12345);
    
    // 等待客户端连接
    std::cout << "等待连接..." << std::endl;
    co_await manager.wait_for_connection();
    std::cout << "客户端已连接!" << std::endl;
    
    // 接收数据
    std::vector<int> buffer(4, 0);
    RdmaOpResult result = co_await manager.tag_recv(
        buffer.data(), 
        buffer.size() * sizeof(int), 
        42  // tag
    );
    
    if (result.success()) {
        std::cout << "接收成功: ";
        for (const auto& val : buffer) {
            std::cout << val << " ";
        }
        std::cout << std::endl;
    }
    
    manager.shutdown();
}
```

### 3. 简单的客户端协程

```cpp
RdmaTask<void> client_coroutine() {
    CoroutineRdmaManager manager;
    manager.initialize(false, 0);
    
    // 连接服务器
    std::cout << "连接服务器..." << std::endl;
    RdmaOpResult connect_result = co_await manager.connect("127.0.0.1", 12345);
    
    if (!connect_result.success()) {
        std::cerr << "连接失败" << std::endl;
        co_return;
    }
    
    std::cout << "连接成功!" << std::endl;
    
    // 发送数据
    std::vector<int> data = {10, 20, 30, 40};
    RdmaOpResult send_result = co_await manager.tag_send(
        data.data(),
        data.size() * sizeof(int),
        42  // tag
    );
    
    if (send_result.success()) {
        std::cout << "发送成功!" << std::endl;
    }
    
    manager.shutdown();
}
```

### 4. 运行协程

```cpp
void run_coroutine(auto coroutine_func) {
    auto task = coroutine_func();
    
    // 等待协程完成
    while (!task.done()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    // 检查结果
    try {
        task.get();
        std::cout << "协程完成成功" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "协程异常: " << e.what() << std::endl;
    }
}

int main() {
    // 运行服务器或客户端
    run_coroutine(server_coroutine);  // 或 client_coroutine
    return 0;
}
```

## API 参考

### CoroutineRdmaManager 类

#### 初始化和生命周期

```cpp
// 构造函数
CoroutineRdmaManager();

// 析构函数
~CoroutineRdmaManager();

// 初始化管理器
bool initialize(bool server_mode = false, uint16_t port = 0);

// 关闭管理器
void shutdown();
```

#### 协程 RDMA 操作

```cpp
// 发送数据
RdmaAwaitable tag_send(void* data, size_t size, uint64_t tag);

// 接收数据
RdmaAwaitable tag_recv(void* data, size_t size, uint64_t tag);

// 连接到远程主机
RdmaAwaitable connect(const std::string& remote_addr, uint16_t remote_port);

// 监听连接
RdmaAwaitable listen(uint16_t port);
```

#### 状态管理

```cpp
// 等待特定状态
StateChangeAwaitable wait_for_state(RdmaState target_state);

// 等待连接建立
StateChangeAwaitable wait_for_connection();

// 查询当前状态
RdmaState get_state() const;
bool is_connected() const;
bool is_running() const;
```

### RdmaOpResult 结构

```cpp
struct RdmaOpResult {
    RdmaResult result;           // 操作结果
    size_t bytes_transferred;    // 传输字节数
    
    bool success() const;        // 是否成功
    operator bool() const;       // 隐式转换为 bool
};
```

### RdmaResult 枚举

```cpp
enum class RdmaResult {
    SUCCESS,        // 成功
    FAILURE,        // 失败
    TIMEOUT,        // 超时
    DISCONNECTED    // 连接断开
};
```

### RdmaState 枚举

```cpp
enum class RdmaState {
    IDLE,           // 空闲
    LISTENING,      // 监听中
    CONNECTING,     // 连接中
    CONNECTED,      // 已连接
    SENDING,        // 发送中
    RECEIVING,      // 接收中
    ERROR,          // 错误
    SHUTDOWN        // 关闭
};
```

## 高级用法

### 1. Tag Matching 通信

```cpp
RdmaTask<void> tag_matching_example() {
    CoroutineRdmaManager manager;
    manager.initialize(true, 12345);
    
    co_await manager.wait_for_connection();
    
    // 接收不同类型的数据
    std::vector<int> int_data(5, 0);
    std::vector<float> float_data(3, 0.0f);
    char string_data[256] = {0};
    
    // 使用不同的 tag 接收数据
    auto int_result = co_await manager.tag_recv(
        int_data.data(), int_data.size() * sizeof(int), 100);
    
    auto float_result = co_await manager.tag_recv(
        float_data.data(), float_data.size() * sizeof(float), 200);
    
    auto string_result = co_await manager.tag_recv(
        string_data, sizeof(string_data), 300);
    
    // 处理结果
    if (int_result.success()) {
        std::cout << "接收到整数数组" << std::endl;
    }
    if (float_result.success()) {
        std::cout << "接收到浮点数组" << std::endl;
    }
    if (string_result.success()) {
        std::cout << "接收到字符串: " << string_data << std::endl;
    }
}
```

### 2. 错误处理

```cpp
RdmaTask<void> error_handling_example() {
    CoroutineRdmaManager manager;
    
    try {
        if (!manager.initialize(false, 0)) {
            throw std::runtime_error("初始化失败");
        }
        
        // 尝试连接
        RdmaOpResult result = co_await manager.connect("192.168.1.100", 12345);
        
        if (!result.success()) {
            if (result.result == RdmaResult::TIMEOUT) {
                std::cerr << "连接超时" << std::endl;
            } else if (result.result == RdmaResult::DISCONNECTED) {
                std::cerr << "连接被拒绝" << std::endl;
            } else {
                std::cerr << "连接失败" << std::endl;
            }
            co_return;
        }
        
        // 继续处理...
        
    } catch (const std::exception& e) {
        std::cerr << "协程异常: " << e.what() << std::endl;
    }
    
    manager.shutdown();
}
```

### 3. 状态监控

```cpp
RdmaTask<void> state_monitoring_example() {
    CoroutineRdmaManager manager;
    manager.initialize(true, 12345);
    
    // 等待连接
    std::cout << "当前状态: " << static_cast<int>(manager.get_state()) << std::endl;
    
    co_await manager.wait_for_state(RdmaState::CONNECTED);
    std::cout << "连接已建立" << std::endl;
    
    // 监控连接状态
    while (manager.is_connected()) {
        // 执行 RDMA 操作
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    std::cout << "连接已断开" << std::endl;
}
```

## 编译和构建

### 依赖要求

- **C++20 编译器**：GCC 10+, Clang 14+
- **UCXX 库**：RDMA 通信库
- **CMake 3.15+**：构建系统
- **可选**：Folly, glog

### 编译选项

```cmake
# CMakeLists.txt 中的设置
target_compile_options(your_target PRIVATE -std=c++20)
target_link_libraries(your_target ucxx::ucxx)
```

### 编译命令

```bash
mkdir build && cd build
cmake ..
make your_target
```

## 性能优化

### 1. 内存管理

```cpp
// 预分配缓冲区
class RdmaBufferPool {
    std::vector<std::unique_ptr<char[]>> buffers;
    
public:
    char* get_buffer(size_t size) {
        // 返回预分配的缓冲区
    }
};
```

### 2. 批量操作

```cpp
RdmaTask<void> batch_operations() {
    CoroutineRdmaManager manager;
    // ... 初始化 ...
    
    // 批量发送多个消息
    std::vector<RdmaOpResult> results;
    
    for (int i = 0; i < 10; ++i) {
        auto result = co_await manager.tag_send(data, size, tag + i);
        results.push_back(result);
    }
    
    // 检查所有结果
    for (const auto& result : results) {
        if (!result.success()) {
            std::cerr << "批量操作中有失败" << std::endl;
        }
    }
}
```

### 3. 连接复用

```cpp
class RdmaConnectionPool {
    std::vector<std::unique_ptr<CoroutineRdmaManager>> connections;
    
public:
    CoroutineRdmaManager* get_connection() {
        // 返回可用连接
    }
};
```

## 调试和故障排除

### 1. 日志输出

协程管理器内置了详细的日志输出：

```cpp
// 启用详细日志
std::cout << "CoroutineRdmaManager: State changed from " << old_state << " to " << new_state << std::endl;
```

### 2. 常见问题

**问题：协程提前退出**
```cpp
// 解决方案：正确等待协程完成
while (!task.done()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
}
```

**问题：内存访问错误**
```cpp
// 解决方案：确保缓冲区生命周期
std::vector<int> buffer(size);  // 确保在 co_await 期间有效
auto result = co_await manager.tag_recv(buffer.data(), buffer.size() * sizeof(int), tag);
```

**问题：连接超时**
```cpp
// 解决方案：检查网络和防火墙设置
// 确保 RDMA 设备正确配置
```

## 最佳实践

### 1. 资源管理

```cpp
// ✅ 使用 RAII
class RdmaSession {
    CoroutineRdmaManager manager;
public:
    RdmaSession() { manager.initialize(...); }
    ~RdmaSession() { manager.shutdown(); }
};
```

### 2. 异常安全

```cpp
// ✅ 使用异常处理
RdmaTask<void> safe_operation() {
    try {
        auto result = co_await manager.tag_send(...);
        if (!result.success()) {
            throw std::runtime_error("发送失败");
        }
    } catch (...) {
        // 清理资源
        manager.shutdown();
        throw;
    }
}
```

### 3. 性能监控

```cpp
// ✅ 监控性能指标
auto start = std::chrono::high_resolution_clock::now();
auto result = co_await manager.tag_send(...);
auto end = std::chrono::high_resolution_clock::now();

auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
std::cout << "操作耗时: " << duration.count() << " 微秒" << std::endl;
```

## 示例程序

项目中包含了完整的示例程序：

- `simple_coroutine_test.cpp`：基础功能测试
- `coroutine_tag_matching_example.cpp`：Tag matching 示例
- `coroutine_rdma_example.cpp`：完整功能演示

运行示例：

```bash
# 编译
make simple_coroutine_test

# 运行接收端
./build/bin/simple_coroutine_test receiver

# 运行发送端（另一个终端）
./build/bin/simple_coroutine_test sender
```

## 与其他框架集成

### 1. 与 Seastar 集成

```cpp
#include <seastar/core/future.hh>
#include "rpc/coroutine_rdma_manager.hpp"

class SeastarRdmaIntegration {
private:
    btsp::CoroutineRdmaManager rdma_manager;

public:
    seastar::future<> initialize_rdma() {
        return seastar::async([this] {
            if (!rdma_manager.initialize(false, 0)) {
                throw std::runtime_error("RDMA 初始化失败");
            }
        });
    }

    seastar::future<bool> send_data_async(void* data, size_t size, uint64_t tag) {
        return seastar::async([this, data, size, tag] {
            // 在 Seastar 异步上下文中运行协程
            auto task = this->send_coroutine(data, size, tag);

            // 等待协程完成
            while (!task.done()) {
                seastar::thread::maybe_yield();
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }

            try {
                task.get();
                return true;
            } catch (const std::exception& e) {
                seastar::print("RDMA 发送失败: %s\n", e.what());
                return false;
            }
        });
    }

private:
    btsp::RdmaTask<void> send_coroutine(void* data, size_t size, uint64_t tag) {
        auto result = co_await rdma_manager.tag_send(data, size, tag);
        if (!result.success()) {
            throw std::runtime_error("RDMA 发送操作失败");
        }
    }
};
```

### 2. 与 std::async 集成

```cpp
#include <future>
#include "rpc/coroutine_rdma_manager.hpp"

class AsyncRdmaWrapper {
private:
    btsp::CoroutineRdmaManager rdma_manager;

public:
    std::future<bool> send_data_future(void* data, size_t size, uint64_t tag) {
        return std::async(std::launch::async, [this, data, size, tag]() {
            auto task = this->send_coroutine(data, size, tag);

            // 等待协程完成
            while (!task.done()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }

            try {
                task.get();
                return true;
            } catch (const std::exception& e) {
                std::cerr << "RDMA 操作失败: " << e.what() << std::endl;
                return false;
            }
        });
    }

private:
    btsp::RdmaTask<void> send_coroutine(void* data, size_t size, uint64_t tag) {
        auto result = co_await rdma_manager.tag_send(data, size, tag);
        if (!result.success()) {
            throw std::runtime_error("发送失败");
        }
    }
};
```

## 高级协程模式

### 1. 协程管道 (Pipeline)

```cpp
RdmaTask<void> pipeline_example() {
    CoroutineRdmaManager manager;
    manager.initialize(false, 0);

    co_await manager.connect("127.0.0.1", 12345);

    // 管道式处理：接收 -> 处理 -> 发送
    for (int i = 0; i < 10; ++i) {
        // 接收原始数据
        std::vector<int> input_data(100, 0);
        auto recv_result = co_await manager.tag_recv(
            input_data.data(),
            input_data.size() * sizeof(int),
            100 + i
        );

        if (!recv_result.success()) continue;

        // 处理数据
        std::vector<int> processed_data;
        for (const auto& val : input_data) {
            processed_data.push_back(val * 2);  // 简单处理
        }

        // 发送处理后的数据
        auto send_result = co_await manager.tag_send(
            processed_data.data(),
            processed_data.size() * sizeof(int),
            200 + i
        );

        if (send_result.success()) {
            std::cout << "管道处理完成: " << i << std::endl;
        }
    }
}
```

### 2. 协程生产者-消费者模式

```cpp
#include <queue>
#include <mutex>

class CoroutineProducerConsumer {
private:
    btsp::CoroutineRdmaManager manager;
    std::queue<std::vector<int>> data_queue;
    std::mutex queue_mutex;
    std::atomic<bool> running{true};

public:
    btsp::RdmaTask<void> producer_coroutine() {
        while (running) {
            // 接收数据
            std::vector<int> buffer(50, 0);
            auto result = co_await manager.tag_recv(
                buffer.data(),
                buffer.size() * sizeof(int),
                1000
            );

            if (result.success()) {
                // 添加到队列
                std::lock_guard<std::mutex> lock(queue_mutex);
                data_queue.push(std::move(buffer));
                std::cout << "生产者: 接收到数据，队列大小: " << data_queue.size() << std::endl;
            }
        }
    }

    btsp::RdmaTask<void> consumer_coroutine() {
        while (running) {
            std::vector<int> data;

            // 从队列获取数据
            {
                std::lock_guard<std::mutex> lock(queue_mutex);
                if (!data_queue.empty()) {
                    data = std::move(data_queue.front());
                    data_queue.pop();
                }
            }

            if (!data.empty()) {
                // 处理并发送数据
                for (auto& val : data) {
                    val *= 3;  // 处理数据
                }

                auto result = co_await manager.tag_send(
                    data.data(),
                    data.size() * sizeof(int),
                    2000
                );

                if (result.success()) {
                    std::cout << "消费者: 发送处理后的数据" << std::endl;
                }
            } else {
                // 队列为空，短暂等待
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    }
};
```

### 3. 协程超时处理

```cpp
#include <chrono>

template<typename T>
btsp::RdmaTask<T> with_timeout(btsp::RdmaTask<T> task, std::chrono::milliseconds timeout) {
    auto start_time = std::chrono::steady_clock::now();

    while (!task.done()) {
        auto current_time = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - start_time);

        if (elapsed >= timeout) {
            throw std::runtime_error("协程操作超时");
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    co_return task.get();
}

// 使用示例
btsp::RdmaTask<void> timeout_example() {
    btsp::CoroutineRdmaManager manager;
    manager.initialize(false, 0);

    try {
        // 带超时的连接操作
        auto connect_task = manager.connect("127.0.0.1", 12345);
        co_await with_timeout(std::move(connect_task), std::chrono::seconds(5));

        std::cout << "连接成功" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "连接超时: " << e.what() << std::endl;
    }
}
```

## 性能调优指南

### 1. 内存池优化

```cpp
class RdmaMemoryPool {
private:
    struct MemoryBlock {
        std::unique_ptr<char[]> data;
        size_t size;
        bool in_use;
    };

    std::vector<MemoryBlock> blocks;
    std::mutex pool_mutex;

public:
    char* allocate(size_t size) {
        std::lock_guard<std::mutex> lock(pool_mutex);

        // 查找可用的块
        for (auto& block : blocks) {
            if (!block.in_use && block.size >= size) {
                block.in_use = true;
                return block.data.get();
            }
        }

        // 创建新块
        blocks.emplace_back();
        auto& new_block = blocks.back();
        new_block.data = std::make_unique<char[]>(size);
        new_block.size = size;
        new_block.in_use = true;

        return new_block.data.get();
    }

    void deallocate(char* ptr) {
        std::lock_guard<std::mutex> lock(pool_mutex);

        for (auto& block : blocks) {
            if (block.data.get() == ptr) {
                block.in_use = false;
                break;
            }
        }
    }
};
```

### 2. 批量操作优化

```cpp
btsp::RdmaTask<std::vector<btsp::RdmaOpResult>> batch_send(
    btsp::CoroutineRdmaManager& manager,
    const std::vector<std::pair<void*, size_t>>& data_list,
    uint64_t base_tag) {

    std::vector<btsp::RdmaOpResult> results;
    results.reserve(data_list.size());

    // 并发发送多个消息
    for (size_t i = 0; i < data_list.size(); ++i) {
        auto result = co_await manager.tag_send(
            data_list[i].first,
            data_list[i].second,
            base_tag + i
        );
        results.push_back(result);
    }

    co_return results;
}
```

### 3. 连接池管理

```cpp
class RdmaConnectionPool {
private:
    struct Connection {
        std::unique_ptr<btsp::CoroutineRdmaManager> manager;
        bool in_use;
        std::chrono::steady_clock::time_point last_used;
    };

    std::vector<Connection> connections;
    std::mutex pool_mutex;
    size_t max_connections;

public:
    RdmaConnectionPool(size_t max_conn = 10) : max_connections(max_conn) {}

    btsp::CoroutineRdmaManager* acquire_connection(const std::string& host, uint16_t port) {
        std::lock_guard<std::mutex> lock(pool_mutex);

        // 查找可用连接
        for (auto& conn : connections) {
            if (!conn.in_use && conn.manager->is_connected()) {
                conn.in_use = true;
                conn.last_used = std::chrono::steady_clock::now();
                return conn.manager.get();
            }
        }

        // 创建新连接
        if (connections.size() < max_connections) {
            connections.emplace_back();
            auto& new_conn = connections.back();
            new_conn.manager = std::make_unique<btsp::CoroutineRdmaManager>();
            new_conn.manager->initialize(false, 0);
            new_conn.in_use = true;
            new_conn.last_used = std::chrono::steady_clock::now();

            // 异步连接（这里简化处理）
            // 实际应用中需要协程化连接过程

            return new_conn.manager.get();
        }

        return nullptr;  // 连接池已满
    }

    void release_connection(btsp::CoroutineRdmaManager* manager) {
        std::lock_guard<std::mutex> lock(pool_mutex);

        for (auto& conn : connections) {
            if (conn.manager.get() == manager) {
                conn.in_use = false;
                break;
            }
        }
    }
};
```

## 监控和诊断

### 1. 性能指标收集

```cpp
class RdmaMetrics {
private:
    std::atomic<uint64_t> total_sends{0};
    std::atomic<uint64_t> total_receives{0};
    std::atomic<uint64_t> total_bytes_sent{0};
    std::atomic<uint64_t> total_bytes_received{0};
    std::atomic<uint64_t> failed_operations{0};

public:
    void record_send(size_t bytes, bool success) {
        total_sends++;
        if (success) {
            total_bytes_sent += bytes;
        } else {
            failed_operations++;
        }
    }

    void record_receive(size_t bytes, bool success) {
        total_receives++;
        if (success) {
            total_bytes_received += bytes;
        } else {
            failed_operations++;
        }
    }

    void print_stats() const {
        std::cout << "=== RDMA 性能统计 ===" << std::endl;
        std::cout << "总发送次数: " << total_sends.load() << std::endl;
        std::cout << "总接收次数: " << total_receives.load() << std::endl;
        std::cout << "总发送字节: " << total_bytes_sent.load() << std::endl;
        std::cout << "总接收字节: " << total_bytes_received.load() << std::endl;
        std::cout << "失败操作数: " << failed_operations.load() << std::endl;
    }
};
```

### 2. 协程状态监控

```cpp
class CoroutineMonitor {
private:
    std::map<std::string, std::chrono::steady_clock::time_point> coroutine_start_times;
    std::mutex monitor_mutex;

public:
    void start_monitoring(const std::string& coroutine_name) {
        std::lock_guard<std::mutex> lock(monitor_mutex);
        coroutine_start_times[coroutine_name] = std::chrono::steady_clock::now();
    }

    void end_monitoring(const std::string& coroutine_name) {
        std::lock_guard<std::mutex> lock(monitor_mutex);
        auto it = coroutine_start_times.find(coroutine_name);
        if (it != coroutine_start_times.end()) {
            auto duration = std::chrono::steady_clock::now() - it->second;
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
            std::cout << "协程 " << coroutine_name << " 执行时间: " << ms.count() << " ms" << std::endl;
            coroutine_start_times.erase(it);
        }
    }
};

// 使用 RAII 自动监控
class ScopedCoroutineMonitor {
private:
    CoroutineMonitor& monitor;
    std::string name;

public:
    ScopedCoroutineMonitor(CoroutineMonitor& m, const std::string& n)
        : monitor(m), name(n) {
        monitor.start_monitoring(name);
    }

    ~ScopedCoroutineMonitor() {
        monitor.end_monitoring(name);
    }
};

#define MONITOR_COROUTINE(monitor, name) \
    ScopedCoroutineMonitor _monitor(monitor, name)
```

这个协程 RDMA 管理器为高性能网络应用提供了现代化的异步编程接口，结合了 C++20 协程的简洁性和 RDMA 的高性能特性。通过合理使用这些高级特性和优化技术，可以构建出高效、可靠的分布式系统。
