#pragma once

#include <memory>
#include <ucxx/api.h>
#include <ucxx/worker.h>
#include <ucxx/endpoint.h>
#include <ucxx/buffer.h>
#include <ucxx/utils/sockaddr.h>
#include <ucxx/utils/ucx.h>
#include "pcq.hpp"
#include <mutex>
#include <thread>
#include <atomic>
#include <string>
#include <coroutine>
#include <optional>
#include <chrono>

namespace btsp {

// RDMA operation result
enum class FCResult {
    SUCCESS,
    FAILURE,
    TIMEOUT,
    DISCONNECTED
};

// RDMA operation types
enum class FCOpType {
    TAG_SEND,
    TAG_RECV,
    MEM_PUT,
    MEM_GET,
    CONNECT,
    LISTEN,
    SHUTDOWN
};

// RDMA state machine states
enum class FCState {
    IDLE,
    LISTENING,
    CONNECTING,
    CONNECTED,
    SENDING,
    RECEIVING,
    ERROR,
    SHUTDOWN
};

// Forward declaration
class FastChannelManager;

// UCX operation timing structure with complete call chain breakdown
struct UCXOperationTiming {
    // Legacy timing points for compatibility
    std::chrono::high_resolution_clock::time_point ucx_start;
    std::chrono::high_resolution_clock::time_point ucx_end;

    // Complete call chain timing points (tagSend to isCompleted path)
    std::chrono::high_resolution_clock::time_point queue_submit_time;           // When operation enters process_coroutine_operation
    std::chrono::high_resolution_clock::time_point ucx_call_start;             // Before _endpoint->tagSend()
    std::chrono::high_resolution_clock::time_point ucx_call_return;            // After _endpoint->tagSend()
    std::chrono::high_resolution_clock::time_point notify_state_change_start;  // Before notify_state_change(SENDING)
    std::chrono::high_resolution_clock::time_point notify_state_change_end;    // After notify_state_change(SENDING)
    std::chrono::high_resolution_clock::time_point state_exchange_start;       // Before _current_state.exchange()
    std::chrono::high_resolution_clock::time_point state_exchange_end;         // After _current_state.exchange()
    std::chrono::high_resolution_clock::time_point resume_waiters_start;       // Before resume_state_waiters()
    std::chrono::high_resolution_clock::time_point resume_waiters_end;         // After resume_state_waiters()
    std::chrono::high_resolution_clock::time_point state_machine_entry;        // When state machine enters handle_sending_state
    std::chrono::high_resolution_clock::time_point is_completed_check;         // When isCompleted() is called
    std::chrono::high_resolution_clock::time_point completion_detected;        // When isCompleted() returns true
    std::chrono::high_resolution_clock::time_point coroutine_resumed;          // When coroutine is resumed

    size_t bytes_transferred = 0;
    uint64_t tag = 0;
    bool completed = false;

    double duration_us() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(ucx_end - ucx_start).count();
    }

    double bandwidth_gbps() const {
        if (duration_us() <= 0 || bytes_transferred == 0) return 0.0;
        double duration_sec = duration_us() / 1e6;
        double bytes_per_sec = bytes_transferred / duration_sec;
        return bytes_per_sec / (1024.0 * 1024.0 * 1024.0);  // Convert to GB/s
    }

    // Detailed timing analysis functions
    double queue_overhead_us() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(ucx_call_start - queue_submit_time).count();
    }

    double ucx_call_overhead_us() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(ucx_call_return - ucx_call_start).count();
    }

    double notify_state_overhead_us() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(notify_state_change_end - notify_state_change_start).count();
    }

    double state_exchange_overhead_us() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(state_exchange_end - state_exchange_start).count();
    }

    double resume_waiters_overhead_us() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(resume_waiters_end - resume_waiters_start).count();
    }

    double state_machine_wait_us() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(state_machine_entry - notify_state_change_end).count();
    }

    double progress_wait_us() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(completion_detected - is_completed_check).count();
    }

    double resume_overhead_us() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(coroutine_resumed - completion_detected).count();
    }

    // Print detailed breakdown (implemented in .cpp file)
    void print_call_chain_breakdown() const;
};

// RDMA operation result with bytes transferred
struct FCOpResult {
    FCResult result;
    size_t bytes_transferred;
    
    FCOpResult(FCResult r = FCResult::FAILURE, size_t bytes = 0) 
        : result(r), bytes_transferred(bytes) {}
    
    bool success() const { return result == FCResult::SUCCESS; }
    operator bool() const { return success(); }
};

// Forward declaration
template<typename T>
class FCTask;

// Specialization for non-void types
template<typename T>
class FCTask {
public:
    struct promise_type {
        std::optional<T> value;
        std::exception_ptr exception;

        FCTask get_return_object() {
            return FCTask{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_never initial_suspend() { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }

        void unhandled_exception() {
            exception = std::current_exception();
        }

        void return_value(T val) {
            value = std::move(val);
        }
    };
    
    using handle_type = std::coroutine_handle<promise_type>;
    
    FCTask(handle_type h) : handle(h) {}
    
    ~FCTask() {
        // No need to destroy handle since final_suspend() returns suspend_never
        // The coroutine will auto-destroy when it completes
    }
    
    // Move-only type
    FCTask(const FCTask&) = delete;
    FCTask& operator=(const FCTask&) = delete;
    
    FCTask(FCTask&& other) noexcept : handle(other.handle) {
        other.handle = {};
    }
    
    FCTask& operator=(FCTask&& other) noexcept {
        if (this != &other) {
            // No need to destroy handle since final_suspend() returns suspend_never
            handle = other.handle;
            other.handle = {};
        }
        return *this;
    }
    
    T get() {
        if (!handle) {
            throw std::runtime_error("Invalid coroutine handle");
        }

        if (handle.promise().exception) {
            std::rethrow_exception(handle.promise().exception);
        }

        if (!handle.promise().value) {
            throw std::runtime_error("Coroutine has no return value");
        }
        return *handle.promise().value;
    }
    
    bool done() const {
        return handle && handle.done();
    }
    
private:
    handle_type handle;
};

// Specialization for void type
template<>
class FCTask<void> {
public:
    struct promise_type {
        std::exception_ptr exception;

        FCTask get_return_object() {
            return FCTask{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_never initial_suspend() { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }

        void unhandled_exception() {
            exception = std::current_exception();
        }

        void return_void() {}
    };

    using handle_type = std::coroutine_handle<promise_type>;

    FCTask(handle_type h) : handle(h) {}

    ~FCTask() {
        // No need to destroy handle since final_suspend() returns suspend_never
        // The coroutine will auto-destroy when it completes
    }

    // Move-only type
    FCTask(const FCTask&) = delete;
    FCTask& operator=(const FCTask&) = delete;

    FCTask(FCTask&& other) noexcept : handle(other.handle) {
        other.handle = {};
    }

    FCTask& operator=(FCTask&& other) noexcept {
        if (this != &other) {
            // No need to destroy handle since final_suspend() returns suspend_never
            handle = other.handle;
            other.handle = {};
        }
        return *this;
    }

    void get() {
        if (!handle) {
            throw std::runtime_error("Invalid coroutine handle");
        }

        if (handle.promise().exception) {
            std::rethrow_exception(handle.promise().exception);
        }
    }

    bool done() const {
        return handle && handle.done();
    }

private:
    handle_type handle;
};

// Awaitable for RDMA operations
class FCAwaitable {
public:
    FCAwaitable(FastChannelManager* manager, FCOpType type, void* data = nullptr,
                  size_t size = 0, uint64_t tag = 0, const std::string& addr = "", uint16_t port = 0);

    // Constructor for memory operations with remote key
    FCAwaitable(FastChannelManager* manager, FCOpType type, void* data, size_t size,
                  uint64_t remote_addr, std::shared_ptr<ucxx::RemoteKey> remote_key, uint64_t offset = 0);

    bool await_ready() const noexcept { return false; }

    void await_suspend(std::coroutine_handle<> handle);

    FCOpResult await_resume() const noexcept {
        return result;
    }

private:
    FastChannelManager* manager;
    FCOpType op_type;
    void* data;
    size_t size;
    uint64_t tag;
    std::string remote_addr;  // Store the string directly to avoid dangling pointer
    uint16_t remote_port;

    // Memory operation specific fields
    uint64_t remote_memory_addr;
    std::shared_ptr<ucxx::RemoteKey> remote_key;
    uint64_t remote_addr_offset;

    mutable FCOpResult result;
    std::coroutine_handle<> suspended_handle;

    friend class FastChannelManager;
};

// State change awaitable
class StateChangeAwaitable {
public:
    StateChangeAwaitable(FastChannelManager* manager, FCState target_state);
    
    bool await_ready() const noexcept;
    void await_suspend(std::coroutine_handle<> handle);
    FCState await_resume() const noexcept { return current_state; }
    
private:
    FastChannelManager* manager;
    FCState target_state;
    mutable FCState current_state;
    std::coroutine_handle<> suspended_handle;
    
    friend class FastChannelManager;
};

/**
 * @brief Coroutine-based RDMA Thread Manager
 * 
 * This class provides a coroutine-based interface for UCXX RDMA operations,
 * making async code look like synchronous code while maintaining performance.
 */
class FastChannelManager {
public:
    FastChannelManager();
    ~FastChannelManager();

    // Initialize the RDMA manager
    bool initialize(bool server_mode = false, uint16_t port = 0, int cuda_device_id = -1);
    
    // Shutdown the RDMA manager
    void shutdown();

    // Coroutine-based RDMA operations
    FCAwaitable tag_send(void* data, size_t size, uint64_t tag);
    FCAwaitable tag_recv(void* data, size_t size, uint64_t tag);

    // Memory operations
    FCAwaitable mem_put(void* local_data, size_t size, uint64_t remote_addr, std::shared_ptr<ucxx::RemoteKey> remote_key, uint64_t offset = 0);
    FCAwaitable mem_get(void* local_data, size_t size, uint64_t remote_addr, std::shared_ptr<ucxx::RemoteKey> remote_key, uint64_t offset = 0);

    FCAwaitable connect(const std::string& remote_addr, uint16_t remote_port);
    FCAwaitable listen(uint16_t port);
    
    // Wait for state change
    StateChangeAwaitable wait_for_state(FCState target_state);
    StateChangeAwaitable wait_for_connection();

    // State and status queries
    FCState get_state() const { return _current_state.load(); }
    bool is_connected() const { return _current_state.load() == FCState::CONNECTED; }
    bool is_running() const { return _running.load(); }

    // Get UCXX resources (for advanced usage)
    std::shared_ptr<ucxx::Worker> get_worker() { return _worker; }
    std::shared_ptr<ucxx::Endpoint> get_endpoint() { return _endpoint; }

    // Get UCX operation timing results (client-side only for bandwidth calculation)
    UCXOperationTiming get_last_send_timing() const { return _last_send_timing; }

private:
    friend class FCAwaitable;
    friend class StateChangeAwaitable;
    
    // Internal operation structure for coroutines
    struct CoroutineOperation {
        FCOpType type;
        void* data;
        size_t size;
        uint64_t tag;
        std::string remote_addr;
        uint16_t remote_port;
        std::coroutine_handle<> handle;
        FCOpResult* result_ptr;

        // Memory operation specific fields
        uint64_t remote_memory_addr;
        std::shared_ptr<ucxx::RemoteKey> remote_key;
        uint64_t remote_addr_offset;

        CoroutineOperation(FCOpType t) : type(t), data(nullptr), size(0), tag(0), remote_port(0),
                                       result_ptr(nullptr), remote_memory_addr(0), remote_addr_offset(0) {}
    };
    
    // State change waiter
    struct StateWaiter {
        FCState target_state;
        std::coroutine_handle<> handle;
        FCState* result_ptr;
    };
    
    // Thread functions
    void progress_thread_func();
    void request_thread_func();
    void state_machine_thread_func();
    
    // State machine handlers
    void handle_idle_state();
    void handle_listening_state();
    void handle_connecting_state();
    void handle_connected_state();
    void handle_sending_state();
    void handle_receiving_state();
    void handle_error_state();
    
    // Request processing
    void process_coroutine_operation(const CoroutineOperation& op);
    void submit_coroutine_operation(CoroutineOperation op);

    // State change notification
    void notify_state_change(FCState new_state);
    void resume_state_waiters(FCState new_state);

    // Coroutine safety
    void safe_resume_coroutine(std::coroutine_handle<> handle);
    
    // Listener callback (static)
    static void listener_callback(ucp_conn_request_h conn_request, void* arg);

    // CUDA context initialization
    bool initialize_cuda_context();
    void setup_cuda_context_for_thread();

    // UCXX resources
    std::shared_ptr<ucxx::Context> _context;
    std::shared_ptr<ucxx::Worker> _worker;
    std::shared_ptr<ucxx::Endpoint> _endpoint;
    std::shared_ptr<ucxx::Listener> _listener;

    // Threading
    std::thread _progress_thread;
    std::thread _request_thread;
    std::thread _state_machine_thread;
    
    // Operation queue for coroutines
    BlockingQueue<CoroutineOperation> _coroutine_queue;
    
    // State waiters
    std::vector<StateWaiter> _state_waiters;
    std::mutex _state_waiters_mutex;
    
    // State management
    std::atomic<FCState> _current_state{FCState::IDLE};
    std::atomic<bool> _shutdown_requested{false};
    std::atomic<bool> _running{false};
    
    // Configuration
    bool _server_mode{false};
    uint16_t _port{0};
    int _cuda_device_id{0};
    
    // Active operations tracking
    std::shared_ptr<ucxx::Request> _active_send_request;
    std::shared_ptr<ucxx::Request> _active_recv_request;
    std::shared_ptr<ucxx::Request> _active_mem_put_request;
    std::shared_ptr<ucxx::Request> _active_mem_get_request;
    CoroutineOperation* _active_send_op{nullptr};
    CoroutineOperation* _active_recv_op{nullptr};
    CoroutineOperation* _active_mem_put_op{nullptr};
    CoroutineOperation* _active_mem_get_op{nullptr};

    // Storage for active operations to avoid dangling pointers
    CoroutineOperation _active_send_op_storage;
    CoroutineOperation _active_recv_op_storage;
    CoroutineOperation _active_mem_put_op_storage;
    CoroutineOperation _active_mem_get_op_storage;

    // UCX operation timing storage (client-side only for bandwidth calculation)
    UCXOperationTiming _last_send_timing;
};

// Global instance management (optional)
FastChannelManager& get_global_coroutine_rdma_manager();
bool initialize_global_coroutine_rdma_manager(bool server_mode = false, uint16_t port = 0, int cuda_device_id = -1);
void shutdown_global_coroutine_rdma_manager();

} // namespace btsp
