#pragma once

#include <memory>
#include <ucxx/api.h>
#include <ucxx/worker.h>
#include <ucxx/endpoint.h>
#include <ucxx/buffer.h>
#include <ucxx/utils/sockaddr.h>
#include <ucxx/utils/ucx.h>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <functional>
#include <future>
#include <string>

namespace btsp {

// RDMA operation result
enum class RdmaResult {
    SUCCESS,
    FAILURE,
    TIMEOUT,
    DISCONNECTED
};

// RDMA operation types
enum class RdmaOpType {
    TAG_SEND,
    TAG_RECV,
    CONNECT,
    LISTEN,
    SHUTDOWN
};

// RDMA state machine states
enum class RdmaState {
    IDLE,
    LISTENING,
    CONNECTING,
    CONNECTED,
    SENDING,
    RECEIVING,
    ERROR,
    SHUTDOWN
};

// Callback function types
using RdmaCallback = std::function<void(RdmaResult result, size_t bytes_transferred)>;
using ConnectionCallback = std::function<void(RdmaResult result)>;
using StateChangeCallback = std::function<void(RdmaState old_state, RdmaState new_state)>;

// RDMA operation request (internal use)
struct RdmaOperation {
    RdmaOpType type;
    void* data;
    size_t size;
    uint64_t tag;
    std::string remote_addr;
    uint16_t remote_port;
    RdmaCallback callback;
    ConnectionCallback conn_callback;
    
    RdmaOperation(RdmaOpType t) : type(t), data(nullptr), size(0), tag(0), remote_port(0) {}
};

/**
 * @brief Independent RDMA Thread Manager
 * 
 * This class manages UCXX RDMA operations in separate threads without any dependency
 * on Seastar or other async frameworks. It uses callbacks for async operations.
 */
class IndependentRdmaManager {
public:
    IndependentRdmaManager();
    ~IndependentRdmaManager();

    // Initialize the RDMA manager
    bool initialize(bool server_mode = false, uint16_t port = 0);
    
    // Shutdown the RDMA manager
    void shutdown();

    // Async RDMA operations with callbacks
    bool submit_tag_send(void* data, size_t size, uint64_t tag, RdmaCallback callback = nullptr);
    bool submit_tag_recv(void* data, size_t size, uint64_t tag, RdmaCallback callback = nullptr);
    bool submit_connect(const std::string& remote_addr, uint16_t remote_port, ConnectionCallback callback = nullptr);
    bool submit_listen(uint16_t port, ConnectionCallback callback = nullptr);

    // Synchronous RDMA operations (blocking)
    RdmaResult tag_send_sync(void* data, size_t size, uint64_t tag, size_t* bytes_sent = nullptr, int timeout_ms = 5000);
    RdmaResult tag_recv_sync(void* data, size_t size, uint64_t tag, size_t* bytes_received = nullptr, int timeout_ms = 5000);
    RdmaResult connect_sync(const std::string& remote_addr, uint16_t remote_port, int timeout_ms = 5000);

    // State and status queries
    RdmaState get_state() const { return _current_state.load(); }
    bool is_connected() const { return _current_state.load() == RdmaState::CONNECTED; }
    bool is_running() const { return _running.load(); }

    // Set state change callback
    void set_state_change_callback(StateChangeCallback callback) { _state_change_callback = callback; }

    // Get UCXX resources (for advanced usage)
    std::shared_ptr<ucxx::Worker> get_worker() { return _worker; }
    std::shared_ptr<ucxx::Endpoint> get_endpoint() { return _endpoint; }

private:
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
    void process_operation(const RdmaOperation& op);
    void submit_operation(RdmaOperation op);
    
    // State change notification
    void notify_state_change(RdmaState new_state);
    
    // Listener callback (static)
    static void listener_callback(ucp_conn_request_h conn_request, void* arg);

    // UCXX resources
    std::shared_ptr<ucxx::Context> _context;
    std::shared_ptr<ucxx::Worker> _worker;
    std::shared_ptr<ucxx::Endpoint> _endpoint;
    std::shared_ptr<ucxx::Listener> _listener;

    // Threading
    std::thread _progress_thread;
    std::thread _request_thread;
    std::thread _state_machine_thread;
    
    // Operation queue
    std::queue<RdmaOperation> _operation_queue;
    std::mutex _queue_mutex;
    std::condition_variable _queue_cv;
    
    // State management
    std::atomic<RdmaState> _current_state{RdmaState::IDLE};
    std::atomic<bool> _shutdown_requested{false};
    std::atomic<bool> _running{false};
    
    // Configuration
    bool _server_mode{false};
    uint16_t _port{0};
    
    // Active operations tracking
    std::shared_ptr<ucxx::Request> _active_send_request;
    std::shared_ptr<ucxx::Request> _active_recv_request;
    RdmaCallback _active_send_callback;
    RdmaCallback _active_recv_callback;
    
    // Callbacks
    StateChangeCallback _state_change_callback;
    ConnectionCallback _pending_connection_callback;
    
    // Synchronous operation support
    struct SyncOperation {
        std::promise<RdmaResult> promise;
        std::atomic<size_t> bytes_transferred{0};
        std::atomic<bool> completed{false};
    };
    
    std::mutex _sync_ops_mutex;
    std::map<void*, std::shared_ptr<SyncOperation>> _sync_operations;
};

// Global instance management (optional)
IndependentRdmaManager& get_global_rdma_manager();
bool initialize_global_rdma_manager(bool server_mode = false, uint16_t port = 0);
void shutdown_global_rdma_manager();

} // namespace btsp
