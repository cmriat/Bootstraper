#include "fast_channel_manager.hpp"
#include <iostream>
#include <chrono>
#include <algorithm>
#include <cstring>

#ifdef CUDA_AVAILABLE
#include <cuda_runtime.h>
#endif

namespace btsp {

// Global instance
static std::unique_ptr<FastChannelManager> g_coroutine_rdma_manager;
static std::mutex g_coroutine_manager_mutex;


FCAwaitable::FCAwaitable(FastChannelManager* mgr, FCOpType type, void* data,
                             size_t size, uint64_t tag, const std::string& addr, uint16_t port)
    : manager(mgr), op_type(type), data(data), size(size), tag(tag), remote_port(port) {
        try {
            if (!addr.empty()) {
                remote_addr = addr;
            } else {
                remote_addr = "127.0.0.1";
            }
        } catch (const std::bad_alloc& e) {
            remote_addr = "127.0.0.1";  // Fallback
        }
}

void FCAwaitable::await_suspend(std::coroutine_handle<> handle) {
    suspended_handle = handle;

    try {
        FastChannelManager::CoroutineOperation op(op_type);
        op.data = data;
        op.size = size;
        op.tag = tag;

        try {
            if (!remote_addr.empty()) {
                op.remote_addr = remote_addr;
            } else {
                op.remote_addr = "127.0.0.1";
            }
        } catch (const std::bad_alloc& e) {
            op.remote_addr = "127.0.0.1";  // Fallback for localhost
        }

        op.remote_port = remote_port;
        op.handle = handle;
        op.result_ptr = &result;

        manager->submit_coroutine_operation(std::move(op));
    } catch (const std::exception& e) {
        // Set error result and resume immediately
        result = FCOpResult(FCResult::FAILURE, 0);
        handle.resume();
    }
}

// StateChangeAwaitable implementation
StateChangeAwaitable::StateChangeAwaitable(FastChannelManager* mgr, FCState target)
    : manager(mgr), target_state(target) {
}

bool StateChangeAwaitable::await_ready() const noexcept {
    current_state = manager->get_state();
    return current_state == target_state;
}

void StateChangeAwaitable::await_suspend(std::coroutine_handle<> handle) {
    suspended_handle = handle;
    
    std::lock_guard<std::mutex> lock(manager->_state_waiters_mutex);
    manager->_state_waiters.push_back({target_state, handle, &current_state});
}

// FastChannelManager implementation
FastChannelManager::FastChannelManager()
    : _coroutine_queue(1024), _active_send_op_storage(FCOpType::TAG_SEND), _active_recv_op_storage(FCOpType::TAG_RECV) {
    std::cout << "FastChannelManager: Constructor called" << std::endl;
}

FastChannelManager::~FastChannelManager() {
    if (_running.load() && !_shutdown_requested.load()) {
        shutdown();
    }
}

bool FastChannelManager::initialize(bool server_mode, uint16_t port, int cuda_device_id) {
    std::cout << "FastChannelManager: Initializing in " << (server_mode ? "server" : "client") << " mode";
    if (server_mode) {
        std::cout << " on port " << port;
    }
    if (cuda_device_id >= 0) {
        std::cout << " with CUDA device " << cuda_device_id;
    }
    std::cout << std::endl;

    _server_mode = server_mode;
    _port = port;
    _cuda_device_id = cuda_device_id;

    // Initialize CUDA device first to establish CUDA context
    if (!initialize_cuda_context()) {
        std::cerr << "FastChannelManager: Failed to initialize CUDA context" << std::endl;
        return false;
    }

    try {
        ucxx::ConfigMap config = {
            {"TLS", "all"}
        };
        _context = ucxx::createContext(config, ucxx::Context::defaultFeatureFlags);
        _worker = _context->createWorker();
        if (_context->hasCudaSupport()) {
            std::cout << "CUDA IPC support is available" << std::endl;
        }
        // Start threads
        _running = true;
        _shutdown_requested = false;

        _progress_thread = std::thread(&FastChannelManager::progress_thread_func, this);
        _request_thread = std::thread(&FastChannelManager::request_thread_func, this);
        _state_machine_thread = std::thread(&FastChannelManager::state_machine_thread_func, this);

        // If server mode, start listening
        if (server_mode && port > 0) {
            _listener = _worker->createListener(port, listener_callback, this);
            notify_state_change(FCState::LISTENING);
        }
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "FastChannelManager: Initialization failed: " << e.what() << std::endl;
        return false;
    }
}

void FastChannelManager::shutdown() {
    // Prevent double shutdown
    bool expected = false;
    if (!_shutdown_requested.compare_exchange_strong(expected, true)) {
        std::cout << "FastChannelManager: Shutdown already in progress" << std::endl;
        return;
    }

    notify_state_change(FCState::SHUTDOWN);

    // Join threads before resuming coroutines to avoid race conditions
    if (_progress_thread.joinable()) {
        _progress_thread.join();
        std::cout << "FastChannelManager: Progress thread joined" << std::endl;
    }

    if (_request_thread.joinable()) {
        _request_thread.join();
        std::cout << "FastChannelManager: Request thread joined" << std::endl;
    }

    if (_state_machine_thread.joinable()) {
        _state_machine_thread.join();
        std::cout << "FastChannelManager: State machine thread joined" << std::endl;
    }

    // Clean up UCXX resources before resuming coroutines
    _active_send_request.reset();
    _active_recv_request.reset();
    _endpoint.reset();
    _listener.reset();
    _worker.reset();
    _context.reset();

    // Resume all waiting coroutines with failure - do this last to avoid use-after-free
    {
        std::lock_guard<std::mutex> lock(_state_waiters_mutex);
        for (auto& waiter : _state_waiters) {
            try {
                if (waiter.result_ptr) {
                    *waiter.result_ptr = FCState::SHUTDOWN;
                }
                // Only resume if the handle is still valid
                if (waiter.handle) {
                    waiter.handle.resume();
                }
            } catch (const std::exception& e) {
                std::cerr << "FastChannelManager: Error resuming coroutine during shutdown: " << e.what() << std::endl;
            }
        }
        _state_waiters.clear();
    }

    _running = false;
    std::cout << "FastChannelManager: Shutdown complete" << std::endl;
}

FCAwaitable FastChannelManager::tag_send(void* data, size_t size, uint64_t tag) {
    return FCAwaitable(this, FCOpType::TAG_SEND, data, size, tag);
}

FCAwaitable FastChannelManager::tag_recv(void* data, size_t size, uint64_t tag) {
    return FCAwaitable(this, FCOpType::TAG_RECV, data, size, tag);
}

FCAwaitable FastChannelManager::connect(const std::string& remote_addr, uint16_t remote_port) {
    try {
        FCAwaitable awaitable(this, FCOpType::CONNECT, nullptr, 0, 0, remote_addr, remote_port);
        return awaitable;
    } catch (const std::exception& e) {
        std::cout << "FastChannelManager: Exception creating FCAwaitable: " << e.what() << std::endl;
        throw;
    }
}

FCAwaitable FastChannelManager::listen(uint16_t port) {
    return FCAwaitable(this, FCOpType::LISTEN, nullptr, 0, 0, "", port);
}

StateChangeAwaitable FastChannelManager::wait_for_state(FCState target_state) {
    return StateChangeAwaitable(this, target_state);
}

StateChangeAwaitable FastChannelManager::wait_for_connection() {
    return StateChangeAwaitable(this, FCState::CONNECTED);
}

void FastChannelManager::submit_coroutine_operation(CoroutineOperation op) {
    try {
        // BlockingQueue handles synchronization and blocking internally
        // If queue is full, this will block until space is available
        if (!_coroutine_queue.write(std::move(op))) {
            std::cout << "FastChannelManager: Failed to write to queue" << std::endl;
            if (op.result_ptr) {
                *op.result_ptr = FCOpResult(FCResult::FAILURE, 0);
            }
            if (op.handle) {
                op.handle.resume();
            }
        }
    } catch (const std::exception& e) {
        std::cout << "FastChannelManager: Exception in submit_coroutine_operation: " << e.what() << std::endl;
        if (op.result_ptr) {
            *op.result_ptr = FCOpResult(FCResult::FAILURE, 0);
        }
        if (op.handle) {
            op.handle.resume();
        }
    }
}

void FastChannelManager::notify_state_change(FCState new_state) {
    FCState old_state = _current_state.exchange(new_state);
    if (old_state != new_state) {
        std::cout << "FastChannelManager: State changed from " << static_cast<int>(old_state) 
                  << " to " << static_cast<int>(new_state) << std::endl;
        
        resume_state_waiters(new_state);
    }
}

void FastChannelManager::resume_state_waiters(FCState new_state) {
    std::lock_guard<std::mutex> lock(_state_waiters_mutex);
    
    auto it = std::remove_if(_state_waiters.begin(), _state_waiters.end(),
        [new_state](StateWaiter& waiter) {
            if (waiter.target_state == new_state || new_state == FCState::SHUTDOWN) {
                if (waiter.result_ptr) {
                    *waiter.result_ptr = new_state;
                }
                waiter.handle.resume();
                return true;  // Remove this waiter
            }
            return false;  // Keep this waiter
        });
    
    _state_waiters.erase(it, _state_waiters.end());
}

// Listener callback implementation
void FastChannelManager::listener_callback(ucp_conn_request_h conn_request, void* arg) {
    char ip_str[INET6_ADDRSTRLEN];
    char port_str[INET6_ADDRSTRLEN];
    ucp_conn_request_attr_t attr{};
    FastChannelManager* manager = reinterpret_cast<FastChannelManager*>(arg);

    attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
    ucxx::utils::ucsErrorThrow(ucp_conn_request_query(conn_request, &attr));
    ucxx::utils::sockaddr_get_ip_port_str(&attr.client_address, ip_str, port_str, INET6_ADDRSTRLEN);
    
    try {
        // Create endpoint from connection request
        manager->_endpoint = manager->_listener->createEndpointFromConnRequest(conn_request, true);
        manager->notify_state_change(FCState::CONNECTED);
    } catch (const std::exception& e) {
        std::cerr << "FastChannelManager: error accepting connection: " << e.what() << std::endl;
    }
}

// Global instance management
FastChannelManager& get_global_coroutine_rdma_manager() {
    std::lock_guard<std::mutex> lock(g_coroutine_manager_mutex);
    if (!g_coroutine_rdma_manager) {
        throw std::runtime_error("Global coroutine RDMA manager not initialized");
    }
    return *g_coroutine_rdma_manager;
}

bool initialize_global_coroutine_rdma_manager(bool server_mode, uint16_t port, int cuda_device_id) {
    std::lock_guard<std::mutex> lock(g_coroutine_manager_mutex);
    if (g_coroutine_rdma_manager) {
        std::cout << "Global coroutine RDMA manager already initialized" << std::endl;
        return true;
    }

    g_coroutine_rdma_manager = std::make_unique<FastChannelManager>();
    return g_coroutine_rdma_manager->initialize(server_mode, port, cuda_device_id);
}

void shutdown_global_coroutine_rdma_manager() {
    std::lock_guard<std::mutex> lock(g_coroutine_manager_mutex);
    if (g_coroutine_rdma_manager) {
        g_coroutine_rdma_manager->shutdown();
        g_coroutine_rdma_manager.reset();
    }
}

// Thread function implementations
void FastChannelManager::progress_thread_func() {
    // Set up CUDA context for this thread
    setup_cuda_context_for_thread();

    while (!_shutdown_requested.load()) {
        if (_worker) {
            try {
                _worker->progress();
            } catch (const std::exception& e) {
                std::cerr << "FastChannelManager: Progress thread error: " << e.what() << std::endl;
            }
        }

        // Small sleep to avoid busy waiting
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

void FastChannelManager::request_thread_func() {
    // Set up CUDA context for this thread
    setup_cuda_context_for_thread();

    while (!_shutdown_requested.load()) {
        try {
            CoroutineOperation op(FCOpType::TAG_SEND); // Default initialization
            // BlockingQueue::read() will block until an operation is available
            if (_coroutine_queue.read(op)) {
                // Check shutdown again after reading
                if (!_shutdown_requested.load()) {
                    process_coroutine_operation(op);
                }
            }
        } catch (const std::exception& e) {
            std::cout << "FastChannelManager: Exception in request_thread_func: " << e.what() << std::endl;
            // Continue processing other operations
        }
    }
}

void FastChannelManager::state_machine_thread_func() {
    // Set up CUDA context for this thread
    setup_cuda_context_for_thread();

    while (!_shutdown_requested.load()) {
        FCState current_state = _current_state.load();

        switch (current_state) {
            case FCState::IDLE:
                handle_idle_state();
                break;
            case FCState::LISTENING:
                handle_listening_state();
                break;
            case FCState::CONNECTING:
                handle_connecting_state();
                break;
            case FCState::CONNECTED:
                handle_connected_state();
                break;
            case FCState::SENDING:
                handle_sending_state();
                break;
            case FCState::RECEIVING:
                handle_receiving_state();
                break;
            case FCState::ERROR:
                handle_error_state();
                break;
            case FCState::SHUTDOWN:
                std::cout << "FastChannelManager: State machine entering shutdown state" << std::endl;
                return;
        }

        // Small sleep to avoid busy waiting
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    std::cout << "FastChannelManager: State machine thread exiting" << std::endl;
}

void FastChannelManager::process_coroutine_operation(const CoroutineOperation& op) {
    try {
        // Validate coroutine handle before processing
        if (!op.handle || op.handle.done()) {
            std::cerr << "FastChannelManager: Invalid or completed coroutine handle" << std::endl;
            return;
        }

        switch (op.type) {
            case FCOpType::TAG_SEND: {
                if (!_endpoint) {
                    std::cerr << "FastChannelManager: No endpoint available for tag send" << std::endl;
                    if (op.result_ptr) {
                        *op.result_ptr = FCOpResult(FCResult::DISCONNECTED, 0);
                    }
                    // Safe resume with validation
                    safe_resume_coroutine(op.handle);
                    return;
                }

                _active_send_request = _endpoint->tagSend(op.data, op.size, ucxx::Tag{op.tag});
                // Store a copy of the operation to avoid dangling pointer
                _active_send_op_storage = op;
                _active_send_op = &_active_send_op_storage;
                notify_state_change(FCState::SENDING);
                break;
            }

            case FCOpType::TAG_RECV: {
                if (!_endpoint) {
                    if (op.result_ptr) {
                        *op.result_ptr = FCOpResult(FCResult::DISCONNECTED, 0);
                    }
                    safe_resume_coroutine(op.handle);
                    return;
                }

                _active_recv_request = _endpoint->tagRecv(op.data, op.size, ucxx::Tag{op.tag}, ucxx::TagMaskFull);
                // Store a copy of the operation to avoid dangling pointer
                _active_recv_op_storage = op;
                _active_recv_op = &_active_recv_op_storage;
                notify_state_change(FCState::RECEIVING);
                break;
            }

            case FCOpType::CONNECT: {
                notify_state_change(FCState::CONNECTING);

                try {
                    // Check if we already have an endpoint to avoid multiple connections
                    if (_endpoint) {
                        notify_state_change(FCState::CONNECTED);
                        if (op.result_ptr) {
                            *op.result_ptr = FCOpResult(FCResult::SUCCESS, 0);
                        }
                    } else {
                        // Add a small delay to reduce memory pressure
                        std::this_thread::sleep_for(std::chrono::milliseconds(100));

                        _endpoint = _worker->createEndpointFromHostname(op.remote_addr.c_str(), op.remote_port, true);
                        notify_state_change(FCState::CONNECTED);

                        if (op.result_ptr) {
                            *op.result_ptr = FCOpResult(FCResult::SUCCESS, 0);
                        }
                    }
                } catch (const std::exception& e) {
                    std::cerr << "FastChannelManager: Connect failed: " << e.what() << std::endl;
                    notify_state_change(FCState::ERROR);
                    if (op.result_ptr) {
                        *op.result_ptr = FCOpResult(FCResult::FAILURE, 0);
                    }
                }

                safe_resume_coroutine(op.handle);
                break;
            }

            case FCOpType::LISTEN: {
                if (!_listener) {
                    _listener = _worker->createListener(op.remote_port, listener_callback, this);
                }
                notify_state_change(FCState::LISTENING);

                if (op.result_ptr) {
                    *op.result_ptr = FCOpResult(FCResult::SUCCESS, 0);
                }
                safe_resume_coroutine(op.handle);
                break;
            }

            case FCOpType::SHUTDOWN: {
                notify_state_change(FCState::SHUTDOWN);
                if (op.result_ptr) {
                    *op.result_ptr = FCOpResult(FCResult::SUCCESS, 0);
                }
                safe_resume_coroutine(op.handle);
                break;
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "FastChannelManager: Error processing operation: " << e.what() << std::endl;
        if (op.result_ptr) {
            *op.result_ptr = FCOpResult(FCResult::FAILURE, 0);
        }
        safe_resume_coroutine(op.handle);
    }
}

// State machine handlers
void FastChannelManager::handle_idle_state() {
    // Nothing to do in idle state
}

void FastChannelManager::handle_listening_state() {
    // Listening is handled by the listener callback
}

void FastChannelManager::handle_connecting_state() {
    // Connection is handled synchronously in process_coroutine_operation
}

void FastChannelManager::handle_connected_state() {
    // Nothing specific to do, just maintain the connection
}

void FastChannelManager::handle_sending_state() {
    if (_active_send_request && _active_send_request->isCompleted()) {
        try {
            _active_send_request->checkError();
            if (_active_send_op && _active_send_op->result_ptr) {
                *_active_send_op->result_ptr = FCOpResult(FCResult::SUCCESS, _active_send_op->size);
            }
            if (_active_send_op) {
                safe_resume_coroutine(_active_send_op->handle);
            }
        } catch (const std::exception& e) {
            if (_active_send_op && _active_send_op->result_ptr) {
                *_active_send_op->result_ptr = FCOpResult(FCResult::FAILURE, 0);
            }
            if (_active_send_op) {
                safe_resume_coroutine(_active_send_op->handle);
            }
        }

        _active_send_request.reset();
        _active_send_op = nullptr;
        notify_state_change(FCState::CONNECTED);
    }
}

void FastChannelManager::handle_receiving_state() {
    if (_active_recv_request && _active_recv_request->isCompleted()) {
        try {
            _active_recv_request->checkError();
            if (_active_recv_op && _active_recv_op->result_ptr) {
                *_active_recv_op->result_ptr = FCOpResult(FCResult::SUCCESS, _active_recv_op->size);
            }
            if (_active_recv_op) {
                safe_resume_coroutine(_active_recv_op->handle);
            }
        } catch (const std::exception& e) {
            std::cerr << "FastChannelManager: Receive operation failed: " << e.what() << std::endl;
            if (_active_recv_op && _active_recv_op->result_ptr) {
                *_active_recv_op->result_ptr = FCOpResult(FCResult::FAILURE, 0);
            }
            if (_active_recv_op) {
                safe_resume_coroutine(_active_recv_op->handle);
            }
        }

        _active_recv_request.reset();
        _active_recv_op = nullptr;
        notify_state_change(FCState::CONNECTED);
    }
}

void FastChannelManager::handle_error_state() {
    std::cerr << "FastChannelManager: In error state, attempting recovery" << std::endl;
    // Could implement error recovery logic here
    notify_state_change(FCState::IDLE);
}

void FastChannelManager::safe_resume_coroutine(std::coroutine_handle<> handle) {
    try {
        // Check if handle is valid and not already completed
        if (handle && !handle.done()) {
            handle.resume();
        } else {
            std::cerr << "FastChannelManager: Attempted to resume invalid or completed coroutine" << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "FastChannelManager: Exception during coroutine resume: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "FastChannelManager: Unknown exception during coroutine resume" << std::endl;
    }
}

// CUDA context initialization functions
bool FastChannelManager::initialize_cuda_context() {
#ifdef CUDA_AVAILABLE
    try {
        // Get device count
        int device_count = 0;
        cudaError_t err = cudaGetDeviceCount(&device_count);
        if (err != cudaSuccess) {
            std::cerr << "FastChannelManager: Failed to get CUDA device count: "
                      << cudaGetErrorString(err) << std::endl;
            return false;
        }

        if (device_count == 0) {
            std::cerr << "FastChannelManager: No CUDA devices found" << std::endl;
            return false;
        }

        std::cout << "FastChannelManager: Found " << device_count << " CUDA device(s)" << std::endl;

        // Determine which device to use
        if (_cuda_device_id == -1) {
            // Use device 0 by default, or get from environment variable
            _cuda_device_id = 0;
            const char* cuda_device_env = std::getenv("CUDA_VISIBLE_DEVICES");
            if (cuda_device_env) {
                try {
                    _cuda_device_id = std::stoi(cuda_device_env);
                } catch (const std::exception& e) {
                    std::cerr << "FastChannelManager: Invalid CUDA_VISIBLE_DEVICES value, using device 0" << std::endl;
                    _cuda_device_id = 0;
                }
            }
        }

        // Validate device ID
        if (_cuda_device_id < 0 || _cuda_device_id >= device_count) {
            std::cerr << "FastChannelManager: Invalid CUDA device ID " << _cuda_device_id
                      << ", available devices: 0-" << (device_count - 1) << std::endl;
            return false;
        }

        // Set the CUDA device
        err = cudaSetDevice(_cuda_device_id);
        if (err != cudaSuccess) {
            std::cerr << "FastChannelManager: Failed to set CUDA device " << _cuda_device_id
                      << ": " << cudaGetErrorString(err) << std::endl;
            return false;
        }

        // Get device properties for information
        cudaDeviceProp prop;
        err = cudaGetDeviceProperties(&prop, _cuda_device_id);
        if (err == cudaSuccess) {
            std::cout << "FastChannelManager: Using CUDA device " << _cuda_device_id
                      << ": " << prop.name << " (Compute Capability: "
                      << prop.major << "." << prop.minor << ")" << std::endl;
        }

        // Initialize CUDA context by allocating and freeing a small amount of memory
        void* dummy_ptr = nullptr;
        err = cudaMalloc(&dummy_ptr, 1);
        if (err != cudaSuccess) {
            std::cerr << "FastChannelManager: Failed to initialize CUDA context: "
                      << cudaGetErrorString(err) << std::endl;
            return false;
        }
        cudaFree(dummy_ptr);

        std::cout << "FastChannelManager: Successfully initialized CUDA context on device "
                  << _cuda_device_id << std::endl;
        return true;

    } catch (const std::exception& e) {
        std::cerr << "FastChannelManager: Exception during CUDA initialization: " << e.what() << std::endl;
        return false;
    }
#else
    std::cout << "FastChannelManager: CUDA not available, skipping CUDA context initialization" << std::endl;
    return true;  // Not an error if CUDA is not available
#endif
}

void FastChannelManager::setup_cuda_context_for_thread() {
#ifdef CUDA_AVAILABLE
    try {
        // Set the CUDA device for this thread
        cudaError_t err = cudaSetDevice(_cuda_device_id);
        if (err != cudaSuccess) {
            std::cerr << "FastChannelManager: Failed to set CUDA device " << _cuda_device_id
                      << " for thread: " << cudaGetErrorString(err) << std::endl;
            return;
        }

        // Initialize CUDA context for this thread by allocating and freeing a small amount of memory
        void* dummy_ptr = nullptr;
        err = cudaMalloc(&dummy_ptr, 1);
        if (err != cudaSuccess) {
            std::cerr << "FastChannelManager: Failed to initialize CUDA context for thread: "
                      << cudaGetErrorString(err) << std::endl;
            return;
        }
        cudaFree(dummy_ptr);

        std::cout << "FastChannelManager: Successfully set up CUDA context for thread on device "
                  << _cuda_device_id << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "FastChannelManager: Exception during thread CUDA setup: " << e.what() << std::endl;
    }
#endif
}

} // namespace btsp
