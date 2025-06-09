#include "coroutine_rdma_manager.hpp"
#include <iostream>
#include <chrono>
#include <algorithm>
#include <cstring>

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
FastChannelManager::FastChannelManager() {
    std::cout << "FastChannelManager: Constructor called" << std::endl;
}

FastChannelManager::~FastChannelManager() {
    if (_running.load() && !_shutdown_requested.load()) {
        shutdown();
    }
}

bool FastChannelManager::initialize(bool server_mode, uint16_t port) {
    std::cout << "FastChannelManager: Initializing in " << (server_mode ? "server" : "client") << " mode";
    if (server_mode) {
        std::cout << " on port " << port;
    }
    std::cout << std::endl;
    
    _server_mode = server_mode;
    _port = port;
    
    try {
        _context = ucxx::createContext({}, ucxx::Context::defaultFeatureFlags);
        _worker = _context->createWorker();

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

    // Notify all waiting threads first
    _queue_cv.notify_all();

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
        std::lock_guard<std::mutex> lock(_queue_mutex);
        // TODO: lockfree queue

        // Check if queue is getting too large (memory pressure indicator)
        if (_coroutine_queue.size() > 100) {
            std::cout << "FastChannelManager: Queue size too large, rejecting operation" << std::endl;
            if (op.result_ptr) {
                *op.result_ptr = FCOpResult(FCResult::FAILURE, 0);
            }
            if (op.handle) {
                op.handle.resume();
            }
            return;
        }

        _coroutine_queue.push(std::move(op));
        _queue_cv.notify_one();
    } catch (const std::exception& e) {
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

bool initialize_global_coroutine_rdma_manager(bool server_mode, uint16_t port) {
    std::lock_guard<std::mutex> lock(g_coroutine_manager_mutex);
    if (g_coroutine_rdma_manager) {
        std::cout << "Global coroutine RDMA manager already initialized" << std::endl;
        return true;
    }
    
    g_coroutine_rdma_manager = std::make_unique<FastChannelManager>();
    return g_coroutine_rdma_manager->initialize(server_mode, port);
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
    while (!_shutdown_requested.load()) {
        std::unique_lock<std::mutex> lock(_queue_mutex);

        // Wait for operations or shutdown
        _queue_cv.wait(lock, [this] {
            return !_coroutine_queue.empty() || _shutdown_requested.load();
        });

        // Process all available operations
        while (!_coroutine_queue.empty() && !_shutdown_requested.load()) {
            CoroutineOperation op = std::move(_coroutine_queue.front());
            _coroutine_queue.pop();
            lock.unlock();

            process_coroutine_operation(op);

            lock.lock();
        }
    }

}

void FastChannelManager::state_machine_thread_func() {
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
        switch (op.type) {
            case FCOpType::TAG_SEND: {
                if (!_endpoint) {
                    std::cerr << "FastChannelManager: No endpoint available for tag send" << std::endl;
                    if (op.result_ptr) {
                        *op.result_ptr = FCOpResult(FCResult::DISCONNECTED, 0);
                    }
                    op.handle.resume();
                    return;
                }

                _active_send_request = _endpoint->tagSend(op.data, op.size, ucxx::Tag{op.tag});
                _active_send_op = const_cast<CoroutineOperation*>(&op);
                notify_state_change(FCState::SENDING);
                break;
            }

            case FCOpType::TAG_RECV: {
                if (!_endpoint) {
                    if (op.result_ptr) {
                        *op.result_ptr = FCOpResult(FCResult::DISCONNECTED, 0);
                    }
                    op.handle.resume();
                    return;
                }

                _active_recv_request = _endpoint->tagRecv(op.data, op.size, ucxx::Tag{op.tag}, ucxx::TagMaskFull);
                _active_recv_op = const_cast<CoroutineOperation*>(&op);
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

                op.handle.resume();
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
                op.handle.resume();
                break;
            }

            case FCOpType::SHUTDOWN: {
                notify_state_change(FCState::SHUTDOWN);
                if (op.result_ptr) {
                    *op.result_ptr = FCOpResult(FCResult::SUCCESS, 0);
                }
                op.handle.resume();
                break;
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "FastChannelManager: Error processing operation: " << e.what() << std::endl;
        if (op.result_ptr) {
            *op.result_ptr = FCOpResult(FCResult::FAILURE, 0);
        }
        op.handle.resume();
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
                _active_send_op->handle.resume();
            }
        } catch (const std::exception& e) {
            if (_active_send_op && _active_send_op->result_ptr) {
                *_active_send_op->result_ptr = FCOpResult(FCResult::FAILURE, 0);
            }
            if (_active_send_op) {
                _active_send_op->handle.resume();
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
                _active_recv_op->handle.resume();
            }
        } catch (const std::exception& e) {
            std::cerr << "FastChannelManager: Receive operation failed: " << e.what() << std::endl;
            if (_active_recv_op && _active_recv_op->result_ptr) {
                *_active_recv_op->result_ptr = FCOpResult(FCResult::FAILURE, 0);
            }
            if (_active_recv_op) {
                _active_recv_op->handle.resume();
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

} // namespace btsp
