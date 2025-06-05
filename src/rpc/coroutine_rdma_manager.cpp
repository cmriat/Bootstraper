#include "rpc/coroutine_rdma_manager.hpp"
#include <iostream>
#include <chrono>
#include <algorithm>

namespace btsp {

// Global instance
static std::unique_ptr<CoroutineRdmaManager> g_coroutine_rdma_manager;
static std::mutex g_coroutine_manager_mutex;

// RdmaAwaitable implementation
RdmaAwaitable::RdmaAwaitable(CoroutineRdmaManager* mgr, RdmaOpType type, void* data, 
                             size_t size, uint64_t tag, const std::string& addr, uint16_t port)
    : manager(mgr), op_type(type), data(data), size(size), tag(tag), remote_addr(addr), remote_port(port) {
}

void RdmaAwaitable::await_suspend(std::coroutine_handle<> handle) {
    suspended_handle = handle;
    
    CoroutineRdmaManager::CoroutineOperation op(op_type);
    op.data = data;
    op.size = size;
    op.tag = tag;
    op.remote_addr = remote_addr;
    op.remote_port = remote_port;
    op.handle = handle;
    op.result_ptr = &result;
    
    manager->submit_coroutine_operation(std::move(op));
}

// StateChangeAwaitable implementation
StateChangeAwaitable::StateChangeAwaitable(CoroutineRdmaManager* mgr, RdmaState target)
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

// CoroutineRdmaManager implementation
CoroutineRdmaManager::CoroutineRdmaManager() {
    std::cout << "CoroutineRdmaManager: Constructor called" << std::endl;
}

CoroutineRdmaManager::~CoroutineRdmaManager() {
    std::cout << "CoroutineRdmaManager: Destructor called" << std::endl;
    if (_running.load() && !_shutdown_requested.load()) {
        shutdown();
    }
}

bool CoroutineRdmaManager::initialize(bool server_mode, uint16_t port) {
    std::cout << "CoroutineRdmaManager: Initializing in " << (server_mode ? "server" : "client") << " mode";
    if (server_mode) {
        std::cout << " on port " << port;
    }
    std::cout << std::endl;
    
    _server_mode = server_mode;
    _port = port;
    
    try {
        // Create UCXX context and worker
        _context = ucxx::createContext({}, ucxx::Context::defaultFeatureFlags);
        _worker = _context->createWorker();
        
        std::cout << "CoroutineRdmaManager: UCXX context and worker created" << std::endl;
        
        // Start threads
        _running = true;
        _shutdown_requested = false;
        
        _progress_thread = std::thread(&CoroutineRdmaManager::progress_thread_func, this);
        _request_thread = std::thread(&CoroutineRdmaManager::request_thread_func, this);
        _state_machine_thread = std::thread(&CoroutineRdmaManager::state_machine_thread_func, this);
        
        std::cout << "CoroutineRdmaManager: All threads started" << std::endl;
        
        // If server mode, start listening
        if (server_mode && port > 0) {
            _listener = _worker->createListener(port, listener_callback, this);
            notify_state_change(RdmaState::LISTENING);
            std::cout << "CoroutineRdmaManager: Listening on port " << port << std::endl;
        }
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "CoroutineRdmaManager: Initialization failed: " << e.what() << std::endl;
        return false;
    }
}

void CoroutineRdmaManager::shutdown() {
    std::cout << "CoroutineRdmaManager: Shutdown requested" << std::endl;
    
    _shutdown_requested = true;
    notify_state_change(RdmaState::SHUTDOWN);
    
    // Resume all waiting coroutines with failure
    {
        std::lock_guard<std::mutex> lock(_state_waiters_mutex);
        for (auto& waiter : _state_waiters) {
            if (waiter.result_ptr) {
                *waiter.result_ptr = RdmaState::SHUTDOWN;
            }
            waiter.handle.resume();
        }
        _state_waiters.clear();
    }
    
    // Notify all waiting threads
    _queue_cv.notify_all();
    
    // Join threads
    if (_progress_thread.joinable()) {
        _progress_thread.join();
        std::cout << "CoroutineRdmaManager: Progress thread joined" << std::endl;
    }
    
    if (_request_thread.joinable()) {
        _request_thread.join();
        std::cout << "CoroutineRdmaManager: Request thread joined" << std::endl;
    }
    
    if (_state_machine_thread.joinable()) {
        _state_machine_thread.join();
        std::cout << "CoroutineRdmaManager: State machine thread joined" << std::endl;
    }
    
    // Clean up UCXX resources
    _active_send_request.reset();
    _active_recv_request.reset();
    _endpoint.reset();
    _listener.reset();
    _worker.reset();
    _context.reset();
    
    _running = false;
    std::cout << "CoroutineRdmaManager: Shutdown complete" << std::endl;
}

RdmaAwaitable CoroutineRdmaManager::tag_send(void* data, size_t size, uint64_t tag) {
    return RdmaAwaitable(this, RdmaOpType::TAG_SEND, data, size, tag);
}

RdmaAwaitable CoroutineRdmaManager::tag_recv(void* data, size_t size, uint64_t tag) {
    return RdmaAwaitable(this, RdmaOpType::TAG_RECV, data, size, tag);
}

RdmaAwaitable CoroutineRdmaManager::connect(const std::string& remote_addr, uint16_t remote_port) {
    return RdmaAwaitable(this, RdmaOpType::CONNECT, nullptr, 0, 0, remote_addr, remote_port);
}

RdmaAwaitable CoroutineRdmaManager::listen(uint16_t port) {
    return RdmaAwaitable(this, RdmaOpType::LISTEN, nullptr, 0, 0, "", port);
}

StateChangeAwaitable CoroutineRdmaManager::wait_for_state(RdmaState target_state) {
    return StateChangeAwaitable(this, target_state);
}

StateChangeAwaitable CoroutineRdmaManager::wait_for_connection() {
    return StateChangeAwaitable(this, RdmaState::CONNECTED);
}

void CoroutineRdmaManager::submit_coroutine_operation(CoroutineOperation op) {
    std::lock_guard<std::mutex> lock(_queue_mutex);
    // TODO: lockfree queue
    _coroutine_queue.push(std::move(op));
    _queue_cv.notify_one();
}

void CoroutineRdmaManager::notify_state_change(RdmaState new_state) {
    RdmaState old_state = _current_state.exchange(new_state);
    if (old_state != new_state) {
        std::cout << "CoroutineRdmaManager: State changed from " << static_cast<int>(old_state) 
                  << " to " << static_cast<int>(new_state) << std::endl;
        
        resume_state_waiters(new_state);
    }
}

void CoroutineRdmaManager::resume_state_waiters(RdmaState new_state) {
    std::lock_guard<std::mutex> lock(_state_waiters_mutex);
    
    auto it = std::remove_if(_state_waiters.begin(), _state_waiters.end(),
        [new_state](StateWaiter& waiter) {
            if (waiter.target_state == new_state || new_state == RdmaState::SHUTDOWN) {
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
void CoroutineRdmaManager::listener_callback(ucp_conn_request_h conn_request, void* arg) {
    char ip_str[INET6_ADDRSTRLEN];
    char port_str[INET6_ADDRSTRLEN];
    ucp_conn_request_attr_t attr{};
    CoroutineRdmaManager* manager = reinterpret_cast<CoroutineRdmaManager*>(arg);

    attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
    ucxx::utils::ucsErrorThrow(ucp_conn_request_query(conn_request, &attr));
    ucxx::utils::sockaddr_get_ip_port_str(&attr.client_address, ip_str, port_str, INET6_ADDRSTRLEN);
    
    std::cout << "CoroutineRdmaManager: received connection request from " << ip_str << ":" << port_str << std::endl;
    
    try {
        // Create endpoint from connection request
        manager->_endpoint = manager->_listener->createEndpointFromConnRequest(conn_request, true);
        manager->notify_state_change(RdmaState::CONNECTED);
        
        std::cout << "CoroutineRdmaManager: connection established" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "CoroutineRdmaManager: error accepting connection: " << e.what() << std::endl;
    }
}

// Global instance management
CoroutineRdmaManager& get_global_coroutine_rdma_manager() {
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
    
    g_coroutine_rdma_manager = std::make_unique<CoroutineRdmaManager>();
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
void CoroutineRdmaManager::progress_thread_func() {
    std::cout << "CoroutineRdmaManager: Progress thread started" << std::endl;

    while (!_shutdown_requested.load()) {
        if (_worker) {
            try {
                _worker->progress();
            } catch (const std::exception& e) {
                std::cerr << "CoroutineRdmaManager: Progress thread error: " << e.what() << std::endl;
            }
        }

        // Small sleep to avoid busy waiting
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    std::cout << "CoroutineRdmaManager: Progress thread exiting" << std::endl;
}

void CoroutineRdmaManager::request_thread_func() {
    std::cout << "CoroutineRdmaManager: Request thread started" << std::endl;

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

    std::cout << "CoroutineRdmaManager: Request thread exiting" << std::endl;
}

void CoroutineRdmaManager::state_machine_thread_func() {
    std::cout << "CoroutineRdmaManager: State machine thread started" << std::endl;

    while (!_shutdown_requested.load()) {
        RdmaState current_state = _current_state.load();

        switch (current_state) {
            case RdmaState::IDLE:
                handle_idle_state();
                break;
            case RdmaState::LISTENING:
                handle_listening_state();
                break;
            case RdmaState::CONNECTING:
                handle_connecting_state();
                break;
            case RdmaState::CONNECTED:
                handle_connected_state();
                break;
            case RdmaState::SENDING:
                handle_sending_state();
                break;
            case RdmaState::RECEIVING:
                handle_receiving_state();
                break;
            case RdmaState::ERROR:
                handle_error_state();
                break;
            case RdmaState::SHUTDOWN:
                std::cout << "CoroutineRdmaManager: State machine entering shutdown state" << std::endl;
                return;
        }

        // Small sleep to avoid busy waiting
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    std::cout << "CoroutineRdmaManager: State machine thread exiting" << std::endl;
}

void CoroutineRdmaManager::process_coroutine_operation(const CoroutineOperation& op) {
    try {
        switch (op.type) {
            case RdmaOpType::TAG_SEND: {
                if (!_endpoint) {
                    std::cerr << "CoroutineRdmaManager: No endpoint available for tag send" << std::endl;
                    if (op.result_ptr) {
                        *op.result_ptr = RdmaOpResult(RdmaResult::DISCONNECTED, 0);
                    }
                    op.handle.resume();
                    return;
                }

                std::cout << "CoroutineRdmaManager: Processing tag send, size=" << op.size << ", tag=" << op.tag << std::endl;
                _active_send_request = _endpoint->tagSend(op.data, op.size, ucxx::Tag{op.tag});
                _active_send_op = const_cast<CoroutineOperation*>(&op);
                notify_state_change(RdmaState::SENDING);
                break;
            }

            case RdmaOpType::TAG_RECV: {
                if (!_endpoint) {
                    std::cerr << "CoroutineRdmaManager: No endpoint available for tag recv" << std::endl;
                    if (op.result_ptr) {
                        *op.result_ptr = RdmaOpResult(RdmaResult::DISCONNECTED, 0);
                    }
                    op.handle.resume();
                    return;
                }

                std::cout << "CoroutineRdmaManager: Processing tag recv, size=" << op.size << ", tag=" << op.tag << std::endl;
                _active_recv_request = _endpoint->tagRecv(op.data, op.size, ucxx::Tag{op.tag}, ucxx::TagMaskFull);
                _active_recv_op = const_cast<CoroutineOperation*>(&op);
                notify_state_change(RdmaState::RECEIVING);
                break;
            }

            case RdmaOpType::CONNECT: {
                std::cout << "CoroutineRdmaManager: Processing connect to " << op.remote_addr << ":" << op.remote_port << std::endl;
                notify_state_change(RdmaState::CONNECTING);
                _endpoint = _worker->createEndpointFromHostname(op.remote_addr.c_str(), op.remote_port, true);
                notify_state_change(RdmaState::CONNECTED);

                if (op.result_ptr) {
                    *op.result_ptr = RdmaOpResult(RdmaResult::SUCCESS, 0);
                }
                op.handle.resume();
                break;
            }

            case RdmaOpType::LISTEN: {
                std::cout << "CoroutineRdmaManager: Processing listen on port " << op.remote_port << std::endl;
                if (!_listener) {
                    _listener = _worker->createListener(op.remote_port, listener_callback, this);
                }
                notify_state_change(RdmaState::LISTENING);

                if (op.result_ptr) {
                    *op.result_ptr = RdmaOpResult(RdmaResult::SUCCESS, 0);
                }
                op.handle.resume();
                break;
            }

            case RdmaOpType::SHUTDOWN: {
                std::cout << "CoroutineRdmaManager: Processing shutdown" << std::endl;
                notify_state_change(RdmaState::SHUTDOWN);
                if (op.result_ptr) {
                    *op.result_ptr = RdmaOpResult(RdmaResult::SUCCESS, 0);
                }
                op.handle.resume();
                break;
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "CoroutineRdmaManager: Error processing operation: " << e.what() << std::endl;
        if (op.result_ptr) {
            *op.result_ptr = RdmaOpResult(RdmaResult::FAILURE, 0);
        }
        op.handle.resume();
    }
}

// State machine handlers
void CoroutineRdmaManager::handle_idle_state() {
    // Nothing to do in idle state
}

void CoroutineRdmaManager::handle_listening_state() {
    // Listening is handled by the listener callback
}

void CoroutineRdmaManager::handle_connecting_state() {
    // Connection is handled synchronously in process_coroutine_operation
}

void CoroutineRdmaManager::handle_connected_state() {
    // Nothing specific to do, just maintain the connection
}

void CoroutineRdmaManager::handle_sending_state() {
    if (_active_send_request && _active_send_request->isCompleted()) {
        try {
            _active_send_request->checkError();
            std::cout << "CoroutineRdmaManager: Send operation completed successfully" << std::endl;
            if (_active_send_op && _active_send_op->result_ptr) {
                *_active_send_op->result_ptr = RdmaOpResult(RdmaResult::SUCCESS, _active_send_op->size);
            }
            if (_active_send_op) {
                _active_send_op->handle.resume();
            }
        } catch (const std::exception& e) {
            std::cerr << "CoroutineRdmaManager: Send operation failed: " << e.what() << std::endl;
            if (_active_send_op && _active_send_op->result_ptr) {
                *_active_send_op->result_ptr = RdmaOpResult(RdmaResult::FAILURE, 0);
            }
            if (_active_send_op) {
                _active_send_op->handle.resume();
            }
        }

        _active_send_request.reset();
        _active_send_op = nullptr;
        notify_state_change(RdmaState::CONNECTED);
    }
}

void CoroutineRdmaManager::handle_receiving_state() {
    if (_active_recv_request && _active_recv_request->isCompleted()) {
        try {
            _active_recv_request->checkError();
            std::cout << "CoroutineRdmaManager: Receive operation completed successfully" << std::endl;
            if (_active_recv_op && _active_recv_op->result_ptr) {
                *_active_recv_op->result_ptr = RdmaOpResult(RdmaResult::SUCCESS, _active_recv_op->size);
            }
            if (_active_recv_op) {
                _active_recv_op->handle.resume();
            }
        } catch (const std::exception& e) {
            std::cerr << "CoroutineRdmaManager: Receive operation failed: " << e.what() << std::endl;
            if (_active_recv_op && _active_recv_op->result_ptr) {
                *_active_recv_op->result_ptr = RdmaOpResult(RdmaResult::FAILURE, 0);
            }
            if (_active_recv_op) {
                _active_recv_op->handle.resume();
            }
        }

        _active_recv_request.reset();
        _active_recv_op = nullptr;
        notify_state_change(RdmaState::CONNECTED);
    }
}

void CoroutineRdmaManager::handle_error_state() {
    std::cerr << "CoroutineRdmaManager: In error state, attempting recovery" << std::endl;
    // Could implement error recovery logic here
    notify_state_change(RdmaState::IDLE);
}

} // namespace btsp
