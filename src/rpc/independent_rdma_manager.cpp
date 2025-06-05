#include "rpc/independent_rdma_manager.hpp"
#include <iostream>
#include <chrono>
#include <map>

namespace btsp {

// Global instance
static std::unique_ptr<IndependentRdmaManager> g_rdma_manager;
static std::mutex g_manager_mutex;

// Listener callback implementation
void IndependentRdmaManager::listener_callback(ucp_conn_request_h conn_request, void* arg) {
    char ip_str[INET6_ADDRSTRLEN];
    char port_str[INET6_ADDRSTRLEN];
    ucp_conn_request_attr_t attr{};
    IndependentRdmaManager* manager = reinterpret_cast<IndependentRdmaManager*>(arg);

    attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
    ucxx::utils::ucsErrorThrow(ucp_conn_request_query(conn_request, &attr));
    ucxx::utils::sockaddr_get_ip_port_str(&attr.client_address, ip_str, port_str, INET6_ADDRSTRLEN);
    
    std::cout << "IndependentRdmaManager: received connection request from " << ip_str << ":" << port_str << std::endl;
    
    try {
        // Create endpoint from connection request
        manager->_endpoint = manager->_listener->createEndpointFromConnRequest(conn_request, true);
        manager->notify_state_change(RdmaState::CONNECTED);
        
        // Notify connection callback if set
        if (manager->_pending_connection_callback) {
            manager->_pending_connection_callback(RdmaResult::SUCCESS);
            manager->_pending_connection_callback = nullptr;
        }
        
        std::cout << "IndependentRdmaManager: connection established" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "IndependentRdmaManager: error accepting connection: " << e.what() << std::endl;
        if (manager->_pending_connection_callback) {
            manager->_pending_connection_callback(RdmaResult::FAILURE);
            manager->_pending_connection_callback = nullptr;
        }
    }
}

IndependentRdmaManager::IndependentRdmaManager() {
    std::cout << "IndependentRdmaManager: Constructor called" << std::endl;
}

IndependentRdmaManager::~IndependentRdmaManager() {
    std::cout << "IndependentRdmaManager: Destructor called" << std::endl;
    if (_running.load()) {
        shutdown();
    }
}

bool IndependentRdmaManager::initialize(bool server_mode, uint16_t port) {
    std::cout << "IndependentRdmaManager: Initializing in " << (server_mode ? "server" : "client") << " mode";
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
        
        std::cout << "IndependentRdmaManager: UCXX context and worker created" << std::endl;
        
        // Start threads
        _running = true;
        _shutdown_requested = false;
        
        _progress_thread = std::thread(&IndependentRdmaManager::progress_thread_func, this);
        _request_thread = std::thread(&IndependentRdmaManager::request_thread_func, this);
        _state_machine_thread = std::thread(&IndependentRdmaManager::state_machine_thread_func, this);
        
        std::cout << "IndependentRdmaManager: All threads started" << std::endl;
        
        // If server mode, start listening
        if (server_mode && port > 0) {
            _listener = _worker->createListener(port, listener_callback, this);
            notify_state_change(RdmaState::LISTENING);
            std::cout << "IndependentRdmaManager: Listening on port " << port << std::endl;
        }
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "IndependentRdmaManager: Initialization failed: " << e.what() << std::endl;
        return false;
    }
}

void IndependentRdmaManager::shutdown() {
    std::cout << "IndependentRdmaManager: Shutdown requested" << std::endl;
    
    _shutdown_requested = true;
    notify_state_change(RdmaState::SHUTDOWN);
    
    // Notify all waiting threads
    _queue_cv.notify_all();
    
    // Join threads
    if (_progress_thread.joinable()) {
        _progress_thread.join();
        std::cout << "IndependentRdmaManager: Progress thread joined" << std::endl;
    }
    
    if (_request_thread.joinable()) {
        _request_thread.join();
        std::cout << "IndependentRdmaManager: Request thread joined" << std::endl;
    }
    
    if (_state_machine_thread.joinable()) {
        _state_machine_thread.join();
        std::cout << "IndependentRdmaManager: State machine thread joined" << std::endl;
    }
    
    // Clean up UCXX resources
    _active_send_request.reset();
    _active_recv_request.reset();
    _endpoint.reset();
    _listener.reset();
    _worker.reset();
    _context.reset();
    
    _running = false;
    std::cout << "IndependentRdmaManager: Shutdown complete" << std::endl;
}

bool IndependentRdmaManager::submit_tag_send(void* data, size_t size, uint64_t tag, RdmaCallback callback) {
    if (!_running.load() || !is_connected()) {
        if (callback) callback(RdmaResult::DISCONNECTED, 0);
        return false;
    }
    
    RdmaOperation op(RdmaOpType::TAG_SEND);
    op.data = data;
    op.size = size;
    op.tag = tag;
    op.callback = callback;
    
    submit_operation(std::move(op));
    return true;
}

bool IndependentRdmaManager::submit_tag_recv(void* data, size_t size, uint64_t tag, RdmaCallback callback) {
    if (!_running.load() || !is_connected()) {
        if (callback) callback(RdmaResult::DISCONNECTED, 0);
        return false;
    }
    
    RdmaOperation op(RdmaOpType::TAG_RECV);
    op.data = data;
    op.size = size;
    op.tag = tag;
    op.callback = callback;
    
    submit_operation(std::move(op));
    return true;
}

bool IndependentRdmaManager::submit_connect(const std::string& remote_addr, uint16_t remote_port, ConnectionCallback callback) {
    if (!_running.load() || _server_mode) {
        if (callback) callback(RdmaResult::FAILURE);
        return false;
    }
    
    RdmaOperation op(RdmaOpType::CONNECT);
    op.remote_addr = remote_addr;
    op.remote_port = remote_port;
    op.conn_callback = callback;
    
    submit_operation(std::move(op));
    return true;
}

bool IndependentRdmaManager::submit_listen(uint16_t port, ConnectionCallback callback) {
    if (!_running.load() || !_server_mode) {
        if (callback) callback(RdmaResult::FAILURE);
        return false;
    }
    
    _pending_connection_callback = callback;
    return true;
}

RdmaResult IndependentRdmaManager::tag_send_sync(void* data, size_t size, uint64_t tag, size_t* bytes_sent, int timeout_ms) {
    auto sync_op = std::make_shared<SyncOperation>();
    
    {
        std::lock_guard<std::mutex> lock(_sync_ops_mutex);
        _sync_operations[data] = sync_op;
    }
    
    bool submitted = submit_tag_send(data, size, tag, [this, data, sync_op](RdmaResult result, size_t bytes) {
        sync_op->bytes_transferred = bytes;
        sync_op->promise.set_value(result);
        sync_op->completed = true;
        
        std::lock_guard<std::mutex> lock(_sync_ops_mutex);
        _sync_operations.erase(data);
    });
    
    if (!submitted) {
        std::lock_guard<std::mutex> lock(_sync_ops_mutex);
        _sync_operations.erase(data);
        return RdmaResult::FAILURE;
    }
    
    auto future = sync_op->promise.get_future();
    auto status = future.wait_for(std::chrono::milliseconds(timeout_ms));
    
    if (status == std::future_status::timeout) {
        std::lock_guard<std::mutex> lock(_sync_ops_mutex);
        _sync_operations.erase(data);
        return RdmaResult::TIMEOUT;
    }
    
    if (bytes_sent) {
        *bytes_sent = sync_op->bytes_transferred.load();
    }
    
    return future.get();
}

RdmaResult IndependentRdmaManager::tag_recv_sync(void* data, size_t size, uint64_t tag, size_t* bytes_received, int timeout_ms) {
    auto sync_op = std::make_shared<SyncOperation>();
    
    {
        std::lock_guard<std::mutex> lock(_sync_ops_mutex);
        _sync_operations[data] = sync_op;
    }
    
    bool submitted = submit_tag_recv(data, size, tag, [this, data, sync_op](RdmaResult result, size_t bytes) {
        sync_op->bytes_transferred = bytes;
        sync_op->promise.set_value(result);
        sync_op->completed = true;
        
        std::lock_guard<std::mutex> lock(_sync_ops_mutex);
        _sync_operations.erase(data);
    });
    
    if (!submitted) {
        std::lock_guard<std::mutex> lock(_sync_ops_mutex);
        _sync_operations.erase(data);
        return RdmaResult::FAILURE;
    }
    
    auto future = sync_op->promise.get_future();
    auto status = future.wait_for(std::chrono::milliseconds(timeout_ms));
    
    if (status == std::future_status::timeout) {
        std::lock_guard<std::mutex> lock(_sync_ops_mutex);
        _sync_operations.erase(data);
        return RdmaResult::TIMEOUT;
    }
    
    if (bytes_received) {
        *bytes_received = sync_op->bytes_transferred.load();
    }
    
    return future.get();
}

RdmaResult IndependentRdmaManager::connect_sync(const std::string& remote_addr, uint16_t remote_port, int timeout_ms) {
    auto sync_op = std::make_shared<SyncOperation>();
    
    bool submitted = submit_connect(remote_addr, remote_port, [sync_op](RdmaResult result) {
        sync_op->promise.set_value(result);
        sync_op->completed = true;
    });
    
    if (!submitted) {
        return RdmaResult::FAILURE;
    }
    
    auto future = sync_op->promise.get_future();
    auto status = future.wait_for(std::chrono::milliseconds(timeout_ms));
    
    if (status == std::future_status::timeout) {
        return RdmaResult::TIMEOUT;
    }
    
    return future.get();
}

void IndependentRdmaManager::submit_operation(RdmaOperation op) {
    std::lock_guard<std::mutex> lock(_queue_mutex);
    _operation_queue.push(std::move(op));
    _queue_cv.notify_one();
}

void IndependentRdmaManager::notify_state_change(RdmaState new_state) {
    RdmaState old_state = _current_state.exchange(new_state);
    if (old_state != new_state) {
        std::cout << "IndependentRdmaManager: State changed from " << static_cast<int>(old_state) 
                  << " to " << static_cast<int>(new_state) << std::endl;
        
        if (_state_change_callback) {
            _state_change_callback(old_state, new_state);
        }
    }
}

// Global instance management
IndependentRdmaManager& get_global_rdma_manager() {
    std::lock_guard<std::mutex> lock(g_manager_mutex);
    if (!g_rdma_manager) {
        throw std::runtime_error("Global RDMA manager not initialized");
    }
    return *g_rdma_manager;
}

bool initialize_global_rdma_manager(bool server_mode, uint16_t port) {
    std::lock_guard<std::mutex> lock(g_manager_mutex);
    if (g_rdma_manager) {
        std::cout << "Global RDMA manager already initialized" << std::endl;
        return true;
    }
    
    g_rdma_manager = std::make_unique<IndependentRdmaManager>();
    return g_rdma_manager->initialize(server_mode, port);
}

void shutdown_global_rdma_manager() {
    std::lock_guard<std::mutex> lock(g_manager_mutex);
    if (g_rdma_manager) {
        g_rdma_manager->shutdown();
        g_rdma_manager.reset();
    }
}

// Thread function implementations
void IndependentRdmaManager::progress_thread_func() {
    std::cout << "IndependentRdmaManager: Progress thread started" << std::endl;

    while (!_shutdown_requested.load()) {
        if (_worker) {
            try {
                _worker->progress();
            } catch (const std::exception& e) {
                std::cerr << "IndependentRdmaManager: Progress thread error: " << e.what() << std::endl;
            }
        }

        // Small sleep to avoid busy waiting
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    std::cout << "IndependentRdmaManager: Progress thread exiting" << std::endl;
}

void IndependentRdmaManager::request_thread_func() {
    std::cout << "IndependentRdmaManager: Request thread started" << std::endl;

    while (!_shutdown_requested.load()) {
        std::unique_lock<std::mutex> lock(_queue_mutex);

        // Wait for operations or shutdown
        _queue_cv.wait(lock, [this] {
            return !_operation_queue.empty() || _shutdown_requested.load();
        });

        // Process all available operations
        while (!_operation_queue.empty() && !_shutdown_requested.load()) {
            RdmaOperation op = std::move(_operation_queue.front());
            _operation_queue.pop();
            lock.unlock();

            process_operation(op);

            lock.lock();
        }
    }

    std::cout << "IndependentRdmaManager: Request thread exiting" << std::endl;
}

void IndependentRdmaManager::state_machine_thread_func() {
    std::cout << "IndependentRdmaManager: State machine thread started" << std::endl;

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
                std::cout << "IndependentRdmaManager: State machine entering shutdown state" << std::endl;
                return;
        }

        // Small sleep to avoid busy waiting
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    std::cout << "IndependentRdmaManager: State machine thread exiting" << std::endl;
}

void IndependentRdmaManager::process_operation(const RdmaOperation& op) {
    try {
        switch (op.type) {
            case RdmaOpType::TAG_SEND: {
                if (!_endpoint) {
                    std::cerr << "IndependentRdmaManager: No endpoint available for tag send" << std::endl;
                    if (op.callback) op.callback(RdmaResult::DISCONNECTED, 0);
                    return;
                }

                std::cout << "IndependentRdmaManager: Processing tag send, size=" << op.size << ", tag=" << op.tag << std::endl;
                _active_send_request = _endpoint->tagSend(op.data, op.size, ucxx::Tag{op.tag});
                _active_send_callback = op.callback;
                notify_state_change(RdmaState::SENDING);
                break;
            }

            case RdmaOpType::TAG_RECV: {
                if (!_endpoint) {
                    std::cerr << "IndependentRdmaManager: No endpoint available for tag recv" << std::endl;
                    if (op.callback) op.callback(RdmaResult::DISCONNECTED, 0);
                    return;
                }

                std::cout << "IndependentRdmaManager: Processing tag recv, size=" << op.size << ", tag=" << op.tag << std::endl;
                _active_recv_request = _endpoint->tagRecv(op.data, op.size, ucxx::Tag{op.tag}, ucxx::TagMaskFull);
                _active_recv_callback = op.callback;
                notify_state_change(RdmaState::RECEIVING);
                break;
            }

            case RdmaOpType::CONNECT: {
                std::cout << "IndependentRdmaManager: Processing connect to " << op.remote_addr << ":" << op.remote_port << std::endl;
                notify_state_change(RdmaState::CONNECTING);
                _endpoint = _worker->createEndpointFromHostname(op.remote_addr.c_str(), op.remote_port, true);
                notify_state_change(RdmaState::CONNECTED);

                if (op.conn_callback) op.conn_callback(RdmaResult::SUCCESS);
                break;
            }

            case RdmaOpType::LISTEN: {
                std::cout << "IndependentRdmaManager: Processing listen on port " << op.remote_port << std::endl;
                if (!_listener) {
                    _listener = _worker->createListener(op.remote_port, listener_callback, this);
                }
                notify_state_change(RdmaState::LISTENING);

                if (op.conn_callback) op.conn_callback(RdmaResult::SUCCESS);
                break;
            }

            case RdmaOpType::SHUTDOWN: {
                std::cout << "IndependentRdmaManager: Processing shutdown" << std::endl;
                notify_state_change(RdmaState::SHUTDOWN);
                break;
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "IndependentRdmaManager: Error processing operation: " << e.what() << std::endl;
        if (op.callback) op.callback(RdmaResult::FAILURE, 0);
        if (op.conn_callback) op.conn_callback(RdmaResult::FAILURE);
    }
}

// State machine handlers
void IndependentRdmaManager::handle_idle_state() {
    // Nothing to do in idle state
}

void IndependentRdmaManager::handle_listening_state() {
    // Listening is handled by the listener callback
}

void IndependentRdmaManager::handle_connecting_state() {
    // Connection is handled synchronously in process_operation
}

void IndependentRdmaManager::handle_connected_state() {
    // Nothing specific to do, just maintain the connection
}

void IndependentRdmaManager::handle_sending_state() {
    if (_active_send_request && _active_send_request->isCompleted()) {
        try {
            _active_send_request->checkError();
            std::cout << "IndependentRdmaManager: Send operation completed successfully" << std::endl;
            if (_active_send_callback) {
                // Note: UCXX Request doesn't provide getSize(), so we pass 0 for now
                // In a real implementation, you'd track the size separately
                _active_send_callback(RdmaResult::SUCCESS, 0);
            }
        } catch (const std::exception& e) {
            std::cerr << "IndependentRdmaManager: Send operation failed: " << e.what() << std::endl;
            if (_active_send_callback) {
                _active_send_callback(RdmaResult::FAILURE, 0);
            }
        }

        _active_send_request.reset();
        _active_send_callback = nullptr;
        notify_state_change(RdmaState::CONNECTED);
    }
}

void IndependentRdmaManager::handle_receiving_state() {
    if (_active_recv_request && _active_recv_request->isCompleted()) {
        try {
            _active_recv_request->checkError();
            std::cout << "IndependentRdmaManager: Receive operation completed successfully" << std::endl;
            if (_active_recv_callback) {
                // Note: UCXX Request doesn't provide getSize(), so we pass 0 for now
                // In a real implementation, you'd track the size separately
                _active_recv_callback(RdmaResult::SUCCESS, 0);
            }
        } catch (const std::exception& e) {
            std::cerr << "IndependentRdmaManager: Receive operation failed: " << e.what() << std::endl;
            if (_active_recv_callback) {
                _active_recv_callback(RdmaResult::FAILURE, 0);
            }
        }

        _active_recv_request.reset();
        _active_recv_callback = nullptr;
        notify_state_change(RdmaState::CONNECTED);
    }
}

void IndependentRdmaManager::handle_error_state() {
    std::cerr << "IndependentRdmaManager: In error state, attempting recovery" << std::endl;
    // Could implement error recovery logic here
    notify_state_change(RdmaState::IDLE);
}

} // namespace btsp
