#include "rpc/tensor_transfer_backend.hpp"
#include <iostream>
#include <chrono>
#include <seastar/core/future-util.hh>

namespace btsp {

// Global listener callback for RDMA thread manager
static void rdma_listener_cb(ucp_conn_request_h conn_request, void* arg) {
    char ip_str[INET6_ADDRSTRLEN];
    char port_str[INET6_ADDRSTRLEN];
    ucp_conn_request_attr_t attr{};
    RdmaThreadManager* manager = reinterpret_cast<RdmaThreadManager*>(arg);

    attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
    ucxx::utils::ucsErrorThrow(ucp_conn_request_query(conn_request, &attr));
    ucxx::utils::sockaddr_get_ip_port_str(&attr.client_address, ip_str, port_str, INET6_ADDRSTRLEN);
    
    std::cout << "RDMA Thread Manager: received connection request from " << ip_str << ":" << port_str << std::endl;
    
    // Create endpoint from connection request
    try {
        // This will be handled in the state machine thread
        // For now, we'll store the connection request for processing
        std::cout << "RDMA Thread Manager: accepting connection" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "RDMA Thread Manager: error handling connection request: " << e.what() << std::endl;
    }
}

RdmaThreadManager::RdmaThreadManager() {
    std::cout << "RdmaThreadManager: Constructor called" << std::endl;
}

RdmaThreadManager::~RdmaThreadManager() {
    std::cout << "RdmaThreadManager: Destructor called" << std::endl;
    if (_running.load()) {
        shutdown();
    }
}

bool RdmaThreadManager::initialize(bool server_mode, uint16_t port) {
    std::cout << "RdmaThreadManager: Initializing in " << (server_mode ? "server" : "client") << " mode" << std::endl;
    
    _server_mode = server_mode;
    _port = port;
    
    try {
        // Create UCXX context and worker
        _context = ucxx::createContext({}, ucxx::Context::defaultFeatureFlags);
        _worker = _context->createWorker();
        
        std::cout << "RdmaThreadManager: UCXX context and worker created" << std::endl;
        
        // Start threads
        _running = true;
        _shutdown_requested = false;
        
        _progress_thread = std::thread(&RdmaThreadManager::progress_thread_func, this);
        _request_thread = std::thread(&RdmaThreadManager::request_thread_func, this);
        _state_machine_thread = std::thread(&RdmaThreadManager::state_machine_thread_func, this);
        
        std::cout << "RdmaThreadManager: All threads started" << std::endl;
        
        // If server mode, start listening
        if (server_mode && port > 0) {
            // Submit listen request
            auto listen_future = submit_listen(port);
            // Note: In a real implementation, you might want to wait for this to complete
        }
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "RdmaThreadManager: Initialization failed: " << e.what() << std::endl;
        return false;
    }
}

void RdmaThreadManager::shutdown() {
    std::cout << "RdmaThreadManager: Shutdown requested" << std::endl;
    
    _shutdown_requested = true;
    _current_state = RdmaState::SHUTDOWN;
    
    // Notify all waiting threads
    _queue_cv.notify_all();
    
    // Join threads
    if (_progress_thread.joinable()) {
        _progress_thread.join();
        std::cout << "RdmaThreadManager: Progress thread joined" << std::endl;
    }
    
    if (_request_thread.joinable()) {
        _request_thread.join();
        std::cout << "RdmaThreadManager: Request thread joined" << std::endl;
    }
    
    if (_state_machine_thread.joinable()) {
        _state_machine_thread.join();
        std::cout << "RdmaThreadManager: State machine thread joined" << std::endl;
    }
    
    // Clean up UCXX resources
    _active_send_request.reset();
    _active_recv_request.reset();
    _endpoint.reset();
    _listener.reset();
    _listener_ctx.reset();
    _worker.reset();
    _context.reset();
    
    _running = false;
    std::cout << "RdmaThreadManager: Shutdown complete" << std::endl;
}

seastar::future<bool> RdmaThreadManager::submit_tag_send(void* data, size_t size, uint64_t tag) {
    auto promise = std::make_shared<seastar::promise<bool>>();
    auto future = promise->get_future();
    
    RdmaRequest request(RdmaRequestType::TAG_SEND);
    request.data = data;
    request.size = size;
    request.tag = tag;
    request.promise = promise;
    
    submit_request(std::move(request));
    
    return future;
}

seastar::future<bool> RdmaThreadManager::submit_tag_recv(void* data, size_t size, uint64_t tag) {
    auto promise = std::make_shared<seastar::promise<bool>>();
    auto future = promise->get_future();
    
    RdmaRequest request(RdmaRequestType::TAG_RECV);
    request.data = data;
    request.size = size;
    request.tag = tag;
    request.promise = promise;
    
    submit_request(std::move(request));
    
    return future;
}

seastar::future<bool> RdmaThreadManager::submit_connect(const std::string& remote_addr, uint16_t remote_port) {
    auto promise = std::make_shared<seastar::promise<bool>>();
    auto future = promise->get_future();
    
    RdmaRequest request(RdmaRequestType::CONNECT);
    request.remote_addr = remote_addr;
    request.remote_port = remote_port;
    request.promise = promise;
    
    submit_request(std::move(request));
    
    return future;
}

seastar::future<bool> RdmaThreadManager::submit_listen(uint16_t port) {
    auto promise = std::make_shared<seastar::promise<bool>>();
    auto future = promise->get_future();
    
    RdmaRequest request(RdmaRequestType::LISTEN);
    request.remote_port = port;
    request.promise = promise;
    
    submit_request(std::move(request));
    
    return future;
}

void RdmaThreadManager::submit_request(RdmaRequest request) {
    std::lock_guard<std::mutex> lock(_queue_mutex);
    _request_queue.push(std::move(request));
    _queue_cv.notify_one();
}

void RdmaThreadManager::progress_thread_func() {
    std::cout << "RdmaThreadManager: Progress thread started" << std::endl;
    
    while (!_shutdown_requested.load()) {
        if (_worker) {
            try {
                _worker->progress();
            } catch (const std::exception& e) {
                std::cerr << "RdmaThreadManager: Progress thread error: " << e.what() << std::endl;
            }
        }
        
        // Small sleep to avoid busy waiting
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    
    std::cout << "RdmaThreadManager: Progress thread exiting" << std::endl;
}

void RdmaThreadManager::request_thread_func() {
    std::cout << "RdmaThreadManager: Request thread started" << std::endl;
    
    while (!_shutdown_requested.load()) {
        std::unique_lock<std::mutex> lock(_queue_mutex);
        
        // Wait for requests or shutdown
        _queue_cv.wait(lock, [this] { 
            return !_request_queue.empty() || _shutdown_requested.load(); 
        });
        
        // Process all available requests
        while (!_request_queue.empty() && !_shutdown_requested.load()) {
            RdmaRequest request = std::move(_request_queue.front());
            _request_queue.pop();
            lock.unlock();
            
            process_request(request);
            
            lock.lock();
        }
    }
    
    std::cout << "RdmaThreadManager: Request thread exiting" << std::endl;
}

void RdmaThreadManager::state_machine_thread_func() {
    std::cout << "RdmaThreadManager: State machine thread started" << std::endl;
    
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
                std::cout << "RdmaThreadManager: State machine entering shutdown state" << std::endl;
                return;
        }
        
        // Small sleep to avoid busy waiting
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    std::cout << "RdmaThreadManager: State machine thread exiting" << std::endl;
}

seastar::future<> RdmaThreadManager::initialize_async(bool server_mode, uint16_t port) {
    bool success = initialize(server_mode, port);
    if (success) {
        return seastar::make_ready_future<>();
    } else {
        return seastar::make_exception_future<>(std::runtime_error("Failed to initialize RDMA Thread Manager"));
    }
}

seastar::future<> RdmaThreadManager::initialize_listener(uint16_t port) {
    // Shutdown current instance if running
    if (_running.load()) {
        shutdown();
    }

    // Reinitialize in server mode
    bool success = initialize(true, port);
    if (success) {
        return seastar::make_ready_future<>();
    } else {
        return seastar::make_exception_future<>(std::runtime_error("Failed to initialize RDMA Thread Manager in server mode"));
    }
}

seastar::future<> RdmaThreadManager::shutdown_async() {
    shutdown();
    return seastar::make_ready_future<>();
}

void RdmaThreadManager::process_request(const RdmaRequest& request) {
    try {
        switch (request.type) {
            case RdmaRequestType::TAG_SEND: {
                if (!_endpoint) {
                    std::cerr << "RdmaThreadManager: No endpoint available for tag send" << std::endl;
                    if (request.promise) request.promise->set_value(false);
                    return;
                }

                std::cout << "RdmaThreadManager: Processing tag send request, size=" << request.size << ", tag=" << request.tag << std::endl;
                _active_send_request = _endpoint->tagSend(request.data, request.size, ucxx::Tag{request.tag});
                _current_state = RdmaState::SENDING;

                // The completion will be handled in the state machine
                if (request.promise) request.promise->set_value(true);
                break;
            }

            case RdmaRequestType::TAG_RECV: {
                if (!_endpoint) {
                    std::cerr << "RdmaThreadManager: No endpoint available for tag recv" << std::endl;
                    if (request.promise) request.promise->set_value(false);
                    return;
                }

                std::cout << "RdmaThreadManager: Processing tag recv request, size=" << request.size << ", tag=" << request.tag << std::endl;
                _active_recv_request = _endpoint->tagRecv(request.data, request.size, ucxx::Tag{request.tag}, ucxx::TagMaskFull);
                _current_state = RdmaState::RECEIVING;

                // The completion will be handled in the state machine
                if (request.promise) request.promise->set_value(true);
                break;
            }

            case RdmaRequestType::CONNECT: {
                std::cout << "RdmaThreadManager: Processing connect request to " << request.remote_addr << ":" << request.remote_port << std::endl;
                _endpoint = _worker->createEndpointFromHostname(request.remote_addr.c_str(), request.remote_port, true);
                _current_state = RdmaState::CONNECTED;

                if (request.promise) request.promise->set_value(true);
                break;
            }

            case RdmaRequestType::LISTEN: {
                std::cout << "RdmaThreadManager: Processing listen request on port " << request.remote_port << std::endl;
                _listener_ctx = std::make_shared<ListenerContext>();
                _listener = _worker->createListener(request.remote_port, rdma_listener_cb, this);
                _listener_ctx->setListener(_listener);
                _current_state = RdmaState::LISTENING;

                if (request.promise) request.promise->set_value(true);
                break;
            }

            case RdmaRequestType::SHUTDOWN: {
                std::cout << "RdmaThreadManager: Processing shutdown request" << std::endl;
                _current_state = RdmaState::SHUTDOWN;
                if (request.promise) request.promise->set_value(true);
                break;
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "RdmaThreadManager: Error processing request: " << e.what() << std::endl;
        if (request.promise) request.promise->set_value(false);
    }
}

void RdmaThreadManager::handle_idle_state() {
    // Nothing to do in idle state, just wait for requests
}

void RdmaThreadManager::handle_listening_state() {
    // Check if we have received a connection
    if (_listener_ctx && !_listener_ctx->isAvailable()) {
        _endpoint = _listener_ctx->getEndpoint();
        if (_endpoint) {
            std::cout << "RdmaThreadManager: Connection established, transitioning to CONNECTED state" << std::endl;
            _current_state = RdmaState::CONNECTED;
        }
    }
}

void RdmaThreadManager::handle_connecting_state() {
    // This state is currently not used as connection is synchronous
    // Could be extended for async connection handling
}

void RdmaThreadManager::handle_connected_state() {
    // Nothing specific to do, just maintain the connection
    // Requests will transition to SENDING/RECEIVING states
}

void RdmaThreadManager::handle_sending_state() {
    if (_active_send_request && _active_send_request->isCompleted()) {
        try {
            _active_send_request->checkError();
            std::cout << "RdmaThreadManager: Send operation completed successfully" << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "RdmaThreadManager: Send operation failed: " << e.what() << std::endl;
        }

        _active_send_request.reset();
        _current_state = RdmaState::CONNECTED;
    }
}

void RdmaThreadManager::handle_receiving_state() {
    if (_active_recv_request && _active_recv_request->isCompleted()) {
        try {
            _active_recv_request->checkError();
            std::cout << "RdmaThreadManager: Receive operation completed successfully" << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "RdmaThreadManager: Receive operation failed: " << e.what() << std::endl;
        }

        _active_recv_request.reset();
        _current_state = RdmaState::CONNECTED;
    }
}

void RdmaThreadManager::handle_error_state() {
    std::cerr << "RdmaThreadManager: In error state, attempting recovery" << std::endl;
    // Could implement error recovery logic here
    _current_state = RdmaState::IDLE;
}

} // namespace btsp
