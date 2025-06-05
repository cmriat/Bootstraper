#include "coucxx/ucxx_thread_manager.hpp"
#include <iostream>
#include <seastar/core/future-util.hh>
#include <seastar/core/sleep.hh>

namespace coucxx {

UcxxThreadManager::UcxxThreadManager(bool server_mode, uint16_t port, const std::string& server_addr)
    : _server_mode(server_mode), _port(port), _server_addr(server_addr) {
    
    // Start the UCXX thread
    _running = true;
    _thread = std::thread(&UcxxThreadManager::thread_func, this);
}

UcxxThreadManager::~UcxxThreadManager() {
    if (_running) {
        // Request shutdown and wait for thread to exit
        _shutdown_requested = true;
        
        // Notify the thread to check for shutdown
        _queue_cv.notify_one();
        
        if (_thread.joinable()) {
            _thread.join();
        }
    }
}

void UcxxThreadManager::thread_func() {
    try {
        // Initialize UCXX context and worker
        _context = ucxx::createContext({}, ucxx::Context::defaultFeatureFlags);
        _worker = _context->createWorker();
        
        // If server mode, create a listener
        if (_server_mode) {
            // Create a listener callback
            auto listener_cb = [this](ucp_conn_request_h conn_request, void* arg) {
                std::cout << "Server received a connection request" << std::endl;
                
                // Create an endpoint from the connection request
                _endpoint = _listener->createEndpointFromConnRequest(conn_request, true);
            };
            
            // Create the listener
            _listener = _worker->createListener(_port, listener_cb, nullptr);
            std::cout << "UCXX server listening on port " << _port << std::endl;
        } else if (!_server_addr.empty()) {
            // Client mode with server address provided - connect immediately
            _endpoint = _worker->createEndpointFromHostname(_server_addr.c_str(), _port, true);
            std::cout << "UCXX client connected to " << _server_addr << ":" << _port << std::endl;
        }
        
        // Start the worker progress thread
        _worker->startProgressThread(true);
        
        // Process commands until shutdown is requested
        while (!_shutdown_requested) {
            UcxxCommand cmd;
            bool has_cmd = false;
            
            // Get a command from the queue
            {
                std::unique_lock<std::mutex> lock(_queue_mutex);
                _queue_cv.wait(lock, [this] { 
                    return !_command_queue.empty() || _shutdown_requested; 
                });
                
                if (!_command_queue.empty()) {
                    cmd = std::move(_command_queue.front());
                    _command_queue.pop();
                    has_cmd = true;
                }
            }
            
            // Execute the command
            if (has_cmd) {
                bool success = true;
                try {
                    cmd.execute();
                } catch (const std::exception& e) {
                    std::cerr << "Error executing UCXX command: " << e.what() << std::endl;
                    success = false;
                }
                
                // Complete the command
                if (cmd.complete) {
                    cmd.complete(success);
                }
            }
        }
        
        // Stop the worker progress thread
        _worker->stopProgressThread();
        
        // Clean up UCXX resources
        _endpoint.reset();
        _listener.reset();
        _worker.reset();
        _context.reset();
        
    } catch (const std::exception& e) {
        std::cerr << "Error in UCXX thread: " << e.what() << std::endl;
    }
    
    _running = false;
}

template<typename Func, typename CompletionFunc>
seastar::future<> UcxxThreadManager::submit_command(
    UcxxCommandType type, Func&& func, CompletionFunc&& complete_func) {
    
    auto id = _next_command_id++;
    auto promise = std::make_shared<seastar::promise<>>();
    
    auto completion_wrapper = [promise, complete_func = std::forward<CompletionFunc>(complete_func)]
                             (bool success) mutable {
        complete_func(success);
        promise->set_value();
    };
    
    UcxxCommand cmd{
        .type = type,
        .execute = std::forward<Func>(func),
        .complete = std::move(completion_wrapper),
        .id = id
    };
    
    {
        std::lock_guard<std::mutex> lock(_queue_mutex);
        _command_queue.push(std::move(cmd));
    }
    
    _queue_cv.notify_one();
    
    return promise->get_future();
}

seastar::future<> UcxxThreadManager::connect(const std::string& remote_addr, uint16_t remote_port) {
    return submit_command(
        UcxxCommandType::CONNECT,
        [this, remote_addr, remote_port]() {
            _endpoint = _worker->createEndpointFromHostname(remote_addr.c_str(), remote_port, true);
        },
        [](bool success) {
            if (!success) {
                throw std::runtime_error("Failed to connect to remote endpoint");
            }
        }
    );
}

seastar::future<uint64_t> UcxxThreadManager::prepare_tensor_send(const TensorSpec& spec) {
    // In a real implementation, this would send the tensor metadata via RPC
    // For now, we'll just return a new tag to use for the RDMA transfer
    auto tag = _next_tag++;
    return seastar::make_ready_future<uint64_t>(tag);
}

seastar::future<std::pair<TensorSpec, uint64_t>> UcxxThreadManager::prepare_tensor_recv() {
    // In a real implementation, this would receive the tensor metadata via RPC
    // For now, we'll just return a dummy tensor spec and tag
    TensorSpec spec;
    spec.buffer_type = BufferType::RMM;
    spec.data_type = DataType::FLOAT32;
    spec.shape = {1, 1, 1};
    
    auto tag = _next_tag++;
    return seastar::make_ready_future<std::pair<TensorSpec, uint64_t>>(
        std::make_pair(spec, tag)
    );
}

seastar::future<> UcxxThreadManager::tag_send(void* buffer, size_t size, uint64_t tag) {
    return submit_command(
        UcxxCommandType::TAG_SEND,
        [this, buffer, size, tag]() {
            if (!_endpoint) {
                throw std::runtime_error("No endpoint available for tag_send");
            }
            
            // Create a request for the send operation
            auto request = _endpoint->tagSend(buffer, size, ucxx::Tag{tag});
            
            // Wait for the request to complete
            while (!request->isCompleted()) {
                // The worker progress is handled by the progress thread
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            
            // Check for errors
            request->checkError();
        },
        [](bool success) {
            if (!success) {
                throw std::runtime_error("Failed to send data using tag");
            }
        }
    );
}

seastar::future<size_t> UcxxThreadManager::tag_recv(void* buffer, size_t size, uint64_t tag) {
    auto promise = std::make_shared<seastar::promise<size_t>>();
    
    submit_command(
        UcxxCommandType::TAG_RECV,
        [this, buffer, size, tag]() {
            if (!_worker) {
                throw std::runtime_error("No worker available for tag_recv");
            }
            
            // Create a request for the receive operation
            auto request = _worker->tagRecv(buffer, size, ucxx::Tag{tag}, ucxx::TagMaskFull);
            
            // Wait for the request to complete
            while (!request->isCompleted()) {
                // The worker progress is handled by the progress thread
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            
            // Check for errors
            request->checkError();
        },
        [promise, size](bool success) {
            if (success) {
                promise->set_value(size);
            } else {
                promise->set_exception(std::make_exception_ptr(
                    std::runtime_error("Failed to receive data using tag")));
            }
        }
    );
    
    return promise->get_future();
}

seastar::future<> UcxxThreadManager::shutdown() {
    return submit_command(
        UcxxCommandType::SHUTDOWN,
        [this]() {
            _shutdown_requested = true;
        },
        [](bool) {}
    );
}

} // namespace coucxx
