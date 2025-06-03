#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <seastar/core/future-util.hh>
#include <seastar/core/sleep.hh>

#include "rpc/tensor_transfer_backend.hpp"

void listener_cb(ucp_conn_request_h conn_request, void* arg) {
  char ip_str[INET6_ADDRSTRLEN];
  char port_str[INET6_ADDRSTRLEN];
  ucp_conn_request_attr_t attr{};
  btsp::ListenerContext* listener_ctx = reinterpret_cast<btsp::ListenerContext*>(arg);

  attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
  ucxx::utils::ucsErrorThrow(ucp_conn_request_query(conn_request, &attr));
  ucxx::utils::sockaddr_get_ip_port_str(&attr.client_address, ip_str, port_str, INET6_ADDRSTRLEN);
  std::cout << "Server received a connection request from client at address " << ip_str << ":"
            << port_str << std::endl;

  if (listener_ctx->isAvailable()) {
    auto&& mgr = btsp::get_tensor_transfer_manager();
    auto worker = mgr.get_worker();
    listener_ctx->createEndpointFromConnRequest(worker, conn_request);
  } else {
    // The server is already handling a connection request from a client,
    // reject this new one
    std::cout << "Rejecting a connection request from " << ip_str << ":" << port_str << "."
              << std::endl
              << "Only one client at a time is supported." << std::endl;
    ucxx::utils::ucsErrorThrow(
      ucp_listener_reject(listener_ctx->getListener()->getHandle(), conn_request));
  }
}

namespace btsp {

// Global tensor transfer manager
static std::unique_ptr<TensorTransferManager> g_tensor_transfer_manager;

TensorTransferManager::TensorTransferManager(){
}

TensorTransferManager::~TensorTransferManager() {
}

seastar::future<> TensorTransferManager::initialize() {
    // Create UCXX context and worker
    _context = ucxx::createContext({}, ucxx::Context::defaultFeatureFlags);
    _worker = _context->createWorker();

    // Start the worker progress thread
    _worker->startProgressThread(true);

    // Return a ready future since initialization is complete
    return seastar::make_ready_future<>();
}

seastar::future<> TensorTransferManager::initialize_listener(uint16_t port) {
    if (!_worker) throw std::runtime_error("Worker must be initialized before listener");
    if (_listener_ctx) return seastar::make_ready_future<>();

    _listener_ctx = std::make_shared<ListenerContext>();

    auto listener = _worker->createListener(
        port,
        listener_cb,
        _listener_ctx.get()
    );
    _listener_ctx->setListener(listener);

    return seastar::make_ready_future<>();
}

std::shared_ptr<ListenerContext> TensorTransferManager::get_listener_context() {
    return _listener_ctx;
}

std::shared_ptr<ucxx::Worker> TensorTransferManager::get_worker() {
    return _worker;
}


seastar::future<> TensorTransferManager::send_tensor(client_type& client, const TensorSpec& spec, void* data) {
    return with_gate(_gate, [this, &client, spec, data]() {
        return get_units(_transfer_sem, 1).then([this, &client, spec, data](auto units) {
            std::cout << "Preparing to send tensor, size " << spec.total_bytes() << " bytes" << std::endl;

            // First, exchange tensor metadata via RPC
            return _prepare_tensor_transfer_client(client, spec).then(
                [this, data, spec, units = std::move(units)](TensorTransferResponse response) {
                    uint64_t tag = response.tag;
                    std::cout << "Sending tensor with tag " << tag << ", size " << spec.total_bytes() << " bytes" << std::endl;

                    // Connect to the server if not already connected
                    if (!_endpoint) {
                        // In a real implementation, we would get the server address from the client
                        // For now, we'll assume we're connecting to localhost
                        _endpoint = _worker->createEndpointFromHostname("127.0.0.1", 12345, true);
                    }

                    // Create a promise to track completion
                    auto promise = std::make_shared<seastar::promise<>>();

                    // Send the tensor data using UCXX tag in a separate thread
                    std::thread([this, data, spec, tag, promise]() {
                        try {
                            // Create a request for the send operation
                            auto request = _endpoint->tagSend(data, spec.total_bytes(), ucxx::Tag{tag});

                            // Wait for the request to complete
                            while (!request->isCompleted()) {
                                // The worker progress is handled by the progress thread
                                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                            }

                            // Check for errors
                            request->checkError();

                            // Set the promise value to indicate completion
                            promise->set_value();
                        } catch (const std::exception& e) {
                            // Set the promise exception to indicate failure
                            promise->set_exception(std::current_exception());
                        }
                    }).detach();

                    // Return the future from the promise
                    return promise->get_future();
                });
        });
    });
}

// seastar::future<> TensorTransferManager::prepare_tensor_receive(TensorSpec spec) {
//     std::stringstream shape_str;
//     shape_str << "[";
//     for (size_t i = 0; i < spec.shape.size(); ++i) {
//         if (i > 0) shape_str << ", ";
//         shape_str << spec.shape[i];
//     }
//     shape_str << "]";

//     std::cout << "Preparing to receive tensor with shape: " << shape_str.str() << "\n"
//                 << "  Buffer type: " << static_cast<int>(spec.buffer_type) << "\n"
//                 << "  Data type: " << static_cast<int>(spec.data_type) << "\n"
//                 << "  Total size: " << spec.total_bytes() << " bytes" << std::endl;

//     try {
//         auto buffer = ucxx::allocateBuffer(spec.buffer_type, spec.total_bytes());
//         std::cout << "Allocated buffer for tensor, size: " << buffer->getSize() << " bytes" << std::endl;
//         return seastar::make_ready_future<>();
//     } catch (const std::exception& e) {
//         std::cerr << "Failed to allocate buffer for tensor: " << e.what() << std::endl;
//         // TODO: Handle error appropriately
//         throw;
//     }
// }

// TODO: modify
void TensorTransferManager::start_tensor_receive(uint64_t tag) {
    TensorSpec spec;
    {
        std::lock_guard<std::mutex> lock(_pending_mutex);
        auto it = _pending_transfers.find(tag);
        if (it == _pending_transfers.end()) {
            throw std::runtime_error("No pending transfer for tag");
        }
        spec = it->second;
    }
    auto buffer = ucxx::allocateBuffer(spec.buffer_type, spec.total_bytes());
    std::thread([this, buffer, tag, spec]() {
        try {
            auto request = _worker->tagRecv(buffer->data(), buffer->getSize(), ucxx::Tag{tag}, ucxx::TagMaskFull);
            while (!request->isCompleted()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            request->checkError();
            std::cout << "Received tensor with tag " << tag << ", size " << spec.total_bytes() << " bytes" << std::endl;
            {
                std::lock_guard<std::mutex> lock(_pending_mutex);
                _pending_transfers.erase(tag);
            }
            std::cout << "Tensor received successfully" << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "Error receiving tensor: " << e.what() << std::endl;
            {
                std::lock_guard<std::mutex> lock(_pending_mutex);
                _pending_transfers.erase(tag);
            }
        }
    }).detach();
}

seastar::future<> TensorTransferManager::shutdown() {
    // Close the gate to prevent new operations
    auto closed = _gate.close();

    // Stop the worker progress thread
    if (_worker) {
        _worker->stopProgressThread();
    }

    // Clean up UCXX resources
    _endpoint.reset();
    _worker.reset();
    _context.reset();

    return closed.then([] {
        return seastar::make_ready_future<>();
    });
}

TensorTransferManager& get_tensor_transfer_manager() {
    if (!g_tensor_transfer_manager) {
        throw std::runtime_error("Tensor transfer manager not initialized");
    }

    return *g_tensor_transfer_manager;
}

seastar::future<> initialize_tensor_transfer() {
    if (g_tensor_transfer_manager) {
        std::cout << "Tensor transfer manager already initialized" << std::endl;
        return seastar::make_ready_future<>();
    }

    g_tensor_transfer_manager = std::make_unique<TensorTransferManager>();
    return g_tensor_transfer_manager->initialize();
}

seastar::future<> shutdown_tensor_transfer() {
    if (!g_tensor_transfer_manager) {
        return seastar::make_ready_future<>();
    }

    return g_tensor_transfer_manager->shutdown().then([] {
        g_tensor_transfer_manager.reset();
        return seastar::make_ready_future<>();
    });
}

} // namespace btsp
