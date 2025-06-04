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
    _worker->startProgressThread(false);

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
