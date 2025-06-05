#include <iostream>
#include <string>

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
    auto&& mgr = btsp::get_rdma_thread_manager();
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

// Global RDMA thread manager
static std::unique_ptr<RdmaThreadManager> g_rdma_thread_manager;

RdmaThreadManager& get_rdma_thread_manager() {
    if (!g_rdma_thread_manager) {
        throw std::runtime_error("RDMA thread manager not initialized");
    }
    return *g_rdma_thread_manager;
}

seastar::future<> initialize_rdma_manager(bool server_mode, uint16_t port) {
    if (g_rdma_thread_manager) {
        std::cout << "RDMA thread manager already initialized" << std::endl;
        return seastar::make_ready_future<>();
    }

    g_rdma_thread_manager = std::make_unique<RdmaThreadManager>();
    return g_rdma_thread_manager->initialize_async(server_mode, port);
}

seastar::future<> initialize_rdma_listener(uint16_t port) {
    if (!g_rdma_thread_manager) {
        return initialize_rdma_manager(true, port);
    }
    return g_rdma_thread_manager->initialize_listener(port);
}

std::shared_ptr<ListenerContext> get_listener_context() {
    if (!g_rdma_thread_manager) {
        return nullptr;
    }
    return g_rdma_thread_manager->get_listener_context();
}

seastar::future<> shutdown_rdma_manager() {
    if (!g_rdma_thread_manager) {
        return seastar::make_ready_future<>();
    }

    return g_rdma_thread_manager->shutdown_async().then([] {
        g_rdma_thread_manager.reset();
        return seastar::make_ready_future<>();
    });
}

} // namespace btsp
