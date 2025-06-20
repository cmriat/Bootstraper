#define LIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE 1
#define UCXX_ENABLE_RMM                                1

#include <rmm/cuda_stream_view.hpp>
#include <rmm/device_buffer.hpp>
// #include <rmm/logger_impl/logger_impl.hpp>
#include <rmm/mr/device/device_memory_resource.hpp>
#include <rmm/mr/device/pool_memory_resource.hpp>

#include <cuda_runtime_api.h>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <memory>
#include <ranges>
#include <string>
#include <ucs/memory/memory_type.h>
#include <ucxx/buffer.h>
#include <ucxx/typedefs.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include <ucxx/api.h>
#include <ucxx/utils/sockaddr.h>
#include <ucxx/utils/ucx.h>
#include <cassert>

// #include <rmm/device_buffer.hpp>

size_t GiB = 1024 * 1024 * 1024;

enum class ProgressMode {
  Polling,
  Blocking,
  Wait,
  ThreadPolling,
  ThreadBlocking,
};
ProgressMode progress_mode    = ProgressMode::Polling;
static uint16_t listener_port = 12345;

using IPAddress = std::string;

class ListenerContext {
 private:
  // std::shared_ptr<
  std::shared_ptr<ucxx::Worker> _worker{nullptr};
  std::unordered_map<IPAddress, std::shared_ptr<ucxx::Endpoint>> _endpoints;
  std::shared_ptr<ucxx::Listener> _listener{nullptr};
  const int _max_endpoints;
  int _num_endpoints{0};
  std::shared_ptr<ucxx::Endpoint> recent_ep{nullptr};

  std::atomic<bool> _isAvailable{false};

 public:
  explicit ListenerContext(std::shared_ptr<ucxx::Worker> worker, const int max_endpoints)
    : _worker{worker}, _max_endpoints(max_endpoints)
  {
  }

  ~ListenerContext() { releaseEndpoints(); }

  void setListener(std::shared_ptr<ucxx::Listener> listener) { _listener = listener; }

  std::shared_ptr<ucxx::Listener> getListener() { return _listener; }

  // bool isAvailable() const { return _num_endpoints < _max_endpoints; }

  auto createEndpointFromConnRequest(ucp_conn_request_h conn_request)
  {
    // if (!isAvailable()) throw std::runtime_error("Listener context already has an endpoint");

    char ip_str[INET6_ADDRSTRLEN];
    char port_str[INET6_ADDRSTRLEN];

    static bool endpoint_error_handling = true;
    ucp_conn_request_attr_t attr{};
    attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
    ucxx::utils::ucsErrorThrow(ucp_conn_request_query(conn_request, &attr));
    ucxx::utils::sockaddr_get_ip_port_str(&attr.client_address, ip_str, port_str, INET6_ADDRSTRLEN);
    std::cout << "Connection request from " << ip_str << ":" << port_str << std::endl;
    std::string ipAddr = std::string(ip_str) + ":" + std::string(port_str);
    // auto ep            = _endpoints.find(ipAddr);
    std::shared_ptr<ucxx::Endpoint> ep = nullptr;
    if (_endpoints.find(ipAddr) != _endpoints.end()) {
      std::cout << "Endpoint already exists for " << ipAddr << std::endl;
      ep = _endpoints[ipAddr];
    } else {
      std::cout << "Creating endpoint for " << ipAddr << std::endl;
      ep =
        ucxx::createEndpointFromConnRequest(this->_listener, conn_request, endpoint_error_handling);
      _endpoints[ipAddr] = ep;
    }
    recent_ep = ep;
    _isAvailable.store(true);
  }

  void releaseEndpoints()
  {
    for (auto& [ip, endpoint] : _endpoints) {
      endpoint.reset();
    }
  }

  std::shared_ptr<ucxx::Endpoint> getEndpoint() { return recent_ep; }
  bool isAvailable() const { return _isAvailable.load(); }
};

static void listener_cb(ucp_conn_request_h conn_request, void* arg)
{
  ListenerContext* listener_ctx = static_cast<ListenerContext*>(arg);
  listener_ctx->createEndpointFromConnRequest(conn_request);
}

int main()
{
  ucxx::ConfigMap config = {
    { "TLS","cuda_copy,cuda_ipc,tcp" }
  };
  RMM_CUDA_TRY(cudaSetDevice(1));
  auto ctx          = ucxx::createContext(config, ucxx::Context::defaultFeatureFlags);
  auto worker       = ctx->createWorker();
  auto listener_ctx = std::make_shared<ListenerContext>(worker, 2);
  auto listener =
    worker->createListener(listener_port, listener_cb, static_cast<void*>(listener_ctx.get()));
  listener_ctx->setListener(listener);

  rmm::mr::cuda_memory_resource cuda_mr;
  auto initial_size = rmm::percent_of_free_device_memory(50);
  rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource> pool_mr{&cuda_mr, initial_size};
  rmm::mr::set_current_device_resource(&pool_mr);

  // size_t size = 10 * GiB;
  // auto buffer = std::make_shared<ucxx::RMMBuffer>(size);

  auto progress = [&worker]() { worker->waitProgress(); };
  std::cout << "Progress mode: Polling" << std::endl;
  while (!listener_ctx->isAvailable()) {
    progress();
    std::cout << "Progressing For Connection..." << std::endl;
  }

  auto ep = listener_ctx->getEndpoint();
  std::vector<std::shared_ptr<ucxx::Request>> requests;
  std::vector<std::shared_ptr<rmm::device_buffer>> recvBuffers;
  for (int i = 0; i < 3; i++) {
    auto buffer = std::make_shared<rmm::device_buffer>(40960, rmm::cuda_stream_default);
    auto req    = ep->tagRecv(buffer->data(), buffer->size(), ucxx::Tag(0), ucxx::TagMaskFull);
    requests.push_back(req);
    recvBuffers.push_back(buffer);
  }

  for (auto& request : requests) {
    while (!request->isCompleted()) {
      worker->progress();
    }
    std::cout << "Request status: " << request->getStatus() << std::endl;
  }

  requests.clear();
  recvBuffers.clear();
  for (int i = 0; i < 3; i++) {
    auto buffer = std::make_shared<rmm::device_buffer>(GiB, rmm::cuda_stream_default);
    auto req    = ep->tagRecv(buffer->data(), buffer->size(), ucxx::Tag(0), ucxx::TagMaskFull);
    requests.push_back(req);
    recvBuffers.push_back(buffer);
  }

  // while (!requests.empty()) {
  //   for (auto it = requests.begin(); it != requests.end();) {
  //     if ((*it)->isCompleted()) {
  //       std::cout << "Request completed" << std::endl;
  //       std::cout << "Request status: " << (*it)->getStatus() << std::endl;
  //       it = requests.erase(it);
  //     } else {
  //       worker->progress();
  //       ++it;
  //     }
  //   }
  // }
  for (auto& request : requests) {
    while (!request->isCompleted()) {
      worker->progress();
    }
    std::cout << "Request status: " << request->getStatus() << std::endl;
  }

  usleep(10000000);
  return 0;
}