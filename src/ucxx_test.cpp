/**
 * SPDX-FileCopyrightText: Copyright (c) 2022-2023, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: BSD-3-Clause
 */
#include <cassert>
#include <iostream>
#include <memory>
#include <unistd.h>
#include <vector>

#include <ucxx/api.h>
#include <ucxx/utils/sockaddr.h>
#include <ucxx/utils/ucx.h>

enum class ProgressMode {
  Polling,
  Blocking,
  Wait,
  ThreadPolling,
  ThreadBlocking,
} progress_mode = ProgressMode::Polling;

static uint16_t listener_port = 12345;

class ListenerContext {
 private:
  std::shared_ptr<ucxx::Worker> _worker{nullptr};
  std::shared_ptr<ucxx::Endpoint> _endpoint{nullptr};
  std::shared_ptr<ucxx::Listener> _listener{nullptr};

 public:
  explicit ListenerContext(std::shared_ptr<ucxx::Worker> worker) : _worker{worker} {}

  ~ListenerContext() { releaseEndpoint(); }

  void setListener(std::shared_ptr<ucxx::Listener> listener) { _listener = listener; }

  std::shared_ptr<ucxx::Listener> getListener() { return _listener; }

  std::shared_ptr<ucxx::Endpoint> getEndpoint() { return _endpoint; }

  bool isAvailable() const { return _endpoint == nullptr; }

  void createEndpointFromConnRequest(ucp_conn_request_h conn_request) {
    if (!isAvailable()) throw std::runtime_error("Listener context already has an endpoint");

    static bool endpoint_error_handling = true;
    _endpoint = _listener->createEndpointFromConnRequest(conn_request, endpoint_error_handling);
  }

  void releaseEndpoint() { _endpoint.reset(); }
};

static void listener_cb(ucp_conn_request_h conn_request, void* arg) {
  char ip_str[INET6_ADDRSTRLEN];
  char port_str[INET6_ADDRSTRLEN];
  ucp_conn_request_attr_t attr{};
  ListenerContext* listener_ctx = reinterpret_cast<ListenerContext*>(arg);

  attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
  ucxx::utils::ucsErrorThrow(ucp_conn_request_query(conn_request, &attr));
  ucxx::utils::sockaddr_get_ip_port_str(&attr.client_address, ip_str, port_str, INET6_ADDRSTRLEN);
  std::cout << "Server received a connection request from client at address " << ip_str << ":"
            << port_str << std::endl;

  if (listener_ctx->isAvailable()) {
    listener_ctx->createEndpointFromConnRequest(conn_request);
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

static void printUsage() {
  std::cerr << "Usage: ucxx_test [parameters]" << std::endl;
  std::cerr << " UCXX installation test example" << std::endl;
  std::cerr << std::endl;
  std::cerr << "Parameters are:" << std::endl;
  std::cerr << "  -m          progress mode to use, valid values are: 'polling', 'blocking',"
            << std::endl;
  std::cerr << "              'thread-polling', 'thread-blocking' and 'wait' (default: 'blocking')"
            << std::endl;
  std::cerr << "  -p <port>   Port number to listen at" << std::endl;
  std::cerr << "  -h          Print this help" << std::endl;
  std::cerr << std::endl;
}

ucs_status_t parseCommand(int argc, char* const argv[]) {
  int c;
  while ((c = getopt(argc, argv, "m:p:h")) != -1) {
    switch (c) {
      case 'm':
        if (strcmp(optarg, "blocking") == 0) {
          progress_mode = ProgressMode::Blocking;
          break;
        } else if (strcmp(optarg, "polling") == 0) {
          progress_mode = ProgressMode::Polling;
          break;
        } else if (strcmp(optarg, "thread-blocking") == 0) {
          progress_mode = ProgressMode::ThreadBlocking;
          break;
        } else if (strcmp(optarg, "thread-polling") == 0) {
          progress_mode = ProgressMode::ThreadPolling;
          break;
        } else if (strcmp(optarg, "wait") == 0) {
          progress_mode = ProgressMode::Wait;
          break;
        } else {
          std::cerr << "Invalid progress mode: " << optarg << std::endl;
          return UCS_ERR_INVALID_PARAM;
        }
      case 'p':
        listener_port = atoi(optarg);
        if (listener_port <= 0) {
          std::cerr << "Wrong listener port: " << listener_port << std::endl;
          return UCS_ERR_UNSUPPORTED;
        }
        break;
      case 'h':
      default: printUsage(); return UCS_ERR_UNSUPPORTED;
    }
  }

  return UCS_OK;
}

std::function<void()> getProgressFunction(std::shared_ptr<ucxx::Worker> worker,
                                          ProgressMode progressMode) {
  switch (progressMode) {
    case ProgressMode::Polling: return std::bind(std::mem_fn(&ucxx::Worker::progress), worker);
    case ProgressMode::Blocking:
      return std::bind(std::mem_fn(&ucxx::Worker::progressWorkerEvent), worker, -1);
    case ProgressMode::Wait: return std::bind(std::mem_fn(&ucxx::Worker::waitProgress), worker);
    default: return []() {};
  }
}

void waitRequests(ProgressMode progressMode,
                  std::shared_ptr<ucxx::Worker> worker,
                  const std::vector<std::shared_ptr<ucxx::Request>>& requests) {
  auto progress = getProgressFunction(worker, progressMode);
  for (auto& r : requests) {
    while (!r->isCompleted())
      progress();
    r->checkError();
  }
}

int main(int argc, char** argv) {
  if (parseCommand(argc, argv) != UCS_OK) return -1;

  std::cout << "UCXX Installation Test" << std::endl;
  std::cout << "======================" << std::endl;

  try {
    auto context = ucxx::createContext({}, ucxx::Context::defaultFeatureFlags);
    auto worker = context->createWorker();
    auto listener_ctx = std::make_unique<ListenerContext>(worker);
    auto listener = worker->createListener(listener_port, listener_cb, listener_ctx.get());
    listener_ctx->setListener(listener);
    auto endpoint = worker->createEndpointFromHostname("127.0.0.1", listener_port, true);
    std::cout << "Successfully created UCXX context and worker." << std::endl;

    if (progress_mode == ProgressMode::Blocking)
        worker->initBlockingProgressMode();
    else if (progress_mode == ProgressMode::ThreadBlocking)
        worker->startProgressThread(false);
    else if (progress_mode == ProgressMode::ThreadPolling)
        worker->startProgressThread(true);

    auto progress = getProgressFunction(worker, progress_mode);
    while (listener_ctx->isAvailable()) progress();

    std::vector<std::shared_ptr<ucxx::Request>> requests;
    std::vector<int> sendWireupBuffer{1, 2, 3, 4, 5};
    std::vector<std::vector<int>> sendBuffers{
        std::vector<int>(5), std::vector<int>(500), std::vector<int>(50000)
    };

    std::vector<int> recvWireupBuffer(sendWireupBuffer.size(), 0);
    std::vector<std::vector<int>> recvBuffers;
    for (const auto& buffer : sendBuffers) recvBuffers.emplace_back(std::vector<int>(buffer.size(), 0));
    requests.push_back(listener_ctx->getEndpoint()->tagSend(
        sendWireupBuffer.data(), sendWireupBuffer.size() * sizeof(int), ucxx::Tag{0}));
    requests.push_back(endpoint->tagRecv(recvWireupBuffer.data(),
                                        sendWireupBuffer.size() * sizeof(int),
                                        ucxx::Tag{0},
                                        ucxx::TagMaskFull));

    ::waitRequests(progress_mode, worker, requests);
    requests.clear();

    requests.push_back(listener_ctx->getEndpoint()->tagSend(
        sendBuffers[0].data(), sendBuffers[0].size() * sizeof(int), ucxx::Tag{0}));
    requests.push_back(listener_ctx->getEndpoint()->tagRecv(
        recvBuffers[1].data(), recvBuffers[1].size() * sizeof(int), ucxx::Tag{1}, ucxx::TagMaskFull));
    requests.push_back(listener_ctx->getEndpoint()->tagSend(
        sendBuffers[2].data(), sendBuffers[2].size() * sizeof(int), ucxx::Tag{2}, ucxx::TagMaskFull));
    requests.push_back(endpoint->tagRecv(
        recvBuffers[2].data(), recvBuffers[2].size() * sizeof(int), ucxx::Tag{2}, ucxx::TagMaskFull));
    requests.push_back(
        endpoint->tagSend(sendBuffers[1].data(), sendBuffers[1].size() * sizeof(int), ucxx::Tag{1}));
    requests.push_back(endpoint->tagRecv(
        recvBuffers[0].data(), recvBuffers[0].size() * sizeof(int), ucxx::Tag{0}, ucxx::TagMaskFull));
    
    ::waitRequests(progress_mode, worker, requests);
    for (size_t i = 0; i < sendWireupBuffer.size(); ++i)
        assert(recvWireupBuffer[i] == sendWireupBuffer[i]);
    for (size_t i = 0; i < sendBuffers.size(); ++i)
        for (size_t j = 0; j < sendBuffers[i].size(); ++j)
            assert(recvBuffers[i][j] == sendBuffers[i][j]);
    
    if (progress_mode == ProgressMode::ThreadPolling || progress_mode == ProgressMode::ThreadBlocking)
        worker->stopProgressThread();
    
    std::cout << "UCXX installation test passed!" << std::endl;
    
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    std::cerr << "UCXX installation test failed!" << std::endl;
    return 1;
  }
}