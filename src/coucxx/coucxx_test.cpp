#include <cassert>
#include <chrono>
#include <iostream>
#include <memory>
#include <numeric>
#include <thread>
#include <unistd.h>
#include <vector>
#include <arpa/inet.h>
#include <netinet/in.h>

#include <ucxx/api.h>
#include <ucxx/utils/sockaddr.h>
#include <ucxx/utils/ucx.h>
#include "coucxx/co_tag.hpp"

enum class ProgressMode {
  Polling,
  Blocking,
  Wait,
  ThreadPolling,
  ThreadBlocking,
} progress_mode = ProgressMode::Polling;

enum class RunMode {
  Server,
  Client,
} run_mode = RunMode::Server;

static uint16_t listener_port = 12345;
static std::string server_address = "127.0.0.1";

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

  void createEndpointFromConnRequest(ucp_conn_request_h conn_request)
  {
    if (!isAvailable()) throw std::runtime_error("Listener context already has an endpoint");

    static bool endpoint_error_handling = true;
    _endpoint = _listener->createEndpointFromConnRequest(conn_request, endpoint_error_handling);
  }

  void releaseEndpoint() { _endpoint.reset(); }
};

static void listener_cb(ucp_conn_request_h conn_request, void* arg)
{
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

static void printUsage()
{
  std::cerr << "Usage: coroutine_tag_example [parameters]" << std::endl;
  std::cerr << " UCXX coroutine tag example" << std::endl;
  std::cerr << std::endl;
  std::cerr << "Parameters are:" << std::endl;
  std::cerr << "  -m          progress mode to use, valid values are: 'polling', 'blocking',"
            << std::endl;
  std::cerr << "              'thread-polling', 'thread-blocking' and 'wait' (default: 'blocking')"
            << std::endl;
  std::cerr << "  -p <port>   Port number to listen at" << std::endl;
  std::cerr << "  -s <addr>   Server address to connect to (client mode only)" << std::endl;
  std::cerr << "  -c          Run as client (default: server)" << std::endl;
  std::cerr << "  -h          Print this help" << std::endl;
  std::cerr << std::endl;
}

ucs_status_t parseCommand(int argc, char* const argv[])
{
  int c;
  while ((c = getopt(argc, argv, "m:p:s:ch")) != -1) {
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
      case 's':
        server_address = optarg;
        break;
      case 'c':
        run_mode = RunMode::Client;
        break;
      case 'h':
      default: printUsage(); return UCS_ERR_UNSUPPORTED;
    }
  }

  return UCS_OK;
}

std::function<void()> getProgressFunction(std::shared_ptr<ucxx::Worker> worker,
                                          ProgressMode progressMode)
{
  switch (progressMode) {
    case ProgressMode::Polling: return std::bind(std::mem_fn(&ucxx::Worker::progress), worker);
    case ProgressMode::Blocking:
      return std::bind(std::mem_fn(&ucxx::Worker::progressWorkerEvent), worker, -1);
    case ProgressMode::Wait: return std::bind(std::mem_fn(&ucxx::Worker::waitProgress), worker);
    default: return []() {};
  }
}

// Coroutine function to send data
coucxx::coroutine::task<void> send_data_coroutine(std::shared_ptr<ucxx::Endpoint> endpoint,
                                                 std::vector<int>& data,
                                                 ucxx::Tag tag) {
  std::cout << "Sending data with tag " << tag << std::endl;

  // Send the data using the coroutine awaitable
  co_await coucxx::coroutine::tag_send(endpoint, data.data(), data.size() * sizeof(int), tag);

  std::cout << "Data sent successfully with tag " << tag << std::endl;
}

// Coroutine function to receive data
coucxx::coroutine::task<size_t> receive_data_coroutine(std::shared_ptr<ucxx::Worker> worker,
                                                      std::vector<int>& buffer,
                                                      ucxx::Tag tag) {
  std::cout << "Waiting to receive data with tag " << tag << std::endl;

  // Receive the data using the coroutine awaitable
  auto recv_awaitable = coucxx::coroutine::tag_recv(worker, buffer.data(), buffer.size() * sizeof(int), tag);
  size_t received_bytes = co_await recv_awaitable;

  std::cout << "Received " << received_bytes << " bytes with tag " << recv_awaitable.sender_tag() << std::endl;

  co_return received_bytes;
}

// Function to drive the coroutine execution
void run_coroutine(coucxx::coroutine::task<void>& task, std::shared_ptr<ucxx::Worker> worker) {
  auto progress = getProgressFunction(worker, progress_mode);

  // Keep progressing until the task is ready
  while (!task.is_ready()) {
    progress();
  }

  // Get the result (will throw if there was an error)
  try {
    task.get_result();
  } catch (const std::exception& e) {
    std::cerr << "Coroutine failed: " << e.what() << std::endl;
  }
}

// Function to drive the coroutine execution with result
template<typename T>
T run_coroutine_with_result(coucxx::coroutine::task<T>& task, std::shared_ptr<ucxx::Worker> worker) {
  auto progress = getProgressFunction(worker, progress_mode);

  // Keep progressing until the task is ready
  while (!task.is_ready()) {
    progress();
  }

  // Get the result (will throw if there was an error)
  try {
    return task.get_result();
  } catch (const std::exception& e) {
    std::cerr << "Coroutine failed: " << e.what() << std::endl;
    throw;
  }
}

int main(int argc, char** argv)
{
  if (parseCommand(argc, argv) != UCS_OK) return -1;

  std::cout << "UCXX Coroutine Tag Example" << std::endl;
  std::cout << "=========================" << std::endl;

  try {
    // Setup: create UCP context, worker
    auto context = ucxx::createContext({}, ucxx::Context::defaultFeatureFlags);
    auto worker = context->createWorker();

    // Create some test data
    std::vector<int> send_data(100);
    std::iota(send_data.begin(), send_data.end(), 0);  // Fill with 0, 1, 2, ...

    std::vector<int> recv_buffer(100, 0);  // Initialize with zeros

    std::shared_ptr<ucxx::Endpoint> endpoint;

    if (run_mode == RunMode::Server) {
      // Server mode
      // Create a listener context
      ListenerContext listener_ctx(worker);

      // Create a listener
      auto listener = worker->createListener(listener_port, listener_cb, &listener_ctx);
      listener_ctx.setListener(listener);

      std::cout << "Server mode: Listening on port " << listener_port << std::endl;

      // Wait for a client connection
      std::cout << "Waiting for client connection..." << std::endl;
      while (!listener_ctx.getEndpoint()) {
        worker->progress();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }

      std::cout << "Client connected!" << std::endl;
      endpoint = listener_ctx.getEndpoint();

      // Create and run the receive coroutine first (server receives first)
      auto recv_task = receive_data_coroutine(worker, recv_buffer, ucxx::Tag{42});
      size_t received_bytes = run_coroutine_with_result(recv_task, worker);

      // Create and run the send coroutine
      auto send_task = send_data_coroutine(endpoint, send_data, ucxx::Tag{42});
      run_coroutine(send_task, worker);
    } else {
      // Client mode
      std::cout << "Client mode: Connecting to server " << server_address << ":" << listener_port << std::endl;

      // Create client endpoint
      endpoint = worker->createEndpointFromHostname(server_address.c_str(), listener_port, true);

      std::cout << "Connected to server!" << std::endl;

      // Create and run the send coroutine first (client sends first)
      auto send_task = send_data_coroutine(endpoint, send_data, ucxx::Tag{42});
      run_coroutine(send_task, worker);

      // Create and run the receive coroutine
      auto recv_task = receive_data_coroutine(worker, recv_buffer, ucxx::Tag{42});
      size_t received_bytes = run_coroutine_with_result(recv_task, worker);
    }

    // Verify the received data
    bool data_correct = true;
    for (size_t i = 0; i < recv_buffer.size(); ++i) {
      if (recv_buffer[i] != static_cast<int>(i)) {
        data_correct = false;
        std::cerr << "Data mismatch at index " << i << ": expected " << i
                  << ", got " << recv_buffer[i] << std::endl;
        break;
      }
    }

    if (data_correct) {
      std::cout << "Data verification successful!" << std::endl;
    } else {
      std::cout << "Data verification failed!" << std::endl;
    }

    std::cout << "UCXX coroutine tag example completed successfully!" << std::endl;
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    std::cerr << "UCXX coroutine tag example failed!" << std::endl;
    return 1;
  }
}
