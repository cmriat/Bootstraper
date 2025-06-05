#pragma once

#include <memory>
#include <ucxx/api.h>
#include <ucxx/worker.h>
#include <ucxx/endpoint.h>
#include <ucxx/buffer.h>
#include <ucxx/utils/sockaddr.h>
#include <ucxx/utils/ucx.h>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>


// listening port
static uint16_t listener_port = 12345;
void listener_cb(ucp_conn_request_h conn_request, void* arg);

namespace btsp {

// Forward declarations
class RdmaThreadManager;

// RDMA request types
enum class RdmaRequestType {
    TAG_SEND,
    TAG_RECV,
    CONNECT,
    LISTEN,
    SHUTDOWN
};

// RDMA request structure
struct RdmaRequest {
    RdmaRequestType type;
    void* data;
    size_t size;
    uint64_t tag;
    std::string remote_addr;
    uint16_t remote_port;
    std::shared_ptr<seastar::promise<bool>> promise;

    RdmaRequest(RdmaRequestType t) : type(t), data(nullptr), size(0), tag(0), remote_port(0) {}
};

// RDMA state machine states
enum class RdmaState {
    IDLE,
    LISTENING,
    CONNECTING,
    CONNECTED,
    SENDING,
    RECEIVING,
    ERROR,
    SHUTDOWN
};

class ListenerContext {
 private:
  std::shared_ptr<ucxx::Listener> _listener{nullptr};
  std::shared_ptr<ucxx::Endpoint> _endpoint{nullptr};

 public:
  ListenerContext() = default;

  ~ListenerContext() { releaseEndpoint(); }

  void setListener(std::shared_ptr<ucxx::Listener> listener) { _listener = listener; }
  std::shared_ptr<ucxx::Listener> getListener() { return _listener; }
  std::shared_ptr<ucxx::Endpoint> getEndpoint() { return _endpoint; }
  bool isAvailable() const { return _endpoint == nullptr; }

  void createEndpointFromConnRequest(std::shared_ptr<ucxx::Worker> worker, ucp_conn_request_h conn_request) {
    if (!isAvailable()) throw std::runtime_error("Listener context already has an endpoint");
    static bool endpoint_error_handling = true;
    _endpoint = _listener->createEndpointFromConnRequest(conn_request, endpoint_error_handling);
  }

  void releaseEndpoint() { _endpoint.reset(); }
};

/**
 * @brief RDMA Thread Manager
 *
 * This class manages UCXX RDMA operations in separate threads to avoid blocking
 * the Seastar main event loop. It implements a state machine pattern with
 * request queues for communication between Seastar RPC thread and UCXX threads.
 */
class RdmaThreadManager {
public:
    RdmaThreadManager();
    ~RdmaThreadManager();

    // Initialize the RDMA thread manager
    bool initialize(bool server_mode = false, uint16_t port = 0);

    // Initialize as Seastar future (for compatibility with TensorTransferManager)
    seastar::future<> initialize_async(bool server_mode = false, uint16_t port = 0);

    // Initialize listener (server mode)
    seastar::future<> initialize_listener(uint16_t port);

    // Shutdown the RDMA thread manager
    void shutdown();

    // Shutdown as Seastar future (for compatibility)
    seastar::future<> shutdown_async();

    // Submit requests to RDMA thread
    seastar::future<bool> submit_tag_send(void* data, size_t size, uint64_t tag);
    seastar::future<bool> submit_tag_recv(void* data, size_t size, uint64_t tag);
    seastar::future<bool> submit_connect(const std::string& remote_addr, uint16_t remote_port);
    seastar::future<bool> submit_listen(uint16_t port);

    // Get current state
    RdmaState get_state() const { return _current_state.load(); }

    // Check if connected
    bool is_connected() const { return _current_state.load() == RdmaState::CONNECTED; }

    // Legacy compatibility methods
    std::shared_ptr<ListenerContext> get_listener_context() { return _listener_ctx; }
    std::shared_ptr<ucxx::Worker> get_worker() { return _worker; }

private:
    // Thread functions
    void progress_thread_func();
    void request_thread_func();
    void state_machine_thread_func();

    // State machine handlers
    void handle_idle_state();
    void handle_listening_state();
    void handle_connecting_state();
    void handle_connected_state();
    void handle_sending_state();
    void handle_receiving_state();
    void handle_error_state();

    // Request processing
    void process_request(const RdmaRequest& request);
    void submit_request(RdmaRequest request);

    // UCXX resources
    std::shared_ptr<ucxx::Context> _context;
    std::shared_ptr<ucxx::Worker> _worker;
    std::shared_ptr<ucxx::Endpoint> _endpoint;
    std::shared_ptr<ucxx::Listener> _listener;
    std::shared_ptr<ListenerContext> _listener_ctx;

    // Threading
    std::thread _progress_thread;
    std::thread _request_thread;
    std::thread _state_machine_thread;

    // Request queue
    std::queue<RdmaRequest> _request_queue;
    std::mutex _queue_mutex;
    std::condition_variable _queue_cv;

    // State management
    std::atomic<RdmaState> _current_state{RdmaState::IDLE};
    std::atomic<bool> _shutdown_requested{false};
    std::atomic<bool> _running{false};

    // Configuration
    bool _server_mode{false};
    uint16_t _port{0};

    // Active requests tracking
    std::shared_ptr<ucxx::Request> _active_send_request;
    std::shared_ptr<ucxx::Request> _active_recv_request;
};

/**
 * @brief Get the global RDMA thread manager
 *
 * @return RdmaThreadManager& Reference to the global RDMA thread manager
 */
RdmaThreadManager& get_rdma_thread_manager();

/**
 * @brief Initialize the global RDMA thread manager
 *
 * @param server_mode Whether this is a server (true) or client (false)
 * @param port Port number for server mode
 * @return seastar::future<> Future that resolves when initialization is complete
 */
seastar::future<> initialize_rdma_manager(bool server_mode = false, uint16_t port = 0);

/**
 * @brief Initialize the global RDMA thread manager listener
 *
 * @param port Port number to listen on
 * @return seastar::future<> Future that resolves when initialization is complete
 */
seastar::future<> initialize_rdma_listener(uint16_t port);

/**
 * @brief Get the global listener context
 *
 * @return std::shared_ptr<ListenerContext> Shared pointer to the listener context
 */
std::shared_ptr<ListenerContext> get_listener_context();

/**
 * @brief Shutdown the global RDMA thread manager
 *
 * @return seastar::future<> Future that resolves when shutdown is complete
 */
seastar::future<> shutdown_rdma_manager();

} // namespace btsp
