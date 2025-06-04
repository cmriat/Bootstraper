#pragma once

#include <memory>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/gate.hh>
#include <seastar/rpc/rpc.hh>
#include <ucxx/api.h>
#include <ucxx/worker.h>
#include <ucxx/endpoint.h>
#include <ucxx/buffer.h>
#include <ucxx/utils/sockaddr.h>
#include <ucxx/utils/ucx.h>


// listening port
static uint16_t listener_port = 12345;
void listener_cb(ucp_conn_request_h conn_request, void* arg);

namespace btsp {

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
 * @brief Tensor Transfer Manager
 *
 * This class manages the transfer of tensor data between client and server
 * using Seastar RPC for metadata exchange and UCXX for RDMA transfer.
 */
class TensorTransferManager {
public:
    TensorTransferManager();

    /**
     * @brief Destroy the Tensor Transfer Manager
     */
    ~TensorTransferManager();

    /**
     * @brief Initialize the tensor transfer manager
     *
     * @return seastar::future<> Future that resolves when initialization is complete
     */
    seastar::future<> initialize();

    /**
     * @brief Initialize the tensor transfer serving listener
     * @param port listen port
     * @return seastar::future<> Future that resolves when initialization is complete
     */
    seastar::future<> initialize_listener(uint16_t port);

    /**
     * @brief Get ListenerContext
     */
    std::shared_ptr<ListenerContext> get_listener_context();

    /**
     * @brief Get Worker
     */
    std::shared_ptr<ucxx::Worker> get_worker();

    // /**
    //  * @brief Register RPC handlers for tensor transfer
    //  *
    //  * @param proto The RPC protocol to register handlers with
    //  */
    // void register_rpc_handlers(protocol_type& proto);

    // /**
    //  * @brief Create RPC clients for tensor transfer
    //  *
    //  * @param proto The RPC protocol to create clients with
    //  */
    // decltype(auto) create_rpc_clients(protocol_type& proto);

    // /**
    //  * @brief Send a tensor to the remote endpoint
    //  *
    //  * @param client The RPC client to use for metadata exchange
    //  * @param spec Tensor specification
    //  * @param data Pointer to the tensor data
    //  * @return seastar::future<> Future that resolves when send is complete
    //  */
    // seastar::future<> send_tensor(client_type& client, const TensorSpec& spec, void* data);

    // /**
    //  * @brief Prepare to receive a tensor (server-side)
    //  *
    //  * @param spec Tensor specification from client
    //  * @return seastar::future<TensorTransferResponse> Future that resolves with the tag to use for RDMA
    //  */
    // seastar::future<> prepare_tensor_receive(TensorSpec spec);


    // TODO: explain
    void start_tensor_receive(uint64_t tag);

    /**
     * @brief Shutdown the tensor transfer manager
     *
     * @return seastar::future<> Future that resolves when shutdown is complete
     */
    seastar::future<> shutdown();

private:
    // UCXX context and worker
    std::shared_ptr<ucxx::Context> _context;
    std::shared_ptr<ucxx::Worker> _worker;
    std::shared_ptr<ucxx::Endpoint> _endpoint;

    std::shared_ptr<ListenerContext> _listener_ctx;

    // Gate to ensure all operations complete before shutdown
    seastar::gate _gate;

};

/**
 * @brief Get the global tensor transfer manager
 *
 * @return TensorTransferManager& Reference to the global tensor transfer manager
 */
TensorTransferManager& get_tensor_transfer_manager();

/**
 * @brief Initialize the global tensor transfer manager
 *
 * @param server_mode Whether this is a server (true) or client (false)
 * @return seastar::future<> Future that resolves when initialization is complete
 */
seastar::future<> initialize_tensor_transfer();

std::shared_ptr<ListenerContext> get_listener_context();

/**
 * @brief Shutdown the global tensor transfer manager
 *
 * @return seastar::future<> Future that resolves when shutdown is complete
 */
seastar::future<> shutdown_tensor_transfer();

} // namespace btsp
