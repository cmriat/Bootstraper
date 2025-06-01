#include "rpc/tensor_transfer.hpp"
#include <iostream>
#include <thread>
#include <seastar/core/future-util.hh>
#include <seastar/core/sleep.hh>

namespace rpc {

// Global tensor transfer manager
static std::unique_ptr<TensorTransferManager> g_tensor_transfer_manager;

TensorTransferManager::TensorTransferManager(bool server_mode)
    : _server_mode(server_mode) {
}

TensorTransferManager::~TensorTransferManager() {
    // Ensure all operations are complete before destroying
    _gate.check();
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

void TensorTransferManager::register_rpc_handlers(protocol_type& proto) {
    // Register handler for prepare_tensor_transfer
    proto.register_handler(to_underlying(msg_type::PREPARE_TENSOR_TRANSFER),
        [this](TensorSpec spec) {
            return with_gate(_gate, [this, spec = std::move(spec)]() {
                return prepare_tensor_receive(std::move(spec));
            });
        });
}

void TensorTransferManager::create_rpc_clients(protocol_type& proto) {
    // Create client for prepare_tensor_transfer
    _prepare_tensor_transfer_client = proto.make_client<seastar::future<TensorTransferResponse> (TensorSpec)>(
        to_underlying(msg_type::PREPARE_TENSOR_TRANSFER));
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

seastar::future<TensorTransferResponse> TensorTransferManager::prepare_tensor_receive(TensorSpec spec) {
    return with_gate(_gate, [this, spec = std::move(spec)]() {
        // Generate a new tag for this tensor transfer
        uint64_t tag = _next_tag++;

        std::cout << "Preparing to receive tensor with tag " << tag << ", size " << spec.total_bytes() << " bytes" << std::endl;

        // Store the tensor spec for later use
        {
            std::lock_guard<std::mutex> lock(_pending_mutex);
            _pending_transfers[tag] = spec;
        }

        // Create a buffer for the tensor data
        auto buffer = ucxx::allocateBuffer(spec.buffer_type, spec.total_bytes());

        // Start receiving the tensor data in a separate thread
        std::thread([this, buffer, tag, spec]() {
            try {
                // Create a request for the receive operation
                auto request = _worker->tagRecv(buffer->data(), buffer->getSize(), ucxx::Tag{tag}, ucxx::TagMaskFull);

                // Wait for the request to complete
                while (!request->isCompleted()) {
                    // The worker progress is handled by the progress thread
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }

                // Check for errors
                request->checkError();

                std::cout << "Received tensor with tag " << tag << ", size " << spec.total_bytes() << " bytes" << std::endl;

                // Remove the pending transfer
                {
                    std::lock_guard<std::mutex> lock(_pending_mutex);
                    _pending_transfers.erase(tag);
                }

                // In a real implementation, we would do something with the received tensor
                // For now, we'll just print a message
                std::cout << "Tensor received successfully" << std::endl;
            } catch (const std::exception& e) {
                std::cerr << "Error receiving tensor: " << e.what() << std::endl;

                // Remove the pending transfer
                {
                    std::lock_guard<std::mutex> lock(_pending_mutex);
                    _pending_transfers.erase(tag);
                }
            }
        }).detach();

        // Return the tag to the client
        TensorTransferResponse response;
        response.tag = tag;
        return seastar::make_ready_future<TensorTransferResponse>(response);
    });
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

seastar::future<> initialize_tensor_transfer(bool server_mode) {
    if (g_tensor_transfer_manager) {
        return seastar::make_ready_future<>();
    }

    g_tensor_transfer_manager = std::make_unique<TensorTransferManager>(server_mode);
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

} // namespace rpc
