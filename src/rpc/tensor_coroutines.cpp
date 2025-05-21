#include "rpc/rpc_common.hpp"
#include "coucxx/co_tag.hpp"
#include <ucxx/api.h>
#include <ucxx/worker.h>
#include <ucxx/endpoint.h>
#include <ucxx/buffer.h>
#include <iostream>
#include <memory>

/**
 * @brief Coroutine function to create a tensor using UCX tag operations
 *
 * This function demonstrates how to use coroutines with UCX tag operations
 * to create a tensor. In a real implementation, this would involve more
 * complex communication between client and server.
 *
 * @param spec The tensor specification
 * @param worker The UCX worker to use for communication
 * @return A task that resolves to the created buffer
 */
coucxx::coroutine::task<std::shared_ptr<ucxx::Buffer>> create_tensor_coroutine(
    const TensorSpec& spec, std::shared_ptr<ucxx::Worker> worker) {

    std::cout << "Creating tensor with coroutine...\n";

    // Allocate the buffer using UCXX's allocateBuffer function
    auto buffer = ucxx::allocateBuffer(BufferType::RMM, spec.total_bytes());

    // In a real implementation, we would use tag operations to communicate
    // with the client, for example:
    //
    // // Receive tensor data from client
    // auto recv_awaitable = coucxx::coroutine::tag_recv(
    //     worker, buffer->data(), buffer->size(), ucxx::Tag{42});
    // size_t received_bytes = co_await recv_awaitable;
    //
    // // Send acknowledgment back to client
    // int ack = 1;
    // co_await coucxx::coroutine::tag_send(
    //     endpoint, &ack, sizeof(ack), ucxx::Tag{43});

    // For now, just return the allocated buffer
    co_return buffer;
}
