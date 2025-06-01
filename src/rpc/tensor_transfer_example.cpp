#include <boost/program_options.hpp>
#include <iostream>
#include <memory>
#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <ucxx/api.h>
#include <ucxx/worker.h>
#include <ucxx/endpoint.h>
#include <ucxx/buffer.h>

using namespace seastar;
namespace bpo = boost::program_options;

// Simple tensor specification
struct SimpleTensorSpec {
    std::vector<size_t> shape;
    size_t element_size;

    size_t total_bytes() const {
        size_t total = element_size;
        for (auto dim : shape) {
            total *= dim;
        }
        return total;
    }
};

// Server function
future<> run_server(uint16_t port) {
    std::cout << "Starting tensor transfer server on port " << port << "\n";

    // Create UCXX context and worker
    auto context = ucxx::createContext({}, ucxx::Context::defaultFeatureFlags);
    auto worker = context->createWorker();

    // Create a listener
    auto listener = worker->createListener(port,
        [](ucp_conn_request_h conn_request, void* arg) {
            auto worker_ptr = static_cast<ucxx::Worker*>(arg);
            std::cout << "Server received a connection request" << std::endl;

            // Create an endpoint from the connection request
            auto listener_ptr = static_cast<ucxx::Listener*>(arg);
            auto endpoint = listener_ptr->createEndpointFromConnRequest(conn_request, true);

            // Receive tensor metadata
            std::vector<size_t> shape_buffer(3);
            size_t element_size;

            // Create a request for the receive operation
            auto shape_request = worker_ptr->tagRecv(shape_buffer.data(), shape_buffer.size() * sizeof(size_t), ucxx::Tag{1}, ucxx::TagMaskFull);
            auto element_size_request = worker_ptr->tagRecv(&element_size, sizeof(element_size), ucxx::Tag{2}, ucxx::TagMaskFull);

            // Wait for the requests to complete
            while (!shape_request->isCompleted() || !element_size_request->isCompleted()) {
                worker_ptr->progress();
            }

            // Check for errors
            shape_request->checkError();
            element_size_request->checkError();

            // Create tensor spec
            SimpleTensorSpec spec;
            spec.shape = shape_buffer;
            spec.element_size = element_size;

            std::cout << "Server received tensor metadata:\n";
            std::cout << "  Shape: [";
            for (size_t i = 0; i < spec.shape.size(); ++i) {
                if (i > 0) std::cout << ", ";
                std::cout << spec.shape[i];
            }
            std::cout << "]\n";
            std::cout << "  Element size: " << spec.element_size << " bytes\n";
            std::cout << "  Total size: " << spec.total_bytes() << " bytes\n";

            // Allocate a buffer for the tensor data
            auto buffer = ucxx::allocateBuffer(ucxx::BufferType::Host, spec.total_bytes());

            // Receive the tensor data
            auto data_request = worker_ptr->tagRecv(buffer->data(), buffer->getSize(), ucxx::Tag{3}, ucxx::TagMaskFull);

            // Wait for the request to complete
            while (!data_request->isCompleted()) {
                worker_ptr->progress();
            }

            // Check for errors
            data_request->checkError();

            std::cout << "Server received tensor data successfully\n";

            // Send acknowledgment
            uint32_t ack = 1;
            auto ack_request = endpoint->tagSend(&ack, sizeof(ack), ucxx::Tag{4});

            // Wait for the request to complete
            while (!ack_request->isCompleted()) {
                worker_ptr->progress();
            }

            // Check for errors
            ack_request->checkError();

            std::cout << "Server sent acknowledgment\n";
        },
        worker.get());

    // Start the worker progress thread
    worker->startProgressThread(true);

    // Create a promise that will be fulfilled when the server should shut down
    auto keep_alive = std::make_shared<promise<>>();

    // In a real implementation, we would handle signals properly
    // For this example, we'll just wait for user input to shut down
    std::cout << "Press Enter to shut down the server...\n";
    std::thread([keep_alive] {
        std::cin.get();
        std::cout << "Shutting down...\n";
        keep_alive->set_value();
    }).detach();

    return keep_alive->get_future().then([worker, context] {
        // Stop the worker progress thread
        worker->stopProgressThread();

        return make_ready_future<>();
    });
}

// Client function
future<> run_client(const std::string& server_addr, uint16_t port) {
    std::cout << "Starting tensor transfer client, connecting to " << server_addr << ":" << port << "\n";

    // Create UCXX context and worker
    auto context = ucxx::createContext({}, ucxx::Context::defaultFeatureFlags);
    auto worker = context->createWorker();

    // Create an endpoint to the server
    auto endpoint = worker->createEndpointFromHostname(server_addr.c_str(), port, true);

    // Start the worker progress thread
    worker->startProgressThread(true);

    // Create a tensor specification
    SimpleTensorSpec spec;
    spec.shape = {2, 3, 4};  // 2x3x4 tensor
    spec.element_size = sizeof(float);

    // Allocate a buffer for the tensor data
    auto buffer = ucxx::allocateBuffer(ucxx::BufferType::Host, spec.total_bytes());

    // Fill the buffer with some test data
    float* data = reinterpret_cast<float*>(buffer->data());
    size_t num_elements = spec.total_bytes() / sizeof(float);
    for (size_t i = 0; i < num_elements; ++i) {
        data[i] = static_cast<float>(i);
    }

    std::cout << "Client sending tensor:\n";
    std::cout << "  Shape: [";
    for (size_t i = 0; i < spec.shape.size(); ++i) {
        if (i > 0) std::cout << ", ";
        std::cout << spec.shape[i];
    }
    std::cout << "]\n";
    std::cout << "  Element size: " << spec.element_size << " bytes\n";
    std::cout << "  Total size: " << spec.total_bytes() << " bytes\n";

    // Send tensor metadata
    auto shape_request = endpoint->tagSend(spec.shape.data(), spec.shape.size() * sizeof(size_t), ucxx::Tag{1});
    auto element_size_request = endpoint->tagSend(&spec.element_size, sizeof(spec.element_size), ucxx::Tag{2});

    // Wait for the requests to complete
    while (!shape_request->isCompleted() || !element_size_request->isCompleted()) {
        worker->progress();
    }

    // Check for errors
    shape_request->checkError();
    element_size_request->checkError();

    std::cout << "Client sent tensor metadata\n";

    // Send tensor data
    auto data_request = endpoint->tagSend(buffer->data(), buffer->getSize(), ucxx::Tag{3});

    // Wait for the request to complete
    while (!data_request->isCompleted()) {
        worker->progress();
    }

    // Check for errors
    data_request->checkError();

    std::cout << "Client sent tensor data\n";

    // Receive acknowledgment
    uint32_t ack = 0;
    auto ack_request = worker->tagRecv(&ack, sizeof(ack), ucxx::Tag{4}, ucxx::TagMaskFull);

    // Wait for the request to complete
    while (!ack_request->isCompleted()) {
        worker->progress();
    }

    // Check for errors
    ack_request->checkError();

    std::cout << "Client received acknowledgment: " << ack << "\n";

    // Stop the worker progress thread
    worker->stopProgressThread();

    return make_ready_future<>();
}

// Main function
int main(int ac, char** av) {
    app_template app;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(12345), "UCXX port")
        ("server", bpo::value<std::string>(), "Server address (client mode only)");

    return app.run_deprecated(ac, av, [&app] {
        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();

        if (config.count("server")) {
            std::string server_addr = config["server"].as<std::string>();
            return run_client(server_addr, port);
        } else {
            return run_server(port);
        }
    });
}
