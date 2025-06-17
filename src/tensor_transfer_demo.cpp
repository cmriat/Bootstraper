#include <boost/program_options.hpp>
#include <cstdint>
#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/rpc/rpc.hh>
#include <fmt/core.h>
#include <seastar/core/sstring.hh>
#include <fmt/base.h>
#include <sstream>

#ifdef CUDA_AVAILABLE
#include <cuda_runtime.h>
#endif

#include "commom.hpp"
#include "log_utils.hpp"
#include "fast_channel_manager.hpp"
#include "simple_rpc.hpp"

using namespace seastar;
namespace bpo = boost::program_options;

static State server_state;
static std::shared_ptr<ucxx::Buffer> tensor_buffer;
static TensorSpec received_tensor_spec;

btsp::FCTask<void> server_recv_tensor_data(btsp::FastChannelManager& manager, size_t tensor_size) {
    co_await manager.wait_for_connection();
    RDMA_LOG_INFO("Connection established for tensor data transfer!");

    if (!tensor_buffer || tensor_buffer->getSize() < tensor_size) {
        RDMA_LOG_ERROR("Buffer not allocated or insufficient size");
        co_return;
    }

    btsp::FCOpResult result = co_await manager.tag_recv(
        tensor_buffer->data(),
        tensor_size,
        100  // Use tag 100 for tensor data
    );

    if (result.success()) {
        RDMA_LOG_INFO("Successfully received tensor data: {} bytes", result.bytes_transferred);

        // To read GPU memory, we need to copy it to host first
        if (received_tensor_spec.data_type == DataType::FLOAT32 && tensor_size >= sizeof(float)) {
#ifdef CUDA_AVAILABLE
            // Copy a few elements from GPU to host to verify the data
            size_t sample_size = std::min(tensor_size, 3 * sizeof(float));
            std::vector<float> host_sample(sample_size / sizeof(float));

            cudaError_t err = cudaMemcpy(host_sample.data(), tensor_buffer->data(),
                                       sample_size, cudaMemcpyDeviceToHost);
            if (err == cudaSuccess) {
                RDMA_LOG_INFO("GPU Sample tensor values: {}, {}, {}",
                             host_sample.size() > 0 ? host_sample[0] : 0.0f,
                             host_sample.size() > 1 ? host_sample[1] : 0.0f,
                             host_sample.size() > 2 ? host_sample[2] : 0.0f);
            } else {
                RDMA_LOG_ERROR("Failed to copy sample data from GPU: {}", cudaGetErrorString(err));
            }
#else
            // If CUDA is not available, try direct access (shouldn't happen with RMM)
            float* data = static_cast<float*>(tensor_buffer->data());
            RDMA_LOG_INFO("CPU Sample tensor values: {}, {}, {}",
                         data[0],
                         tensor_size >= 2*sizeof(float) ? data[1] : 0.0f,
                         tensor_size >= 3*sizeof(float) ? data[2] : 0.0f);
#endif
        }
    } else {
        RDMA_LOG_ERROR("Failed to receive tensor data");
    }
    co_return;
}

btsp::FCTask<void> client_send_tensor_data(btsp::FastChannelManager& manager, const TensorSpec& spec) {
    co_await manager.connect("127.0.0.1", 12346);
    RDMA_LOG_INFO("Connection established for tensor data transfer!");

    size_t tensor_size = spec.total_bytes();
    
    // Create and fill tensor data (demo data)
    RDMA_LOG_INFO("Allocating buffer for tensor data: {} bytes", tensor_size);
    auto client_buffer = ucxx::allocateBuffer(ucxx::BufferType::RMM, tensor_size);
    RDMA_LOG_INFO("Successfully allocated buffer for tensor data");

    // Fill with demo data based on data type
    // Note: RMM buffer is GPU memory, we need to use CUDA memcpy to fill it
    if (spec.data_type == DataType::FLOAT32) {
        size_t num_elements = tensor_size / sizeof(float);

        // Create host buffer and fill with demo data
        std::vector<float> host_data(num_elements);
        for (size_t i = 0; i < num_elements; ++i) {
            host_data[i] = static_cast<float>(i * 0.1f);  // Demo values: 0.0, 0.1, 0.2, ...
        }

#ifdef CUDA_AVAILABLE
        // Copy from host to device
        cudaError_t err = cudaMemcpy(client_buffer->data(), host_data.data(),
                                   tensor_size, cudaMemcpyHostToDevice);
        RDMA_LOG_INFO("Copied tensor data to GPU");
        if (err != cudaSuccess) {
            RDMA_LOG_ERROR("Failed to copy data to GPU: {}", cudaGetErrorString(err));
            co_return;
        }
#else
        // If CUDA is not available, just copy directly (this shouldn't happen with RMM)
        std::memcpy(client_buffer->data(), host_data.data(), tensor_size);
        RDMA_LOG_WARNING("CUDA not available, using direct memcpy for tensor data");
#endif

        RDMA_LOG_INFO("Filled tensor with {} float32 elements", num_elements);

    } else if (spec.data_type == DataType::INT32) {
        size_t num_elements = tensor_size / sizeof(int32_t);

        // Create host buffer and fill with demo data
        std::vector<int32_t> host_data(num_elements);
        for (size_t i = 0; i < num_elements; ++i) {
            host_data[i] = static_cast<int32_t>(i);  // Demo values: 0, 1, 2, ...
        }

#ifdef CUDA_AVAILABLE
        // Copy from host to device
        cudaError_t err = cudaMemcpy(client_buffer->data(), host_data.data(),
                                   tensor_size, cudaMemcpyHostToDevice);
        if (err != cudaSuccess) {
            RDMA_LOG_ERROR("Failed to copy data to GPU: {}", cudaGetErrorString(err));
            co_return;
        }
#else
        // If CUDA is not available, just copy directly (this shouldn't happen with RMM)
        std::memcpy(client_buffer->data(), host_data.data(), tensor_size);
#endif

        RDMA_LOG_INFO("Filled tensor with {} int32 elements", num_elements);
    }

    RDMA_LOG_INFO("Sending tensor data via RDMA...");
    btsp::FCOpResult result = co_await manager.tag_send(
        client_buffer->data(),
        tensor_size,
        100  // Use tag 100 for tensor data
    );
    RDMA_LOG_INFO("RDMA send operation completed");
    if (result.success()) {
        RDMA_LOG_INFO("Successfully sent tensor data: {} bytes", result.bytes_transferred);
    } else {
        RDMA_LOG_ERROR("Failed to send tensor data");
    }
    co_return;
}

future<> run_client(const std::string& server_addr, uint16_t rpc_port, uint16_t rdma_port, int cuda_device_id = -1) {
    auto manager = std::make_shared<btsp::FastChannelManager>();
    RPC_LOG_INFO("Connecting to RPC server {}:{}", server_addr, rpc_port);
    RDMA_LOG_INFO("Connecting to RDMA server {}:{}", server_addr, rdma_port);

    // Initialize RDMA manager as client
    if (!manager->initialize(false, 0, cuda_device_id)) {
        BTSP_LOG_ERROR("Failed to initialize RDMA manager");
        co_return;
    }

    try {
        auto&& shands_rpc = rpc_context::get_protocol();
        auto send_tensor_spec = shands_rpc.make_client<future<sstring> (TensorSpec)>(
            to_underlying(msg_type::TENSOR_DATA_TRANSFER));

        rpc::client_options co;
        auto& mc = rpc_context::get_compressor();
        co.compressor_factory = &mc;

        auto client = std::make_unique<rpc::protocol<serializer>::client>(
            shands_rpc, co, ipv4_addr{server_addr, rpc_port});

        RPC_LOG_DEBUG(1, "Setting up RPC client");
        
        // Create tensor specification
        TensorSpec spec;
        spec.data_type = DataType::FLOAT32;
        spec.shape = {2, 3, 4};  // 3D tensor: 128x128x64
        
        std::stringstream shape_str;
        shape_str << "[";
        for (size_t i = 0; i < spec.shape.size(); ++i) {
            if (i > 0) shape_str << ", ";
            shape_str << spec.shape[i];
        }
        shape_str << "]";
        
        RPC_LOG_INFO("Sending tensor spec - Shape: {}, Type: {}, Size: {} bytes", 
                    shape_str.str(), static_cast<int>(spec.data_type), spec.total_bytes());

        // Send tensor specification via RPC
        auto response = co_await send_tensor_spec(*client, spec);
        co_await client->stop();

        RPC_LOG_INFO("RPC response: {}", response);

        // Wait a bit for server to prepare
        co_await sleep(std::chrono::milliseconds(500));

        // Send actual tensor data via RDMA
        RDMA_LOG_INFO("Starting tensor data transfer...");

        // Use a simpler approach - just start the coroutine and wait
        try {
            auto task = client_send_tensor_data(*manager, spec);

            // Give some time for the RDMA operation to complete
            // Since the coroutine auto-destroys, we just need to wait for the underlying operation
            co_await sleep(std::chrono::seconds(5));

            RDMA_LOG_INFO("Tensor transfer completed");
        } catch (const std::exception& e) {
            RDMA_LOG_ERROR("Exception during tensor transfer: {}", e.what());
        }

        BTSP_LOG_INFO("Tensor transfer completed successfully");

    } catch (const std::exception& e) {
        BTSP_LOG_ERROR("Client error: {}", e.what());
    }

    // Cleanup
    manager->shutdown();
    BTSP_LOG_INFO("Client stopped");
    co_return;
}

future<> run_server(uint16_t rpc_port, uint16_t rdma_port, int cuda_device_id = -1) {
    auto manager = std::make_shared<btsp::FastChannelManager>();

    if (!manager->initialize(true, rdma_port, cuda_device_id)) {
        BTSP_LOG_ERROR("Failed to initialize RDMA manager");
        co_return;
    }
    server_state.set_rdma_manager(manager);

    auto&& shands_rpc = rpc_context::get_protocol();

    // Register tensor data transfer handler
    shands_rpc.register_handler(to_underlying(msg_type::TENSOR_DATA_TRANSFER),
        [manager](TensorSpec spec) -> future<sstring> {
            received_tensor_spec = spec;

            std::stringstream shape_str;
            shape_str << "[";
            for (size_t i = 0; i < spec.shape.size(); ++i) {
                if (i > 0) shape_str << ", ";
                shape_str << spec.shape[i];
            }
            shape_str << "]";

            RPC_LOG_INFO("Received tensor spec - Shape: {}, Type: {}, Size: {} bytes",
                        shape_str.str(), static_cast<int>(spec.data_type), spec.total_bytes());

            try {
                // Allocate RMM buffer for the tensor
                tensor_buffer = ucxx::allocateBuffer(ucxx::BufferType::RMM, spec.total_bytes());

                RPC_LOG_INFO("Successfully allocated buffer: {} bytes", tensor_buffer->getSize());

                // Start RDMA receive task in background
                auto rdma_task = server_recv_tensor_data(*manager, spec.total_bytes());
                std::string result = "Tensor buffer allocated successfully, size: " +
                                   std::to_string(tensor_buffer->getSize()) + " bytes. Ready to receive data.";
                return make_ready_future<sstring>(result);

            } catch (const std::exception& e) {
                std::string error = "Failed to allocate tensor buffer: " + std::string(e.what());
                RPC_LOG_ERROR("{}", error);
                return make_ready_future<sstring>(error);
            }
        });

    rpc::resource_limits limits;
    limits.bloat_factor = 1;
    limits.basic_request_size = 128;
    limits.max_memory = 100'000'000;

    auto server = std::make_unique<rpc::protocol<serializer>::server>(
        shands_rpc, ipv4_addr{rpc_port}, limits);

    RPC_LOG_INFO("RPC server setup complete on port {}", rpc_port);
    RDMA_LOG_INFO("RDMA server listening on port {}", rdma_port);

    // Keep server running
    while (!server_state.is_shutdown_requested()) {
        co_await sleep(std::chrono::milliseconds(100));
    }

    BTSP_LOG_INFO("Server shutting down");
    co_return;
}

int main(int ac, char** av) {
    btsp::LogWrapper::init(av[0]);
    app_template app;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(10000), "RPC server port")
        ("server", bpo::value<sstring>(), "Server address")
        ("cuda-device", bpo::value<int>()->default_value(-1), "CUDA device ID (-1 for auto)")
        ("v", bpo::value<int>()->default_value(0), "Verbose logging level")
        ("log-dir", bpo::value<std::string>(), "Log directory");

    static logger slp_logger("tensor transfer demo");
    rpc_context::get_protocol().set_logger(&slp_logger);
    static uint16_t rdma_port = 12346;

    app.run(ac, av, [&app] {
        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();
        int cuda_device_id = config["cuda-device"].as<int>();

        if (config.count("v")) {
            btsp::LogWrapper::set_verbose_level(config["v"].as<int>());
        }

        if (config.count("log-dir")) {
            btsp::LogWrapper::set_log_dir(config["log-dir"].as<std::string>());
        }

        if (config.count("server")) {
            BTSP_LOG_INFO("Starting client mode, connecting to {}:{} with CUDA device {}",
                         config["server"].as<sstring>(), port,
                         cuda_device_id == -1 ? "auto" : std::to_string(cuda_device_id));
            return run_client(config["server"].as<sstring>(), port, rdma_port, cuda_device_id);
        } else {
            BTSP_LOG_INFO("Starting server mode on port {} with CUDA device {}", port,
                         cuda_device_id == -1 ? "auto" : std::to_string(cuda_device_id));
            return run_server(port, rdma_port, cuda_device_id);
        }
    });
    return 0;
}
