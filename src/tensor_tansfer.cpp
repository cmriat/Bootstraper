#include <boost/program_options.hpp>
#include <cstdint>
#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <fmt/core.h>
#include <seastar/core/sstring.hh>
#include <fmt/base.h>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <numeric>
#include <algorithm>
#include <thread>
#include <atomic>
#include <signal.h>

#ifdef CUDA_AVAILABLE
#include <cuda_runtime.h>
#endif

#include "commom.hpp"
#include "log_utils.hpp"
#include "fast_channel_manager.hpp"

using namespace seastar;
namespace bpo = boost::program_options;

// Buffer data type for transfer testing
enum buffer_data_type_t {
    BUFFER_TYPE_FLOAT32,     // Original FLOAT32 with incremental values (default)
    BUFFER_TYPE_CHAR_0XAA,   // char type initialized to 0xaa
    BUFFER_TYPE_EMPTY        // Empty buffer (no initialization)
};

// Simple tensor specification for direct transfer
struct SimpleTensorSpec {
    uint32_t data_type;  // 0=FLOAT32, 2=INT32
    std::vector<uint64_t> shape;
    buffer_data_type_t buffer_type = BUFFER_TYPE_FLOAT32;  // Buffer initialization type

    size_t total_bytes() const {
        if (shape.empty()) return 0;

        size_t total_elements = 1;
        for (auto dim : shape) {
            total_elements *= dim;
        }

        size_t element_size = (data_type == 0) ? 4 : 4;  // FLOAT32 or INT32
        return total_elements * element_size;
    }
};

// Performance measurement structure
struct TransferTiming {
    std::chrono::high_resolution_clock::time_point operation_start;
    std::chrono::high_resolution_clock::time_point transfer_start;
    std::chrono::high_resolution_clock::time_point transfer_end;
    std::chrono::high_resolution_clock::time_point operation_end;
    size_t bytes_transferred = 0;
    bool success = false;

    double operation_duration_us() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(operation_end - operation_start).count();
    }

    double transfer_duration_us() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(transfer_end - transfer_start).count();
    }

    double bandwidth_gbps() const {
        if (transfer_duration_us() <= 0 || bytes_transferred == 0) return 0.0;
        double duration_sec = transfer_duration_us() / 1e6;
        double bytes_per_sec = bytes_transferred / duration_sec;
        return bytes_per_sec / (1024.0 * 1024.0 * 1024.0);  // Convert to GB/s
    }

    double throughput_mbps() const {
        if (transfer_duration_us() <= 0 || bytes_transferred == 0) return 0.0;
        double duration_sec = transfer_duration_us() / 1e6;
        double bytes_per_sec = bytes_transferred / duration_sec;
        return bytes_per_sec / (1024.0 * 1024.0);  // Convert to MB/s
    }
};

static std::atomic<bool> should_shutdown{false};
static std::shared_ptr<ucxx::Buffer> tensor_buffer;
static TransferTiming server_timing;
static TransferTiming client_timing;
static btsp::FastChannelManager* global_server_manager = nullptr;

// Function to print comprehensive performance report
void print_performance_report(const SimpleTensorSpec& spec, btsp::FastChannelManager* client_manager = nullptr, btsp::FastChannelManager* server_manager = nullptr) {
    size_t tensor_size = spec.total_bytes();
    double size_gb = tensor_size / (1024.0 * 1024.0 * 1024.0);

    RDMA_LOG_INFO("========================================");
    RDMA_LOG_INFO("       TENSOR TRANSFER PERFORMANCE     ");
    RDMA_LOG_INFO("========================================");
    RDMA_LOG_INFO("Tensor size: {} bytes ({:.3f} GB)", tensor_size, size_gb);
    RDMA_LOG_INFO("Data type: {}", spec.data_type == 0 ? "FLOAT32" : "INT32");

    std::stringstream shape_str;
    shape_str << "[";
    for (size_t i = 0; i < spec.shape.size(); ++i) {
        if (i > 0) shape_str << ", ";
        shape_str << spec.shape[i];
    }
    shape_str << "]";
    RDMA_LOG_INFO("Shape: {}", shape_str.str());

    // Only client-side timing for bandwidth calculation
    if (client_timing.success) {
        RDMA_LOG_INFO("----------------------------------------");
        RDMA_LOG_INFO("CLIENT SEND METRICS (for bandwidth calculation):");
        RDMA_LOG_INFO("  Transfer time: {:.3f} us", client_timing.transfer_duration_us());
        RDMA_LOG_INFO("  Bandwidth: {:.6f} GB/s", client_timing.bandwidth_gbps());
        RDMA_LOG_INFO("  Throughput: {:.3f} MB/s", client_timing.throughput_mbps());
    }

    // UCX-level timing (client-side only for bandwidth calculation)
    if (client_manager) {
        auto ucx_send_timing = client_manager->get_last_send_timing();
        if (ucx_send_timing.completed) {
            RDMA_LOG_INFO("----------------------------------------");
            RDMA_LOG_INFO("CLIENT SEND METRICS (UCX Level - for bandwidth calculation):");
            RDMA_LOG_INFO("  UCX operation time: {:.3f} us", ucx_send_timing.duration_us());
            RDMA_LOG_INFO("  UCX bandwidth: {:.6f} GB/s", ucx_send_timing.bandwidth_gbps());
            RDMA_LOG_INFO("  Tag: {}", ucx_send_timing.tag);
        }
    }

    // No end-to-end metrics since we only track client-side timing

    RDMA_LOG_INFO("========================================");
}

btsp::FCTask<void> server_recv_tensor_data(btsp::FastChannelManager& manager, const SimpleTensorSpec& spec) {
    // No timing on server side - only client-side timing for bandwidth calculation
    co_await manager.wait_for_connection();
    RDMA_LOG_INFO("Connection established for tensor data transfer!");

    size_t tensor_size = spec.total_bytes();

    // Allocate buffer for the tensor (reuse across multiple receives)
    try {
        tensor_buffer = ucxx::allocateBuffer(ucxx::BufferType::RMM, tensor_size);
        RDMA_LOG_INFO("Successfully allocated buffer: {} bytes", tensor_buffer->getSize());
    } catch (const std::exception& e) {
        RDMA_LOG_ERROR("Failed to allocate tensor buffer: {}", e.what());
        server_timing.success = false;
        server_timing.operation_end = std::chrono::high_resolution_clock::now();
        co_return;
    }

    // Loop to handle multiple receives
    size_t receive_count = 0;
    while (!should_shutdown.load()) {
        receive_count++;
        RDMA_LOG_INFO("=== Starting receive operation #{} ===", receive_count);

        // No timing on server side - just perform the receive operation
        RDMA_LOG_INFO("Starting tag_recv operation");

        btsp::FCOpResult result = co_await manager.tag_recv(
            tensor_buffer->data(),
            tensor_size,
            100  // Use tag 100 for tensor data
        );

        RDMA_LOG_INFO("Completed tag_recv operation");

        if (result.success()) {
            RDMA_LOG_INFO("Successfully received tensor data: {} bytes", result.bytes_transferred);
            // No performance metrics on server side - only client-side timing matters for bandwidth calculation

            // To read GPU memory, we need to copy it to host first
            // Display sample data based on buffer type
            switch (spec.buffer_type) {
                case BUFFER_TYPE_FLOAT32: {
                    if (spec.data_type == 0 && tensor_size >= sizeof(float)) {  // FLOAT32
#ifdef CUDA_AVAILABLE
                        // Copy a few elements from GPU to host to verify the data
                        size_t sample_size = std::min(tensor_size, 3 * sizeof(float));
                        std::vector<float> host_sample(sample_size / sizeof(float));

                        cudaError_t err = cudaMemcpy(host_sample.data(), tensor_buffer->data(),
                                                   sample_size, cudaMemcpyDeviceToHost);
                        if (err == cudaSuccess) {
                            RDMA_LOG_INFO("GPU Sample FLOAT32 values: {}, {}, {}",
                                         host_sample.size() > 0 ? host_sample[0] : 0.0f,
                                         host_sample.size() > 1 ? host_sample[1] : 0.0f,
                                         host_sample.size() > 2 ? host_sample[2] : 0.0f);
                        } else {
                            RDMA_LOG_ERROR("Failed to copy sample FLOAT32 data from GPU: {}", cudaGetErrorString(err));
                        }
#else
                        // If CUDA is not available, try direct access (shouldn't happen with RMM)
                        float* data = static_cast<float*>(tensor_buffer->data());
                        RDMA_LOG_INFO("CPU Sample FLOAT32 values: {}, {}, {}",
                                     data[0],
                                     tensor_size >= 2*sizeof(float) ? data[1] : 0.0f,
                                     tensor_size >= 3*sizeof(float) ? data[2] : 0.0f);
#endif
                    }
                    break;
                }

                case BUFFER_TYPE_CHAR_0XAA: {
#ifdef CUDA_AVAILABLE
                    // Copy a few bytes from GPU to host to verify the data
                    size_t sample_size = std::min(tensor_size, static_cast<size_t>(3));
                    std::vector<char> host_sample(sample_size);

                    cudaError_t err = cudaMemcpy(host_sample.data(), tensor_buffer->data(),
                                               sample_size, cudaMemcpyDeviceToHost);
                    if (err == cudaSuccess) {
                        RDMA_LOG_INFO("GPU Sample char values: 0x{:02x}, 0x{:02x}, 0x{:02x}",
                                     host_sample.size() > 0 ? static_cast<unsigned char>(host_sample[0]) : 0,
                                     host_sample.size() > 1 ? static_cast<unsigned char>(host_sample[1]) : 0,
                                     host_sample.size() > 2 ? static_cast<unsigned char>(host_sample[2]) : 0);
                    } else {
                        RDMA_LOG_ERROR("Failed to copy sample char data from GPU: {}", cudaGetErrorString(err));
                    }
#else
                    // If CUDA is not available, try direct access (shouldn't happen with RMM)
                    char* data = static_cast<char*>(tensor_buffer->data());
                    RDMA_LOG_INFO("CPU Sample char values: 0x{:02x}, 0x{:02x}, 0x{:02x}",
                                 static_cast<unsigned char>(data[0]),
                                 tensor_size >= 2 ? static_cast<unsigned char>(data[1]) : 0,
                                 tensor_size >= 3 ? static_cast<unsigned char>(data[2]) : 0);
#endif
                    break;
                }

                case BUFFER_TYPE_EMPTY: {
                    RDMA_LOG_INFO("Empty buffer received (no sample data to display)");
                    break;
                }
            }

            // Print comprehensive performance report including UCX timing
            if (global_server_manager) {
                print_performance_report(spec, nullptr, global_server_manager);
            }

            RDMA_LOG_INFO("=== Completed receive operation #{} ===", receive_count);
        } else {
            RDMA_LOG_ERROR("Failed to receive tensor data in operation #{}", receive_count);
            // Don't break on failure, continue waiting for next transfer
        }

        // Small delay before next receive to allow client to prepare next iteration
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    RDMA_LOG_INFO("Server receive loop terminated after {} operations", receive_count);
    co_return;
}

btsp::FCTask<void> client_send_tensor_data(btsp::FastChannelManager& manager, const SimpleTensorSpec& spec, const std::string& server_addr, uint16_t port) {
    co_await manager.connect(server_addr, port);
    RDMA_LOG_INFO("Connection established for tensor data transfer!");

    size_t tensor_size = spec.total_bytes();

    // Create and fill tensor data (demo data)
    RDMA_LOG_INFO("Allocating buffer for tensor data: {} bytes", tensor_size);
    auto client_buffer = ucxx::allocateBuffer(ucxx::BufferType::RMM, tensor_size);
    RDMA_LOG_INFO("Successfully allocated buffer for tensor data");

    // Fill with data based on buffer type
    // Note: RMM buffer is GPU memory, we need to use CUDA memcpy to fill it
    switch (spec.buffer_type) {
        case BUFFER_TYPE_FLOAT32: {
            if (spec.data_type == 0) {  // FLOAT32
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
                RDMA_LOG_INFO("Copied FLOAT32 tensor data to GPU");
                if (err != cudaSuccess) {
                    RDMA_LOG_ERROR("Failed to copy FLOAT32 data to GPU: {}", cudaGetErrorString(err));
                    co_return;
                }
#else
                // If CUDA is not available, just copy directly (this shouldn't happen with RMM)
                std::memcpy(client_buffer->data(), host_data.data(), tensor_size);
                RDMA_LOG_WARNING("CUDA not available, using direct memcpy for FLOAT32 tensor data");
#endif

                RDMA_LOG_INFO("Filled tensor with {} float32 elements", num_elements);

            } else if (spec.data_type == 2) {  // INT32
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
                    RDMA_LOG_ERROR("Failed to copy INT32 data to GPU: {}", cudaGetErrorString(err));
                    co_return;
                }
#else
                // If CUDA is not available, just copy directly (this shouldn't happen with RMM)
                std::memcpy(client_buffer->data(), host_data.data(), tensor_size);
#endif

                RDMA_LOG_INFO("Filled tensor with {} int32 elements", num_elements);
            }
            break;
        }

        case BUFFER_TYPE_CHAR_0XAA: {
            // Fill with char type initialized to 0xaa
            std::vector<char> host_data(tensor_size);
            for (size_t i = 0; i < tensor_size; ++i) {
                host_data[i] = static_cast<char>(0xaa);
            }

#ifdef CUDA_AVAILABLE
            // Copy from host to device
            cudaError_t err = cudaMemcpy(client_buffer->data(), host_data.data(),
                                       tensor_size, cudaMemcpyHostToDevice);
            RDMA_LOG_INFO("Copied char 0xaa data to GPU");
            if (err != cudaSuccess) {
                RDMA_LOG_ERROR("Failed to copy char 0xaa data to GPU: {}", cudaGetErrorString(err));
                co_return;
            }
#else
            // If CUDA is not available, just copy directly (this shouldn't happen with RMM)
            std::memcpy(client_buffer->data(), host_data.data(), tensor_size);
            RDMA_LOG_WARNING("CUDA not available, using direct memcpy for char 0xaa data");
#endif

            RDMA_LOG_INFO("Filled buffer with {} char bytes (0xaa pattern)", tensor_size);
            break;
        }

        case BUFFER_TYPE_EMPTY: {
            // Empty buffer - no initialization
            RDMA_LOG_INFO("Using empty buffer (no initialization) - {} bytes", tensor_size);
            break;
        }
    }

    RDMA_LOG_INFO("Sending tensor data via RDMA...");

    // Record transfer start time (just before tag_send)
    client_timing.transfer_start = std::chrono::high_resolution_clock::now();
    RDMA_LOG_INFO("Starting tag_send operation at {} us",
                  std::chrono::duration_cast<std::chrono::microseconds>(
                      client_timing.transfer_start.time_since_epoch()).count());

    btsp::FCOpResult result = co_await manager.tag_send(
        client_buffer->data(),
        tensor_size,
        100  // Use tag 100 for tensor data
    );

    // Record transfer end time (immediately after tag_send completes)
    client_timing.transfer_end = std::chrono::high_resolution_clock::now();

    RDMA_LOG_INFO("Completed tag_send operation at {} us",
                  std::chrono::duration_cast<std::chrono::microseconds>(
                      client_timing.transfer_end.time_since_epoch()).count());

    RDMA_LOG_INFO("RDMA send operation completed");
    if (result.success()) {
        client_timing.success = true;
        client_timing.bytes_transferred = result.bytes_transferred;

        RDMA_LOG_INFO("Successfully sent tensor data: {} bytes", result.bytes_transferred);

        // Calculate and log performance metrics (client-side timing for bandwidth calculation)
        double transfer_time_us = client_timing.transfer_duration_us();
        double bandwidth_gbps = client_timing.bandwidth_gbps();
        double throughput_mbps = client_timing.throughput_mbps();

        RDMA_LOG_INFO("=== CLIENT SEND PERFORMANCE (for bandwidth calculation) ===");
        RDMA_LOG_INFO("Transfer time: {:.3f} us", transfer_time_us);
        RDMA_LOG_INFO("Bandwidth: {:.6f} GB/s", bandwidth_gbps);
        RDMA_LOG_INFO("Throughput: {:.3f} MB/s", throughput_mbps);
        RDMA_LOG_INFO("Bytes transferred: {} bytes", client_timing.bytes_transferred);
        RDMA_LOG_INFO("===========================================================");

        // Print detailed UCX call chain breakdown
        auto ucx_timing = manager.get_last_send_timing();
        if (ucx_timing.completed) {
            ucx_timing.print_call_chain_breakdown();
        }
    } else {
        client_timing.success = false;
        RDMA_LOG_ERROR("Failed to send tensor data");
    }
    co_return;
}

void run_client(const std::string& server_addr, uint16_t rdma_port, int cuda_device_id = -1,
                size_t iterations = 10, size_t warmup_iterations = 3, buffer_data_type_t buffer_type = BUFFER_TYPE_FLOAT32) {
    auto manager = std::make_shared<btsp::FastChannelManager>();
    RDMA_LOG_INFO("Connecting to RDMA server {}:{}", server_addr, rdma_port);

    // Initialize RDMA manager as client
    if (!manager->initialize(false, 0, cuda_device_id)) {
        BTSP_LOG_ERROR("Failed to initialize RDMA manager");
        return;
    }

    try {
        // Create tensor specification for 1GB data (no RPC needed)
        SimpleTensorSpec spec;
        spec.data_type = 0;  // FLOAT32 (4 bytes per element)
        // Calculate shape for approximately 1GB: 1GB / 4 bytes = 268,435,456 elements
        // Using shape [16384, 16384] = 268,435,456 elements = 1,073,741,824 bytes = 1GB
        spec.shape = {16384, 16384};  // 2D tensor for 1GB
        spec.buffer_type = buffer_type;

        std::stringstream shape_str;
        shape_str << "[";
        for (size_t i = 0; i < spec.shape.size(); ++i) {
            if (i > 0) shape_str << ", ";
            shape_str << spec.shape[i];
        }
        shape_str << "]";

        RDMA_LOG_INFO("Sending tensor - Shape: {}, Type: {}, Size: {} bytes",
                    shape_str.str(), spec.data_type, spec.total_bytes());

        // Display buffer type information
        std::string buffer_type_str;
        switch (spec.buffer_type) {
            case BUFFER_TYPE_FLOAT32:
                buffer_type_str = "FLOAT32 with incremental values";
                break;
            case BUFFER_TYPE_CHAR_0XAA:
                buffer_type_str = "char initialized to 0xaa";
                break;
            case BUFFER_TYPE_EMPTY:
                buffer_type_str = "empty buffer (no initialization)";
                break;
        }
        RDMA_LOG_INFO("Buffer type: {}", buffer_type_str);
        RDMA_LOG_INFO("Test configuration - Warmup iterations: {}, Test iterations: {}",
                    warmup_iterations, iterations);

        std::vector<double> transfer_times;
        std::vector<double> bandwidths;

        // Warmup iterations
        if (warmup_iterations > 0) {
            RDMA_LOG_INFO("Starting warmup iterations...");
            for (size_t i = 0; i < warmup_iterations; ++i) {
                RDMA_LOG_INFO("Warmup iteration {}/{}", i + 1, warmup_iterations);

                try {
                    auto task = client_send_tensor_data(*manager, spec, server_addr, rdma_port);
                    std::this_thread::sleep_for(std::chrono::seconds(5));
                    RDMA_LOG_INFO("Warmup iteration {} completed", i + 1);
                } catch (const std::exception& e) {
                    RDMA_LOG_ERROR("Warmup iteration {} failed: {}", i + 1, e.what());
                }

                // Small delay between iterations
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            RDMA_LOG_INFO("Warmup completed");
        }

        // Test iterations
        RDMA_LOG_INFO("Starting test iterations...");
        for (size_t i = 0; i < iterations; ++i) {
            RDMA_LOG_INFO("Test iteration {}/{}", i + 1, iterations);

            try {
                // Reset timing data for this iteration
                client_timing = TransferTiming{};

                auto task = client_send_tensor_data(*manager, spec, server_addr, rdma_port);
                std::this_thread::sleep_for(std::chrono::seconds(5));

                if (client_timing.success) {
                    double transfer_time_us = client_timing.transfer_duration_us();
                    double bandwidth_gbps = client_timing.bandwidth_gbps();

                    transfer_times.push_back(transfer_time_us);
                    bandwidths.push_back(bandwidth_gbps);

                    RDMA_LOG_INFO("Iteration {} - Transfer time: {:.3f} us, Bandwidth: {:.6f} GB/s",
                                i + 1, transfer_time_us, bandwidth_gbps);
                } else {
                    RDMA_LOG_ERROR("Iteration {} failed", i + 1);
                }
            } catch (const std::exception& e) {
                RDMA_LOG_ERROR("Test iteration {} failed: {}", i + 1, e.what());
            }

            // Small delay between iterations
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // Calculate and report statistics
        if (!transfer_times.empty()) {
            double avg_time = std::accumulate(transfer_times.begin(), transfer_times.end(), 0.0) / transfer_times.size();
            double avg_bandwidth = std::accumulate(bandwidths.begin(), bandwidths.end(), 0.0) / bandwidths.size();

            double min_time = *std::min_element(transfer_times.begin(), transfer_times.end());
            double max_time = *std::max_element(transfer_times.begin(), transfer_times.end());
            double min_bandwidth = *std::min_element(bandwidths.begin(), bandwidths.end());
            double max_bandwidth = *std::max_element(bandwidths.begin(), bandwidths.end());

            RDMA_LOG_INFO("=== PERFORMANCE SUMMARY ===");
            RDMA_LOG_INFO("Successful iterations: {}/{}", transfer_times.size(), iterations);
            RDMA_LOG_INFO("Average transfer time: {:.3f} us", avg_time);
            RDMA_LOG_INFO("Average bandwidth: {:.6f} GB/s", avg_bandwidth);
            RDMA_LOG_INFO("Min/Max transfer time: {:.3f}/{:.3f} us", min_time, max_time);
            RDMA_LOG_INFO("Min/Max bandwidth: {:.6f}/{:.6f} GB/s", min_bandwidth, max_bandwidth);
            RDMA_LOG_INFO("==========================");
        }

        // Print final performance report
        print_performance_report(spec, manager.get(), nullptr);

    } catch (const std::exception& e) {
        BTSP_LOG_ERROR("Client error: {}", e.what());
    }

    // Cleanup
    manager->shutdown();
    BTSP_LOG_INFO("Client stopped");
}

void run_server(uint16_t rdma_port, int cuda_device_id = -1, buffer_data_type_t buffer_type = BUFFER_TYPE_FLOAT32) {
    auto manager = std::make_shared<btsp::FastChannelManager>();

    if (!manager->initialize(true, rdma_port, cuda_device_id)) {
        BTSP_LOG_ERROR("Failed to initialize RDMA manager");
        return;
    }

    // Set global manager pointer for performance reporting
    global_server_manager = manager.get();

    RDMA_LOG_INFO("RDMA server listening on port {}", rdma_port);

    // Create tensor specification for 1GB data (matching client)
    SimpleTensorSpec spec;
    spec.data_type = 0;  // FLOAT32 (4 bytes per element)
    // Calculate shape for approximately 1GB: 1GB / 4 bytes = 268,435,456 elements
    // Using shape [16384, 16384] = 268,435,456 elements = 1,073,741,824 bytes = 1GB
    spec.shape = {16384, 16384};  // 2D tensor for 1GB
    spec.buffer_type = buffer_type;

    std::stringstream shape_str;
    shape_str << "[";
    for (size_t i = 0; i < spec.shape.size(); ++i) {
        if (i > 0) shape_str << ", ";
        shape_str << spec.shape[i];
    }
    shape_str << "]";

    RDMA_LOG_INFO("Expecting tensor - Shape: {}, Type: {}, Size: {} bytes",
                shape_str.str(), spec.data_type, spec.total_bytes());

    // Display buffer type information
    std::string buffer_type_str;
    switch (spec.buffer_type) {
        case BUFFER_TYPE_FLOAT32:
            buffer_type_str = "FLOAT32 with incremental values";
            break;
        case BUFFER_TYPE_CHAR_0XAA:
            buffer_type_str = "char initialized to 0xaa";
            break;
        case BUFFER_TYPE_EMPTY:
            buffer_type_str = "empty buffer (no initialization)";
            break;
    }
    RDMA_LOG_INFO("Expected buffer type: {}", buffer_type_str);

    try {
        // Start RDMA receive task using coroutine
        auto task = server_recv_tensor_data(*manager, spec);

        // Keep server running
        while (!should_shutdown.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

    } catch (const std::exception& e) {
        RDMA_LOG_ERROR("Server error: {}", e.what());
    }

    // Clear global manager pointer
    global_server_manager = nullptr;
    BTSP_LOG_INFO("Server shutting down");
}

int main(int ac, char** av) {
    btsp::LogWrapper::init(av[0]);

    // Parse command line arguments
    bpo::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("port,p", bpo::value<uint16_t>()->default_value(12346), "RDMA server port")
        ("server,s", bpo::value<std::string>(), "Server address (client mode)")
        ("cuda-device,c", bpo::value<int>()->default_value(-1), "CUDA device ID (-1 for auto)")
        ("iterations,n", bpo::value<size_t>()->default_value(10), "Number of iterations to run")
        ("warmup,w", bpo::value<size_t>()->default_value(3), "Number of warmup iterations")
        ("buffer-type,b", bpo::value<std::string>()->default_value("float32"),
         "Buffer data type: 'float32' (default), 'char', 'empty'\n"
         "  float32: FLOAT32 with incremental values\n"
         "  char: char type initialized to 0xaa\n"
         "  empty: no initialization (empty buffer)")
        ("verbose,v", bpo::value<int>()->default_value(0), "Verbose logging level")
        ("log-dir,l", bpo::value<std::string>(), "Log directory");

    bpo::variables_map vm;
    try {
        bpo::store(bpo::parse_command_line(ac, av, desc), vm);
        bpo::notify(vm);
    } catch (const std::exception& e) {
        std::cerr << "Error parsing command line: " << e.what() << std::endl;
        std::cerr << desc << std::endl;
        return 1;
    }

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    uint16_t port = vm["port"].as<uint16_t>();
    int cuda_device_id = vm["cuda-device"].as<int>();
    size_t iterations = vm["iterations"].as<size_t>();
    size_t warmup_iterations = vm["warmup"].as<size_t>();

    // Parse buffer type
    buffer_data_type_t buffer_type = BUFFER_TYPE_FLOAT32;
    std::string buffer_type_str = vm["buffer-type"].as<std::string>();
    if (buffer_type_str == "float32") {
        buffer_type = BUFFER_TYPE_FLOAT32;
    } else if (buffer_type_str == "char") {
        buffer_type = BUFFER_TYPE_CHAR_0XAA;
    } else if (buffer_type_str == "empty") {
        buffer_type = BUFFER_TYPE_EMPTY;
    } else {
        std::cerr << "Invalid buffer type: " << buffer_type_str << std::endl;
        std::cerr << "Valid options: float32, char, empty" << std::endl;
        return -1;
    }

    if (vm.count("verbose")) {
        btsp::LogWrapper::set_verbose_level(vm["verbose"].as<int>());
    }

    if (vm.count("log-dir")) {
        btsp::LogWrapper::set_log_dir(vm["log-dir"].as<std::string>());
    }

    // Setup signal handler for graceful shutdown
    signal(SIGINT, [](int) {
        std::cout << "\nReceived SIGINT, shutting down..." << std::endl;
        should_shutdown.store(true);
    });

    if (vm.count("server")) {
        std::string server_addr = vm["server"].as<std::string>();
        BTSP_LOG_INFO("Starting client mode, connecting to {}:{} with CUDA device {}, {} iterations, {} warmup",
                     server_addr, port,
                     cuda_device_id == -1 ? "auto" : std::to_string(cuda_device_id),
                     iterations, warmup_iterations);
        run_client(server_addr, port, cuda_device_id, iterations, warmup_iterations, buffer_type);
    } else {
        BTSP_LOG_INFO("Starting server mode on port {} with CUDA device {}", port,
                     cuda_device_id == -1 ? "auto" : std::to_string(cuda_device_id));
        run_server(port, cuda_device_id, buffer_type);
    }

    return 0;
}
