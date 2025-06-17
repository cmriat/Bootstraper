#include <benchmark/benchmark.h>
#include <memory>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <fmt/format.h>

#ifdef CUDA_AVAILABLE
#include <cuda_runtime.h>
#include <cuda.h>
#endif

#include "fast_channel_manager.hpp"
#include "log_utils.hpp"

// Global managers for benchmarks
static std::shared_ptr<btsp::FastChannelManager> g_server_manager;
static std::shared_ptr<btsp::FastChannelManager> g_client_manager;
static std::atomic<bool> g_benchmark_initialized{false};

// Benchmark configuration
struct BenchmarkConfig {
    int server_cuda_device = 0;
    int client_cuda_device = 1;
    uint16_t rdma_port = 12350;
    std::string server_addr = "127.0.0.1";
};

static BenchmarkConfig g_config;

// Initialize CUDA IPC benchmark environment
bool InitializeCudaIpcBenchmark() {
    if (g_benchmark_initialized.load()) {
        return true;
    }

#ifdef CUDA_AVAILABLE
    // Check if we have at least 2 CUDA devices for IPC
    int device_count = 0;
    cudaError_t err = cudaGetDeviceCount(&device_count);
    if (err != cudaSuccess || device_count < 2) {
        std::cerr << "CUDA IPC benchmark requires at least 2 CUDA devices, found: " << device_count << std::endl;
        return false;
    }

    std::cout << "Found " << device_count << " CUDA devices for IPC benchmark" << std::endl;
    
    // Validate device IDs
    if (g_config.server_cuda_device >= device_count || g_config.client_cuda_device >= device_count) {
        std::cerr << "Invalid CUDA device IDs: server=" << g_config.server_cuda_device 
                  << ", client=" << g_config.client_cuda_device << ", available: 0-" << (device_count-1) << std::endl;
        return false;
    }

    // Enable peer access between devices to avoid IPC issues
    cudaSetDevice(g_config.server_cuda_device);
    int can_access_peer = 0;
    err = cudaDeviceCanAccessPeer(&can_access_peer, g_config.server_cuda_device, g_config.client_cuda_device);
    if (err == cudaSuccess && can_access_peer) {
        cudaError_t peer_err = cudaDeviceEnablePeerAccess(g_config.client_cuda_device, 0);
        if (peer_err == cudaSuccess) {
            std::cout << "Enabled peer access from server device " << g_config.server_cuda_device
                      << " to client device " << g_config.client_cuda_device << std::endl;
        } else if (peer_err != cudaErrorPeerAccessAlreadyEnabled) {
            std::cerr << "Failed to enable peer access: " << cudaGetErrorString(peer_err) << std::endl;
        }
    }

    cudaSetDevice(g_config.client_cuda_device);
    err = cudaDeviceCanAccessPeer(&can_access_peer, g_config.client_cuda_device, g_config.server_cuda_device);
    if (err == cudaSuccess && can_access_peer) {
        cudaError_t peer_err = cudaDeviceEnablePeerAccess(g_config.server_cuda_device, 0);
        if (peer_err == cudaSuccess) {
            std::cout << "Enabled peer access from client device " << g_config.client_cuda_device
                      << " to server device " << g_config.server_cuda_device << std::endl;
        } else if (peer_err != cudaErrorPeerAccessAlreadyEnabled) {
            std::cerr << "Failed to enable peer access: " << cudaGetErrorString(peer_err) << std::endl;
        }
    }

    if (!can_access_peer) {
        std::cout << "Warning: CUDA devices " << g_config.server_cuda_device << " and "
                  << g_config.client_cuda_device << " may not support peer access" << std::endl;
    } else {
        std::cout << "CUDA devices support peer access for IPC" << std::endl;
    }
#else
    std::cerr << "CUDA not available, cannot run CUDA IPC benchmark" << std::endl;
    return false;
#endif

    // Initialize server manager
    g_server_manager = std::make_shared<btsp::FastChannelManager>();
    if (!g_server_manager->initialize(true, g_config.rdma_port, g_config.server_cuda_device)) {
        std::cerr << "Failed to initialize server manager" << std::endl;
        return false;
    }

    // Initialize client manager  
    g_client_manager = std::make_shared<btsp::FastChannelManager>();
    if (!g_client_manager->initialize(false, 0, g_config.client_cuda_device)) {
        std::cerr << "Failed to initialize client manager" << std::endl;
        return false;
    }

    g_benchmark_initialized = true;
    std::cout << "CUDA IPC benchmark environment initialized successfully" << std::endl;
    return true;
}

// Cleanup benchmark environment
void CleanupCudaIpcBenchmark() {
    if (g_server_manager) {
        g_server_manager->shutdown();
        g_server_manager.reset();
    }
    if (g_client_manager) {
        g_client_manager->shutdown();
        g_client_manager.reset();
    }
    g_benchmark_initialized = false;
}

// Server receive task
btsp::FCTask<bool> server_recv_task(size_t buffer_size, uint64_t tag) {
    try {
        // Allocate RMM buffer on server device
        auto recv_buffer = ucxx::allocateBuffer(ucxx::BufferType::RMM, buffer_size);
        
        // Wait for connection
        co_await g_server_manager->wait_for_connection();
        
        // Perform tag receive
        btsp::FCOpResult result = co_await g_server_manager->tag_recv(
            recv_buffer->data(), buffer_size, tag);
        
        co_return result.success();
    } catch (const std::exception& e) {
        std::cerr << "Server recv error: " << e.what() << std::endl;
        co_return false;
    }
}

// Client send task
btsp::FCTask<bool> client_send_task(size_t buffer_size, uint64_t tag) {
    try {
        // Allocate RMM buffer on client device
        auto send_buffer = ucxx::allocateBuffer(ucxx::BufferType::RMM, buffer_size);
        
        // Fill buffer with test data
#ifdef CUDA_AVAILABLE
        std::vector<char> host_data(buffer_size, 0x42);
        cudaError_t err = cudaMemcpy(send_buffer->data(), host_data.data(), 
                                   buffer_size, cudaMemcpyHostToDevice);
        if (err != cudaSuccess) {
            std::cerr << "Failed to copy data to GPU: " << cudaGetErrorString(err) << std::endl;
            co_return false;
        }
#endif
        
        // Connect to server
        co_await g_client_manager->connect(g_config.server_addr, g_config.rdma_port);
        
        // Perform tag send
        btsp::FCOpResult result = co_await g_client_manager->tag_send(
            send_buffer->data(), buffer_size, tag);
        
        co_return result.success();
    } catch (const std::exception& e) {
        std::cerr << "Client send error: " << e.what() << std::endl;
        co_return false;
    }
}

// Structure to hold operation results with timestamps
struct OperationResult {
    bool success = false;
    std::chrono::high_resolution_clock::time_point start_time;
    std::chrono::high_resolution_clock::time_point end_time;
    std::chrono::high_resolution_clock::time_point actual_transfer_start;
    std::chrono::high_resolution_clock::time_point actual_transfer_end;

    double duration_us() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
    }

    double actual_transfer_duration_us() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(actual_transfer_end - actual_transfer_start).count();
    }
};

// Structure for benchmark results export
struct BenchmarkResult {
    size_t buffer_size;
    double transfer_time_us;
    double bandwidth_gbps;
    double throughput_mbps;
    int server_device;
    int client_device;
    bool success;
    std::string timestamp;
};

// Global state for tracking operations with timestamps
static std::atomic<bool> g_server_recv_completed{false};
static std::atomic<bool> g_client_send_completed{false};
static OperationResult g_server_recv_result;
static OperationResult g_client_send_result;

// Global synchronization for accurate timing
static std::atomic<bool> g_server_ready{false};
static std::atomic<bool> g_transfer_started{false};
static std::vector<BenchmarkResult> g_benchmark_results;

// Global shared buffer for server receive
static std::shared_ptr<ucxx::Buffer> g_server_recv_buffer;

// Server receive coroutine with improved timing
btsp::FCTask<bool> server_recv_coroutine(size_t buffer_size, uint64_t tag) {
    try {
        // Wait for connection
        co_await g_server_manager->wait_for_connection();

        // Signal that server is ready
        g_server_ready = true;

        // Record operation start time (when server starts waiting for data)
        g_server_recv_result.start_time = std::chrono::high_resolution_clock::now();

        // Record actual transfer start time (server is ready to receive)
        g_server_recv_result.actual_transfer_start = std::chrono::high_resolution_clock::now();

        // Perform actual tag receive
        btsp::FCOpResult result = co_await g_server_manager->tag_recv(
            g_server_recv_buffer->data(),
            buffer_size,
            tag
        );

        // Record actual transfer end time (when data is fully received)
        g_server_recv_result.actual_transfer_end = std::chrono::high_resolution_clock::now();
        g_server_recv_result.end_time = g_server_recv_result.actual_transfer_end;
        g_server_recv_result.success = result.success();

        if (result.success()) {
            std::cout << "Server recv completed: " << result.bytes_transferred << " bytes" << std::endl;
        } else {
            std::cerr << "Server recv failed" << std::endl;
        }

        co_return result.success();
    } catch (const std::exception& e) {
        std::cerr << "Server recv coroutine error: " << e.what() << std::endl;
        g_server_recv_result.end_time = std::chrono::high_resolution_clock::now();
        g_server_recv_result.actual_transfer_end = g_server_recv_result.end_time;
        g_server_recv_result.success = false;
        co_return false;
    }
}

// Server receive operation that runs in background
void run_server_recv(size_t buffer_size, uint64_t tag) {
    g_server_recv_completed = false;
    g_server_recv_result = OperationResult{};

    try {
        // Allocate RMM buffer on server device
        g_server_recv_buffer = ucxx::allocateBuffer(ucxx::BufferType::RMM, buffer_size);

        // Start the coroutine task
        auto task = server_recv_coroutine(buffer_size, tag);

        // Wait for completion with timeout
        auto start = std::chrono::steady_clock::now();
        while ((std::chrono::steady_clock::now() - start) < std::chrono::seconds(10)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));

            // Check if operation completed by checking if end_time is set
            if (g_server_recv_result.end_time != std::chrono::high_resolution_clock::time_point{}) {
                g_server_recv_completed = true;
                break;
            }
        }

        if (!g_server_recv_completed) {
            std::cerr << "Server recv timeout" << std::endl;
            g_server_recv_result.success = false;
            g_server_recv_result.end_time = std::chrono::high_resolution_clock::now();
            g_server_recv_result.actual_transfer_end = g_server_recv_result.end_time;
            g_server_recv_completed = true;
        }

    } catch (const std::exception& e) {
        std::cerr << "Server recv error: " << e.what() << std::endl;
        g_server_recv_result.success = false;
        g_server_recv_result.end_time = std::chrono::high_resolution_clock::now();
        g_server_recv_result.actual_transfer_end = g_server_recv_result.end_time;
        g_server_recv_completed = true;
    }
}

// Global shared buffer for client send
static std::shared_ptr<ucxx::Buffer> g_client_send_buffer;

// Client send coroutine with improved timing
btsp::FCTask<bool> client_send_coroutine(size_t buffer_size, uint64_t tag) {
    try {
        // Connect to server
        co_await g_client_manager->connect(g_config.server_addr, g_config.rdma_port);

        // Wait for server to be ready
        while (!g_server_ready.load()) {
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        }

        // Record operation start time and actual transfer start time
        g_client_send_result.start_time = std::chrono::high_resolution_clock::now();
        g_client_send_result.actual_transfer_start = std::chrono::high_resolution_clock::now();

        // Perform actual tag send
        btsp::FCOpResult result = co_await g_client_manager->tag_send(
            g_client_send_buffer->data(),
            buffer_size,
            tag
        );

        // Record actual transfer end time (when send operation completes)
        g_client_send_result.actual_transfer_end = std::chrono::high_resolution_clock::now();
        g_client_send_result.end_time = g_client_send_result.actual_transfer_end;
        g_client_send_result.success = result.success();

        if (result.success()) {
            std::cout << "Client send completed: " << result.bytes_transferred << " bytes" << std::endl;
        } else {
            std::cerr << "Client send failed" << std::endl;
        }

        co_return result.success();
    } catch (const std::exception& e) {
        std::cerr << "Client send coroutine error: " << e.what() << std::endl;
        g_client_send_result.end_time = std::chrono::high_resolution_clock::now();
        g_client_send_result.actual_transfer_end = g_client_send_result.end_time;
        g_client_send_result.success = false;
        co_return false;
    }
}

// Client send operation that runs in background
void run_client_send(size_t buffer_size, uint64_t tag) {
    g_client_send_completed = false;
    g_client_send_result = OperationResult{};

    try {
        // Allocate RMM buffer on client device
        g_client_send_buffer = ucxx::allocateBuffer(ucxx::BufferType::RMM, buffer_size);

        // Fill buffer with test data
#ifdef CUDA_AVAILABLE
        std::vector<char> host_data(buffer_size, 0x42);
        cudaError_t err = cudaMemcpy(g_client_send_buffer->data(), host_data.data(),
                                   buffer_size, cudaMemcpyHostToDevice);
        if (err != cudaSuccess) {
            std::cerr << "Failed to copy data to GPU: " << cudaGetErrorString(err) << std::endl;
            g_client_send_result.success = false;
            g_client_send_result.end_time = std::chrono::high_resolution_clock::now();
            g_client_send_result.actual_transfer_end = g_client_send_result.end_time;
            g_client_send_completed = true;
            return;
        }
#endif

        // Start the coroutine task
        auto task = client_send_coroutine(buffer_size, tag);

        // Wait for completion with timeout
        auto start = std::chrono::steady_clock::now();
        while ((std::chrono::steady_clock::now() - start) < std::chrono::seconds(10)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));

            // Check if operation completed by checking if end_time is set
            if (g_client_send_result.end_time != std::chrono::high_resolution_clock::time_point{}) {
                g_client_send_completed = true;
                break;
            }
        }

        if (!g_client_send_completed) {
            std::cerr << "Client send timeout" << std::endl;
            g_client_send_result.success = false;
            g_client_send_result.end_time = std::chrono::high_resolution_clock::now();
            g_client_send_result.actual_transfer_end = g_client_send_result.end_time;
            g_client_send_completed = true;
        }

    } catch (const std::exception& e) {
        std::cerr << "Client send error: " << e.what() << std::endl;
        g_client_send_result.success = false;
        g_client_send_result.end_time = std::chrono::high_resolution_clock::now();
        g_client_send_result.actual_transfer_end = g_client_send_result.end_time;
        g_client_send_completed = true;
    }
}

// Wait for operation completion and return results
std::pair<OperationResult, OperationResult> wait_for_operations(std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) {
    auto start = std::chrono::steady_clock::now();
    while ((std::chrono::steady_clock::now() - start) < timeout) {
        if (g_server_recv_completed.load() && g_client_send_completed.load()) {
            return {g_server_recv_result, g_client_send_result};
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return {g_server_recv_result, g_client_send_result};
}

// Export benchmark results to CSV
void export_results_to_csv(const std::string& filename) {
    std::ofstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Failed to open file for writing: " << filename << std::endl;
        return;
    }

    // Write CSV header
    file << "timestamp,buffer_size_bytes,transfer_time_us,bandwidth_gbps,throughput_mbps,server_device,client_device,success\n";

    // Write data
    for (const auto& result : g_benchmark_results) {
        file << result.timestamp << ","
             << result.buffer_size << ","
             << std::fixed << std::setprecision(3) << result.transfer_time_us << ","
             << std::fixed << std::setprecision(6) << result.bandwidth_gbps << ","
             << std::fixed << std::setprecision(3) << result.throughput_mbps << ","
             << result.server_device << ","
             << result.client_device << ","
             << (result.success ? "true" : "false") << "\n";
    }

    file.close();
    std::cout << "Benchmark results exported to: " << filename << std::endl;
}

// Get current timestamp as string
std::string get_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;

    std::stringstream ss;
    ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d_%H:%M:%S");
    ss << "." << std::setfill('0') << std::setw(3) << ms.count();
    return ss.str();
}

// Benchmark CUDA IPC tag send/recv with accurate timing
void BM_CudaIpcTagSendRecv(benchmark::State& state) {
    if (!InitializeCudaIpcBenchmark()) {
        state.SkipWithError("Failed to initialize CUDA IPC benchmark environment");
        return;
    }

    const size_t buffer_size = state.range(0);
    uint64_t tag_counter = 1000;
    double total_transfer_time_us = 0.0;

    for (auto _ : state) {
        uint64_t tag = tag_counter++;

        // Reset synchronization flags and results for each iteration
        g_server_ready = false;
        g_transfer_started = false;
        g_server_recv_completed = false;
        g_client_send_completed = false;
        g_server_recv_result = OperationResult{};
        g_client_send_result = OperationResult{};

        // Start server receive in background thread
        std::thread server_thread([&]() {
            run_server_recv(buffer_size, tag);
        });

        // Small delay to ensure server thread starts first
        std::this_thread::sleep_for(std::chrono::milliseconds(5));

        // Start client send in background thread
        std::thread client_thread([&]() {
            run_client_send(buffer_size, tag);
        });

        // Wait for both operations to complete and get results
        auto [recv_result, send_result] = wait_for_operations(std::chrono::milliseconds(15000));

        // Wait for threads to finish
        server_thread.join();
        client_thread.join();

        if (!recv_result.success || !send_result.success) {
            std::cerr << "Iteration failed - recv_success: " << recv_result.success
                      << ", send_success: " << send_result.success << std::endl;
            state.SkipWithError("Send or receive operation failed");
            return;
        }

        // Calculate the actual data transfer time
        // Use the time from when server finishes receiving (most accurate for IPC)
        auto actual_transfer_time_us = recv_result.actual_transfer_duration_us();

        total_transfer_time_us += actual_transfer_time_us;
        state.SetIterationTime(actual_transfer_time_us / 1000000.0);

        // Store detailed timing information
        state.counters["ActualTransferTime_us"] = actual_transfer_time_us;
        state.counters["RecvOperationTime_us"] = recv_result.duration_us();
        state.counters["SendOperationTime_us"] = send_result.duration_us();

        // Store result for export
        BenchmarkResult result;
        result.buffer_size = buffer_size;
        result.transfer_time_us = actual_transfer_time_us;
        result.bandwidth_gbps = (static_cast<double>(buffer_size) / (actual_transfer_time_us / 1000000.0)) / (1024.0 * 1024.0 * 1024.0);
        result.throughput_mbps = (static_cast<double>(buffer_size) / (actual_transfer_time_us / 1000000.0)) / (1024.0 * 1024.0);
        result.server_device = g_config.server_cuda_device;
        result.client_device = g_config.client_cuda_device;
        result.success = true;
        result.timestamp = get_timestamp();
        g_benchmark_results.push_back(result);
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * buffer_size);

    // Calculate average metrics based on actual transfer times
    double avg_transfer_time_us = total_transfer_time_us / static_cast<double>(state.iterations());
    double bandwidth_gbps = (static_cast<double>(buffer_size) / (avg_transfer_time_us / 1000000.0)) / (1024.0 * 1024.0 * 1024.0);

    // Add custom counters for better analysis
    state.counters["BufferSize_B"] = buffer_size;
    state.counters["Bandwidth_GBps"] = bandwidth_gbps;
    state.counters["AvgActualTransferTime_us"] = avg_transfer_time_us;
    state.counters["Throughput_MBps"] = (static_cast<double>(buffer_size) / (avg_transfer_time_us / 1000000.0)) / (1024.0 * 1024.0);

    // Format label with device info
    std::string size_str;
    if (buffer_size >= 1024*1024*1024) {
        size_str = fmt::format("{:.1f}GB", static_cast<double>(buffer_size)/(1024*1024*1024));
    } else if (buffer_size >= 1024*1024) {
        size_str = fmt::format("{}MB", buffer_size/(1024*1024));
    } else if (buffer_size >= 1024) {
        size_str = fmt::format("{}KB", buffer_size/1024);
    } else {
        size_str = fmt::format("{}B", buffer_size);
    }

    std::string label = fmt::format("CUDA_IPC_Dev{}->Dev{}_{}",
                                   g_config.server_cuda_device,
                                   g_config.client_cuda_device,
                                   size_str);
    state.SetLabel(label);
}

// Buffer sizes to test
static const std::vector<int64_t> ipc_buffer_sizes = {
    1024,                    // 1KB
    64 * 1024,              // 64KB  
    1024 * 1024,            // 1MB
    10 * 1024 * 1024,       // 10MB
    100 * 1024 * 1024       // 100MB
};

// Register benchmarks
BENCHMARK(BM_CudaIpcTagSendRecv)
    ->ArgsProduct({ipc_buffer_sizes})
    ->Unit(benchmark::kMicrosecond)
    ->UseManualTime()
    ->Iterations(5);

int main(int argc, char** argv) {
    // Initialize log wrapper
    btsp::LogWrapper::init(argv[0]);

    // Parse custom arguments for CUDA device configuration before benchmark::Initialize
    std::vector<char*> filtered_args;
    filtered_args.push_back(argv[0]); // Keep program name

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.find("--server-device=") == 0) {
            g_config.server_cuda_device = std::stoi(arg.substr(16));
        } else if (arg.find("--client-device=") == 0) {
            g_config.client_cuda_device = std::stoi(arg.substr(16));
        } else if (arg.find("--rdma-port=") == 0) {
            g_config.rdma_port = static_cast<uint16_t>(std::stoi(arg.substr(12)));
        } else {
            // Keep other arguments for benchmark library
            filtered_args.push_back(argv[i]);
        }
    }

    std::cout << "CUDA IPC Tag Benchmark Configuration:" << std::endl;
    std::cout << "  Server CUDA device: " << g_config.server_cuda_device << std::endl;
    std::cout << "  Client CUDA device: " << g_config.client_cuda_device << std::endl;
    std::cout << "  RDMA port: " << g_config.rdma_port << std::endl;

    // Initialize and run benchmarks with filtered arguments
    int filtered_argc = static_cast<int>(filtered_args.size());
    benchmark::Initialize(&filtered_argc, filtered_args.data());
    if (benchmark::ReportUnrecognizedArguments(filtered_argc, filtered_args.data())) return 1;

    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();

    // Export results to CSV
    if (!g_benchmark_results.empty()) {
        std::string filename = fmt::format("cuda_ipc_benchmark_results_{}.csv",
                                          std::chrono::duration_cast<std::chrono::seconds>(
                                              std::chrono::system_clock::now().time_since_epoch()).count());
        export_results_to_csv(filename);
    }

    // Cleanup
    CleanupCudaIpcBenchmark();

    return 0;
}
