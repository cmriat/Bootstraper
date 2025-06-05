#include "rpc/coroutine_rdma_manager.hpp"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <cstring>

using namespace btsp;

// Coroutine-based server example
RdmaTask<void> coroutine_server(uint16_t port) {
    std::cout << "=== Coroutine RDMA Server ===" << std::endl;
    
    // Initialize global RDMA manager in server mode
    if (!initialize_global_coroutine_rdma_manager(true, port)) {
        std::cerr << "Failed to initialize coroutine RDMA manager" << std::endl;
        co_return;
    }
    
    auto& rdma_manager = get_global_coroutine_rdma_manager();
    
    std::cout << "Server: Waiting for client connection..." << std::endl;
    
    // Wait for connection using coroutine
    RdmaState state = co_await rdma_manager.wait_for_connection();
    if (state != RdmaState::CONNECTED) {
        std::cerr << "Server: Failed to establish connection" << std::endl;
        co_return;
    }
    
    std::cout << "Server: Client connected! Starting coroutine operations..." << std::endl;
    
    // Example 1: Receive data with tag 100
    std::cout << "\n--- Example 1: Receive with tag 100 ---" << std::endl;
    std::vector<int> buffer1(4, 0);
    
    RdmaOpResult result1 = co_await rdma_manager.tag_recv(
        buffer1.data(),
        buffer1.size() * sizeof(int),
        100
    );
    
    if (result1.success()) {
        std::cout << "Server: Successfully received data with tag 100!" << std::endl;
        std::cout << "Server: Data: ";
        for (const auto& val : buffer1) {
            std::cout << val << " ";
        }
        std::cout << std::endl;
    } else {
        std::cout << "Server: Failed to receive data with tag 100" << std::endl;
    }
    
    // Example 2: Receive float data with tag 200
    std::cout << "\n--- Example 2: Receive with tag 200 ---" << std::endl;
    std::vector<float> buffer2(3, 0.0f);
    
    RdmaOpResult result2 = co_await rdma_manager.tag_recv(
        buffer2.data(),
        buffer2.size() * sizeof(float),
        200
    );
    
    if (result2.success()) {
        std::cout << "Server: Successfully received data with tag 200!" << std::endl;
        std::cout << "Server: Data: ";
        for (const auto& val : buffer2) {
            std::cout << val << " ";
        }
        std::cout << std::endl;
    } else {
        std::cout << "Server: Failed to receive data with tag 200" << std::endl;
    }
    
    // Example 3: Receive string with tag 300
    std::cout << "\n--- Example 3: Receive string with tag 300 ---" << std::endl;
    char string_buffer[256] = {0};
    
    RdmaOpResult result3 = co_await rdma_manager.tag_recv(
        string_buffer,
        sizeof(string_buffer),
        300
    );
    
    if (result3.success()) {
        std::cout << "Server: Successfully received string with tag 300!" << std::endl;
        std::cout << "Server: String: \"" << string_buffer << "\"" << std::endl;
    } else {
        std::cout << "Server: Failed to receive string with tag 300" << std::endl;
    }
    
    // Example 4: Send response back to client
    std::cout << "\n--- Example 4: Send response with tag 400 ---" << std::endl;
    std::vector<int> response = {100, 200, 300};
    
    RdmaOpResult result4 = co_await rdma_manager.tag_send(
        response.data(),
        response.size() * sizeof(int),
        400
    );
    
    if (result4.success()) {
        std::cout << "Server: Successfully sent response with tag 400!" << std::endl;
    } else {
        std::cout << "Server: Failed to send response with tag 400" << std::endl;
    }
    
    std::cout << "Server: All operations completed. Shutting down..." << std::endl;
    shutdown_global_coroutine_rdma_manager();
}

// Coroutine-based client example
RdmaTask<void> coroutine_client(const std::string& server_addr, uint16_t port) {
    std::cout << "=== Coroutine RDMA Client ===" << std::endl;
    
    // Initialize global RDMA manager in client mode
    if (!initialize_global_coroutine_rdma_manager(false, 0)) {
        std::cerr << "Failed to initialize coroutine RDMA manager" << std::endl;
        co_return;
    }
    
    auto& rdma_manager = get_global_coroutine_rdma_manager();
    
    // Connect to server using coroutine
    std::cout << "Client: Connecting to server " << server_addr << ":" << port << "..." << std::endl;
    
    RdmaOpResult connect_result = co_await rdma_manager.connect(server_addr, port);
    if (!connect_result.success()) {
        std::cerr << "Client: Failed to connect to server" << std::endl;
        co_return;
    }
    
    std::cout << "Client: Connected to server! Starting coroutine operations..." << std::endl;
    
    // Wait a bit for server to set up receives
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    // Example 1: Send int data with tag 100
    std::cout << "\n--- Example 1: Send with tag 100 ---" << std::endl;
    std::vector<int> data1 = {10, 20, 30, 40};
    
    RdmaOpResult result1 = co_await rdma_manager.tag_send(
        data1.data(),
        data1.size() * sizeof(int),
        100
    );
    
    if (result1.success()) {
        std::cout << "Client: Successfully sent data with tag 100!" << std::endl;
        std::cout << "Client: Sent data: ";
        for (const auto& val : data1) {
            std::cout << val << " ";
        }
        std::cout << std::endl;
    } else {
        std::cout << "Client: Failed to send data with tag 100" << std::endl;
    }
    
    // Example 2: Send float data with tag 200
    std::cout << "\n--- Example 2: Send with tag 200 ---" << std::endl;
    std::vector<float> data2 = {1.1f, 2.2f, 3.3f};
    
    RdmaOpResult result2 = co_await rdma_manager.tag_send(
        data2.data(),
        data2.size() * sizeof(float),
        200
    );
    
    if (result2.success()) {
        std::cout << "Client: Successfully sent data with tag 200!" << std::endl;
        std::cout << "Client: Sent data: ";
        for (const auto& val : data2) {
            std::cout << val << " ";
        }
        std::cout << std::endl;
    } else {
        std::cout << "Client: Failed to send data with tag 200" << std::endl;
    }
    
    // Example 3: Send string with tag 300
    std::cout << "\n--- Example 3: Send string with tag 300 ---" << std::endl;
    const char* message = "Hello from coroutine client!";
    
    RdmaOpResult result3 = co_await rdma_manager.tag_send(
        const_cast<char*>(message),
        strlen(message) + 1,
        300
    );
    
    if (result3.success()) {
        std::cout << "Client: Successfully sent string with tag 300!" << std::endl;
        std::cout << "Client: Sent string: \"" << message << "\"" << std::endl;
    } else {
        std::cout << "Client: Failed to send string with tag 300" << std::endl;
    }
    
    // Example 4: Receive response from server
    std::cout << "\n--- Example 4: Receive response with tag 400 ---" << std::endl;
    std::vector<int> response(3, 0);
    
    RdmaOpResult result4 = co_await rdma_manager.tag_recv(
        response.data(),
        response.size() * sizeof(int),
        400
    );
    
    if (result4.success()) {
        std::cout << "Client: Successfully received response with tag 400!" << std::endl;
        std::cout << "Client: Response: ";
        for (const auto& val : response) {
            std::cout << val << " ";
        }
        std::cout << std::endl;
    } else {
        std::cout << "Client: Failed to receive response with tag 400" << std::endl;
    }
    
    std::cout << "Client: All operations completed. Shutting down..." << std::endl;
    shutdown_global_coroutine_rdma_manager();
}

// Simple wrapper to run coroutines
void run_server_coroutine(uint16_t port) {
    auto task = coroutine_server(port);
    // In a real application, you'd have a proper coroutine scheduler
    // For this example, we'll just sleep to let the coroutine run
    std::this_thread::sleep_for(std::chrono::seconds(30));
}

void run_client_coroutine(const std::string& server_addr, uint16_t port) {
    auto task = coroutine_client(server_addr, port);
    // In a real application, you'd have a proper coroutine scheduler
    // For this example, we'll just sleep to let the coroutine run
    std::this_thread::sleep_for(std::chrono::seconds(15));
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "Coroutine RDMA Example Usage:" << std::endl;
        std::cout << "  Server: " << argv[0] << " server [port]" << std::endl;
        std::cout << "  Client: " << argv[0] << " client [server_addr] [port]" << std::endl;
        std::cout << std::endl;
        std::cout << "This example demonstrates C++20 coroutines with RDMA operations:" << std::endl;
        std::cout << "- Uses co_await for async RDMA operations" << std::endl;
        std::cout << "- Makes async code look like synchronous code" << std::endl;
        std::cout << "- Maintains high performance with non-blocking operations" << std::endl;
        return 1;
    }
    
    std::string mode = argv[1];
    
    if (mode == "server") {
        uint16_t port = 12345;
        if (argc > 2) {
            port = static_cast<uint16_t>(std::stoi(argv[2]));
        }
        run_server_coroutine(port);
    } else if (mode == "client") {
        std::string server_addr = "127.0.0.1";
        uint16_t port = 12345;
        
        if (argc > 2) {
            server_addr = argv[2];
        }
        if (argc > 3) {
            port = static_cast<uint16_t>(std::stoi(argv[3]));
        }
        
        run_client_coroutine(server_addr, port);
    } else {
        std::cerr << "Invalid mode. Use 'server' or 'client'" << std::endl;
        return 1;
    }
    
    return 0;
}
