#include "rpc/coroutine_rdma_manager.hpp"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>

using namespace btsp;

// Simple test coroutine for receiver
RdmaTask<void> simple_receiver() {
    std::cout << "=== Simple Coroutine Receiver ===" << std::endl;
    
    CoroutineRdmaManager manager;
    if (!manager.initialize(true, 12345)) {
        std::cerr << "Failed to initialize RDMA manager" << std::endl;
        co_return;
    }
    
    std::cout << "Receiver: Waiting for connection..." << std::endl;
    co_await manager.wait_for_connection();
    std::cout << "Receiver: Connected!" << std::endl;
    
    // Simple receive test
    std::vector<int> buffer(4, 0);
    std::cout << "Receiver: Waiting for data (tag=42)..." << std::endl;
    
    RdmaOpResult result = co_await manager.tag_recv(
        buffer.data(),
        buffer.size() * sizeof(int),
        42
    );
    
    if (result.success()) {
        std::cout << "Receiver: Successfully received data!" << std::endl;
        std::cout << "Receiver: Data: ";
        for (const auto& val : buffer) {
            std::cout << val << " ";
        }
        std::cout << std::endl;
    } else {
        std::cout << "Receiver: Failed to receive data" << std::endl;
    }
    
    std::cout << "Receiver: Shutting down..." << std::endl;
    manager.shutdown();
}

// Simple test coroutine for sender
RdmaTask<void> simple_sender() {
    std::cout << "=== Simple Coroutine Sender ===" << std::endl;
    
    CoroutineRdmaManager manager;
    if (!manager.initialize(false, 0)) {
        std::cerr << "Failed to initialize RDMA manager" << std::endl;
        co_return;
    }
    
    std::cout << "Sender: Connecting to 127.0.0.1:12345..." << std::endl;
    RdmaOpResult connect_result = co_await manager.connect("127.0.0.1", 12345);
    
    if (!connect_result.success()) {
        std::cerr << "Sender: Failed to connect" << std::endl;
        co_return;
    }
    
    std::cout << "Sender: Connected!" << std::endl;
    
    // Wait a bit for receiver to set up
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    // Simple send test
    std::vector<int> data = {10, 20, 30, 40};
    std::cout << "Sender: Sending data (tag=42)..." << std::endl;
    
    RdmaOpResult send_result = co_await manager.tag_send(
        data.data(),
        data.size() * sizeof(int),
        42
    );
    
    if (send_result.success()) {
        std::cout << "Sender: Successfully sent data!" << std::endl;
        std::cout << "Sender: Sent data: ";
        for (const auto& val : data) {
            std::cout << val << " ";
        }
        std::cout << std::endl;
    } else {
        std::cout << "Sender: Failed to send data" << std::endl;
    }
    
    std::cout << "Sender: Shutting down..." << std::endl;
    manager.shutdown();
}

// Coroutine runner with proper waiting
void run_simple_receiver() {
    std::cout << "Starting simple receiver coroutine..." << std::endl;
    auto task = simple_receiver();
    
    // Wait for the coroutine to complete
    while (!task.done()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    try {
        task.get();  // Check for exceptions
        std::cout << "Simple receiver coroutine completed successfully" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Simple receiver coroutine failed: " << e.what() << std::endl;
    }
}

void run_simple_sender() {
    std::cout << "Starting simple sender coroutine..." << std::endl;
    auto task = simple_sender();
    
    // Wait for the coroutine to complete
    while (!task.done()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    try {
        task.get();  // Check for exceptions
        std::cout << "Simple sender coroutine completed successfully" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Simple sender coroutine failed: " << e.what() << std::endl;
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "Simple Coroutine Test Usage:" << std::endl;
        std::cout << "  Receiver: " << argv[0] << " receiver" << std::endl;
        std::cout << "  Sender:   " << argv[0] << " sender" << std::endl;
        std::cout << std::endl;
        std::cout << "This is a simple test to verify coroutine functionality:" << std::endl;
        std::cout << "- Basic connection establishment" << std::endl;
        std::cout << "- Single tag send/receive operation" << std::endl;
        std::cout << "- Proper coroutine completion" << std::endl;
        return 1;
    }
    
    std::string mode = argv[1];
    
    if (mode == "receiver") {
        run_simple_receiver();
    } else if (mode == "sender") {
        run_simple_sender();
    } else {
        std::cerr << "Invalid mode. Use 'receiver' or 'sender'" << std::endl;
        return 1;
    }
    
    return 0;
}
