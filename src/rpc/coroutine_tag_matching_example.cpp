#include "rpc/coroutine_rdma_manager.hpp"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>

using namespace btsp;

// Simple coroutine for receiving data with specific tag
RdmaTask<void> receive_with_tag(CoroutineRdmaManager& manager, uint64_t tag, void* buffer, size_t size, const std::string& description) {
    std::cout << "Starting receive for " << description << " (tag=" << tag << ")..." << std::endl;
    
    RdmaOpResult result = co_await manager.tag_recv(buffer, size, tag);
    
    if (result.success()) {
        std::cout << "✅ Successfully received " << description << " (tag=" << tag << ")" << std::endl;
    } else {
        std::cout << "❌ Failed to receive " << description << " (tag=" << tag << ")" << std::endl;
    }
}

// Simple coroutine for sending data with specific tag
RdmaTask<void> send_with_tag(CoroutineRdmaManager& manager, uint64_t tag, void* data, size_t size, const std::string& description) {
    std::cout << "Starting send for " << description << " (tag=" << tag << ")..." << std::endl;
    
    RdmaOpResult result = co_await manager.tag_send(data, size, tag);
    
    if (result.success()) {
        std::cout << "✅ Successfully sent " << description << " (tag=" << tag << ")" << std::endl;
    } else {
        std::cout << "❌ Failed to send " << description << " (tag=" << tag << ")" << std::endl;
    }
}

// Coroutine receiver that handles multiple tags
RdmaTask<void> multi_tag_receiver() {
    std::cout << "=== Coroutine Multi-Tag Receiver ===" << std::endl;
    
    // Initialize RDMA manager
    CoroutineRdmaManager manager;
    if (!manager.initialize(true, 12345)) {
        std::cerr << "Failed to initialize RDMA manager" << std::endl;
        co_return;
    }
    
    std::cout << "Receiver: Waiting for connection..." << std::endl;
    co_await manager.wait_for_connection();
    std::cout << "Receiver: Connected!" << std::endl;
    
    // Prepare buffers for different data types
    std::vector<int> int_buffer(5, 0);
    std::vector<float> float_buffer(3, 0.0f);
    char string_buffer[128] = {0};
    std::vector<uint8_t> byte_buffer(10, 0);
    
    // Start sequential receives with different tags
    std::cout << "\nStarting sequential receives for different tags..." << std::endl;

    // Receive integer array
    std::cout << "Waiting for integer array (tag=100)..." << std::endl;
    RdmaOpResult int_result = co_await manager.tag_recv(int_buffer.data(), int_buffer.size() * sizeof(int), 100);
    if (int_result.success()) {
        std::cout << "✅ Successfully received integer array (tag=100)" << std::endl;
    } else {
        std::cout << "❌ Failed to receive integer array (tag=100)" << std::endl;
    }

    // Receive float array
    std::cout << "Waiting for float array (tag=200)..." << std::endl;
    RdmaOpResult float_result = co_await manager.tag_recv(float_buffer.data(), float_buffer.size() * sizeof(float), 200);
    if (float_result.success()) {
        std::cout << "✅ Successfully received float array (tag=200)" << std::endl;
    } else {
        std::cout << "❌ Failed to receive float array (tag=200)" << std::endl;
    }

    // Receive string message
    std::cout << "Waiting for string message (tag=300)..." << std::endl;
    RdmaOpResult string_result = co_await manager.tag_recv(string_buffer, sizeof(string_buffer), 300);
    if (string_result.success()) {
        std::cout << "✅ Successfully received string message (tag=300)" << std::endl;
    } else {
        std::cout << "❌ Failed to receive string message (tag=300)" << std::endl;
    }

    // Receive byte array
    std::cout << "Waiting for byte array (tag=400)..." << std::endl;
    RdmaOpResult byte_result = co_await manager.tag_recv(byte_buffer.data(), byte_buffer.size(), 400);
    if (byte_result.success()) {
        std::cout << "✅ Successfully received byte array (tag=400)" << std::endl;
    } else {
        std::cout << "❌ Failed to receive byte array (tag=400)" << std::endl;
    }
    
    // Display received data
    std::cout << "\n=== Received Data ===" << std::endl;
    
    std::cout << "Integer array (tag 100): ";
    for (const auto& val : int_buffer) {
        std::cout << val << " ";
    }
    std::cout << std::endl;
    
    std::cout << "Float array (tag 200): ";
    for (const auto& val : float_buffer) {
        std::cout << val << " ";
    }
    std::cout << std::endl;
    
    std::cout << "String (tag 300): \"" << string_buffer << "\"" << std::endl;
    
    std::cout << "Byte array (tag 400): ";
    for (const auto& val : byte_buffer) {
        std::cout << static_cast<int>(val) << " ";
    }
    std::cout << std::endl;
    
    manager.shutdown();
}

// Coroutine sender that sends data with multiple tags
RdmaTask<void> multi_tag_sender(const std::string& server_addr) {
    std::cout << "=== Coroutine Multi-Tag Sender ===" << std::endl;
    
    // Initialize RDMA manager
    CoroutineRdmaManager manager;
    if (!manager.initialize(false, 0)) {
        std::cerr << "Failed to initialize RDMA manager" << std::endl;
        co_return;
    }
    
    // Connect to receiver
    std::cout << "Sender: Connecting to " << server_addr << ":12345..." << std::endl;
    RdmaOpResult connect_result = co_await manager.connect(server_addr, 12345);
    
    if (!connect_result.success()) {
        std::cerr << "Sender: Failed to connect" << std::endl;
        co_return;
    }
    
    std::cout << "Sender: Connected!" << std::endl;
    
    // Wait a bit for receiver to set up
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    // Prepare data to send
    std::vector<int> int_data = {10, 20, 30, 40, 50};
    std::vector<float> float_data = {1.1f, 2.2f, 3.3f};
    const char* string_data = "Hello from coroutine sender!";
    std::vector<uint8_t> byte_data = {0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22, 0x33, 0x44};
    
    std::cout << "\nSending data with different tags..." << std::endl;

    // Send integer array
    std::cout << "Sending integer array (tag=100)..." << std::endl;
    RdmaOpResult int_result = co_await manager.tag_send(int_data.data(), int_data.size() * sizeof(int), 100);
    if (int_result.success()) {
        std::cout << "✅ Successfully sent integer array (tag=100)" << std::endl;
    } else {
        std::cout << "❌ Failed to send integer array (tag=100)" << std::endl;
    }

    // Send float array
    std::cout << "Sending float array (tag=200)..." << std::endl;
    RdmaOpResult float_result = co_await manager.tag_send(float_data.data(), float_data.size() * sizeof(float), 200);
    if (float_result.success()) {
        std::cout << "✅ Successfully sent float array (tag=200)" << std::endl;
    } else {
        std::cout << "❌ Failed to send float array (tag=200)" << std::endl;
    }

    // Send string message
    std::cout << "Sending string message (tag=300)..." << std::endl;
    RdmaOpResult string_result = co_await manager.tag_send(const_cast<char*>(string_data), strlen(string_data) + 1, 300);
    if (string_result.success()) {
        std::cout << "✅ Successfully sent string message (tag=300)" << std::endl;
    } else {
        std::cout << "❌ Failed to send string message (tag=300)" << std::endl;
    }

    // Send byte array
    std::cout << "Sending byte array (tag=400)..." << std::endl;
    RdmaOpResult byte_result = co_await manager.tag_send(byte_data.data(), byte_data.size(), 400);
    if (byte_result.success()) {
        std::cout << "✅ Successfully sent byte array (tag=400)" << std::endl;
    } else {
        std::cout << "❌ Failed to send byte array (tag=400)" << std::endl;
    }
    
    std::cout << "\nAll sends completed!" << std::endl;
    
    manager.shutdown();
}

// Simple coroutine runner with proper waiting
void run_receiver() {
    std::cout << "Starting receiver coroutine..." << std::endl;
    auto task = multi_tag_receiver();

    // Wait for the coroutine to complete
    while (!task.done()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    try {
        task.get();  // Check for exceptions
        std::cout << "Receiver coroutine completed successfully" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Receiver coroutine failed: " << e.what() << std::endl;
    }
}

void run_sender(const std::string& server_addr) {
    std::cout << "Starting sender coroutine..." << std::endl;
    auto task = multi_tag_sender(server_addr);

    // Wait for the coroutine to complete
    while (!task.done()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    try {
        task.get();  // Check for exceptions
        std::cout << "Sender coroutine completed successfully" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Sender coroutine failed: " << e.what() << std::endl;
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "Coroutine Tag Matching Example Usage:" << std::endl;
        std::cout << "  Receiver: " << argv[0] << " receiver" << std::endl;
        std::cout << "  Sender:   " << argv[0] << " sender [server_addr]" << std::endl;
        std::cout << std::endl;
        std::cout << "This example demonstrates C++20 coroutines with RDMA tag matching:" << std::endl;
        std::cout << "- Multiple concurrent receives with different tags" << std::endl;
        std::cout << "- Clean async/await syntax for RDMA operations" << std::endl;
        std::cout << "- Automatic tag matching between sender and receiver" << std::endl;
        std::cout << std::endl;
        std::cout << "Features demonstrated:" << std::endl;
        std::cout << "- Tag 100: Integer array transfer" << std::endl;
        std::cout << "- Tag 200: Float array transfer" << std::endl;
        std::cout << "- Tag 300: String message transfer" << std::endl;
        std::cout << "- Tag 400: Byte array transfer" << std::endl;
        return 1;
    }
    
    std::string mode = argv[1];
    
    if (mode == "receiver") {
        run_receiver();
    } else if (mode == "sender") {
        std::string server_addr = "127.0.0.1";
        if (argc > 2) {
            server_addr = argv[2];
        }
        run_sender(server_addr);
    } else {
        std::cerr << "Invalid mode. Use 'receiver' or 'sender'" << std::endl;
        return 1;
    }
    
    return 0;
}
