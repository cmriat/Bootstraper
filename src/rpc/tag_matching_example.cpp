#include "rpc/independent_rdma_manager.hpp"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <cstring>

using namespace btsp;

// Simple tag matching receiver example
void tag_matching_receiver_example() {
    std::cout << "=== Tag Matching Receiver Example ===" << std::endl;
    
    // Initialize global RDMA manager in server mode
    if (!initialize_global_rdma_manager(true, 12345)) {
        std::cerr << "Failed to initialize global RDMA manager" << std::endl;
        return;
    }
    
    auto& rdma_manager = get_global_rdma_manager();
    
    std::cout << "Receiver: Waiting for client connection..." << std::endl;
    
    // Wait for connection
    while (!rdma_manager.is_connected()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    std::cout << "Receiver: Client connected! Starting tag matching operations..." << std::endl;
    
    // Example 1: Receive data with specific tag using callback
    std::cout << "\n--- Example 1: Async receive with tag 100 ---" << std::endl;
    std::vector<int> buffer1(4, 0);
    
    bool submitted1 = rdma_manager.submit_tag_recv(
        buffer1.data(),
        buffer1.size() * sizeof(int),
        100,  // tag
        [&buffer1](RdmaResult result, size_t bytes_received) {
            if (result == RdmaResult::SUCCESS) {
                std::cout << "Receiver: Successfully received data with tag 100!" << std::endl;
                std::cout << "Receiver: Data: ";
                for (const auto& val : buffer1) {
                    std::cout << val << " ";
                }
                std::cout << std::endl;
            } else {
                std::cout << "Receiver: Failed to receive data with tag 100" << std::endl;
            }
        }
    );
    
    if (!submitted1) {
        std::cerr << "Receiver: Failed to submit receive for tag 100" << std::endl;
    }
    
    // // Example 2: Receive data with different tag synchronously
    // std::cout << "\n--- Example 2: Sync receive with tag 200 ---" << std::endl;
    // std::vector<float> buffer2(3, 0.0f);
    
    // size_t bytes_received = 0;
    // RdmaResult result2 = rdma_manager.tag_recv_sync(
    //     buffer2.data(),
    //     buffer2.size() * sizeof(float),
    //     200,  // tag
    //     &bytes_received,
    //     10000  // 10 second timeout
    // );
    
    // if (result2 == RdmaResult::SUCCESS) {
    //     std::cout << "Receiver: Successfully received data with tag 200!" << std::endl;
    //     std::cout << "Receiver: Data: ";
    //     for (const auto& val : buffer2) {
    //         std::cout << val << " ";
    //     }
    //     std::cout << std::endl;
    // } else {
    //     std::cout << "Receiver: Failed to receive data with tag 200, result=" << static_cast<int>(result2) << std::endl;
    // }
    
    // // Example 3: Receive string data with tag 300
    // std::cout << "\n--- Example 3: Async receive string with tag 300 ---" << std::endl;
    // char string_buffer[256] = {0};
    
    // bool submitted3 = rdma_manager.submit_tag_recv(
    //     string_buffer,
    //     sizeof(string_buffer),
    //     300,  // tag
    //     [&string_buffer](RdmaResult result, size_t bytes_received) {
    //         if (result == RdmaResult::SUCCESS) {
    //             std::cout << "Receiver: Successfully received string with tag 300!" << std::endl;
    //             std::cout << "Receiver: String: \"" << string_buffer << "\"" << std::endl;
    //         } else {
    //             std::cout << "Receiver: Failed to receive string with tag 300" << std::endl;
    //         }
    //     }
    // );
    
    // if (!submitted3) {
    //     std::cerr << "Receiver: Failed to submit receive for tag 300" << std::endl;
    // }
    
    // // Example 4: Multiple receives with different tags
    // std::cout << "\n--- Example 4: Multiple receives with tags 401, 402, 403 ---" << std::endl;
    
    // std::vector<std::vector<uint8_t>> buffers(3);
    // for (int i = 0; i < 3; ++i) {
    //     buffers[i].resize(10, 0);
        
    //     uint64_t tag = 401 + i;
    //     bool submitted = rdma_manager.submit_tag_recv(
    //         buffers[i].data(),
    //         buffers[i].size(),
    //         tag,
    //         [i, tag, &buffers](RdmaResult result, size_t bytes_received) {
    //             if (result == RdmaResult::SUCCESS) {
    //                 std::cout << "Receiver: Successfully received data with tag " << tag << "!" << std::endl;
    //                 std::cout << "Receiver: Data (buffer " << i << "): ";
    //                 for (const auto& val : buffers[i]) {
    //                     std::cout << static_cast<int>(val) << " ";
    //                 }
    //                 std::cout << std::endl;
    //             } else {
    //                 std::cout << "Receiver: Failed to receive data with tag " << tag << std::endl;
    //             }
    //         }
    //     );
        
    //     if (!submitted) {
    //         std::cerr << "Receiver: Failed to submit receive for tag " << tag << std::endl;
    //     }
    // }
    
    // Keep running to receive all messages
    std::cout << "\nReceiver: Waiting for all messages (30 seconds timeout)..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(30));
    
    std::cout << "Receiver: Shutting down..." << std::endl;
    shutdown_global_rdma_manager();
}

// Simple tag matching sender example
void tag_matching_sender_example(const std::string& server_addr) {
    std::cout << "=== Tag Matching Sender Example ===" << std::endl;
    
    // Initialize global RDMA manager in client mode
    if (!initialize_global_rdma_manager(false, 0)) {
        std::cerr << "Failed to initialize global RDMA manager" << std::endl;
        return;
    }
    
    auto& rdma_manager = get_global_rdma_manager();
    
    // Connect to receiver
    std::cout << "Sender: Connecting to receiver at " << server_addr << ":12345..." << std::endl;
    RdmaResult connect_result = rdma_manager.connect_sync(server_addr, 12345, 5000);
    
    if (connect_result != RdmaResult::SUCCESS) {
        std::cerr << "Sender: Failed to connect to receiver" << std::endl;
        return;
    }
    
    std::cout << "Sender: Connected! Starting to send tagged messages..." << std::endl;
    
    // Wait a bit for receiver to set up
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    // Send data with tag 100
    std::cout << "\n--- Sending data with tag 100 ---" << std::endl;
    std::vector<int> data1 = {1, 2, 3, 4};
    RdmaResult result1 = rdma_manager.tag_send_sync(data1.data(), data1.size() * sizeof(int), 100);
    std::cout << "Sender: Tag 100 send result: " << static_cast<int>(result1) << std::endl;
    
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    // // Send data with tag 200
    // std::cout << "\n--- Sending data with tag 200 ---" << std::endl;
    // std::vector<float> data2 = {1.1f, 2.2f, 3.3f};
    // RdmaResult result2 = rdma_manager.tag_send_sync(data2.data(), data2.size() * sizeof(float), 200);
    // std::cout << "Sender: Tag 200 send result: " << static_cast<int>(result2) << std::endl;
    
    // std::this_thread::sleep_for(std::chrono::seconds(1));
    
    // // Send string with tag 300
    // std::cout << "\n--- Sending string with tag 300 ---" << std::endl;
    // const char* message = "Hello from RDMA sender!";
    // RdmaResult result3 = rdma_manager.tag_send_sync(const_cast<char*>(message), strlen(message) + 1, 300);
    // std::cout << "Sender: Tag 300 send result: " << static_cast<int>(result3) << std::endl;
    
    // std::this_thread::sleep_for(std::chrono::seconds(1));
    
    // // Send multiple messages with tags 401, 402, 403
    // std::cout << "\n--- Sending multiple messages with tags 401-403 ---" << std::endl;
    // for (int i = 0; i < 3; ++i) {
    //     std::vector<uint8_t> data(10);
    //     for (int j = 0; j < 10; ++j) {
    //         data[j] = (i + 1) * 10 + j;  // Generate some test data
    //     }
        
    //     uint64_t tag = 401 + i;
    //     RdmaResult result = rdma_manager.tag_send_sync(data.data(), data.size(), tag);
    //     std::cout << "Sender: Tag " << tag << " send result: " << static_cast<int>(result) << std::endl;
        
    //     std::this_thread::sleep_for(std::chrono::milliseconds(500));
    // }
    
    std::cout << "Sender: All messages sent. Shutting down..." << std::endl;
    shutdown_global_rdma_manager();
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "Tag Matching Example Usage:" << std::endl;
        std::cout << "  Receiver: " << argv[0] << " receiver" << std::endl;
        std::cout << "  Sender:   " << argv[0] << " sender [server_addr]" << std::endl;
        std::cout << std::endl;
        std::cout << "This example demonstrates UCXX tag matching functionality:" << std::endl;
        std::cout << "- Receiver waits for messages with specific tags" << std::endl;
        std::cout << "- Sender sends messages with corresponding tags" << std::endl;
        std::cout << "- Messages are matched by tag and delivered to the correct receiver" << std::endl;
        return 1;
    }
    
    std::string mode = argv[1];
    
    if (mode == "receiver") {
        tag_matching_receiver_example();
    } else if (mode == "sender") {
        std::string server_addr = "127.0.0.1";
        if (argc > 2) {
            server_addr = argv[2];
        }
        tag_matching_sender_example(server_addr);
    } else {
        std::cerr << "Invalid mode. Use 'receiver' or 'sender'" << std::endl;
        return 1;
    }
    
    return 0;
}
