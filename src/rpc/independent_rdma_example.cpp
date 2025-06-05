#include "rpc/independent_rdma_manager.hpp"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <cstring>

using namespace btsp;

// Server example using IndependentRdmaManager
void run_server(uint16_t port) {
    std::cout << "=== RDMA Server (Independent) ===" << std::endl;
    
    // Create and initialize RDMA manager
    IndependentRdmaManager rdma_manager;
    
    // Set state change callback
    rdma_manager.set_state_change_callback([](RdmaState old_state, RdmaState new_state) {
        std::cout << "Server: State changed from " << static_cast<int>(old_state) 
                  << " to " << static_cast<int>(new_state) << std::endl;
    });
    
    // Initialize in server mode
    if (!rdma_manager.initialize(true, port)) {
        std::cerr << "Failed to initialize RDMA manager in server mode" << std::endl;
        return;
    }
    
    std::cout << "Server: RDMA manager initialized, listening on port " << port << std::endl;
    
    // Wait for connection
    std::cout << "Server: Waiting for client connection..." << std::endl;
    while (!rdma_manager.is_connected()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    std::cout << "Server: Client connected!" << std::endl;
    
    // Prepare receive buffer
    std::vector<int> recv_buffer(5, 0);
    
    // Submit tag receive request with callback
    std::cout << "Server: Submitting tag receive request (tag=42)..." << std::endl;
    bool submitted = rdma_manager.submit_tag_recv(
        recv_buffer.data(), 
        recv_buffer.size() * sizeof(int), 
        42,  // tag
        [&recv_buffer](RdmaResult result, size_t bytes_received) {
            if (result == RdmaResult::SUCCESS) {
                std::cout << "Server: Successfully received data via callback!" << std::endl;
                std::cout << "Server: Received data: ";
                for (const auto& val : recv_buffer) {
                    std::cout << val << " ";
                }
                std::cout << std::endl;
            } else {
                std::cout << "Server: Failed to receive data, result=" << static_cast<int>(result) << std::endl;
            }
        }
    );
    
    if (!submitted) {
        std::cerr << "Server: Failed to submit tag receive request" << std::endl;
        return;
    }
    
    // Also demonstrate synchronous receive
    std::cout << "Server: Waiting for second message (synchronous)..." << std::endl;
    std::vector<int> recv_buffer2(3, 0);
    size_t bytes_received = 0;
    RdmaResult result = rdma_manager.tag_recv_sync(
        recv_buffer2.data(),
        recv_buffer2.size() * sizeof(int),
        43,  // tag
        &bytes_received,
        10000  // 10 second timeout
    );
    
    if (result == RdmaResult::SUCCESS) {
        std::cout << "Server: Successfully received second message synchronously!" << std::endl;
        std::cout << "Server: Received data: ";
        for (const auto& val : recv_buffer2) {
            std::cout << val << " ";
        }
        std::cout << std::endl;
    } else {
        std::cout << "Server: Failed to receive second message, result=" << static_cast<int>(result) << std::endl;
    }
    
    // Keep server running for a while
    std::cout << "Server: Keeping server running for 10 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    std::cout << "Server: Shutting down..." << std::endl;
    rdma_manager.shutdown();
}

// Client example using IndependentRdmaManager
void run_client(const std::string& server_addr, uint16_t port) {
    std::cout << "=== RDMA Client (Independent) ===" << std::endl;
    
    // Create and initialize RDMA manager
    IndependentRdmaManager rdma_manager;
    
    // Set state change callback
    rdma_manager.set_state_change_callback([](RdmaState old_state, RdmaState new_state) {
        std::cout << "Client: State changed from " << static_cast<int>(old_state) 
                  << " to " << static_cast<int>(new_state) << std::endl;
    });
    
    // Initialize in client mode
    if (!rdma_manager.initialize(false, 0)) {
        std::cerr << "Failed to initialize RDMA manager in client mode" << std::endl;
        return;
    }
    
    std::cout << "Client: RDMA manager initialized" << std::endl;
    
    // Connect to server
    std::cout << "Client: Connecting to server " << server_addr << ":" << port << "..." << std::endl;
    RdmaResult connect_result = rdma_manager.connect_sync(server_addr, port, 5000);
    
    if (connect_result != RdmaResult::SUCCESS) {
        std::cerr << "Client: Failed to connect to server, result=" << static_cast<int>(connect_result) << std::endl;
        return;
    }
    
    std::cout << "Client: Connected to server!" << std::endl;
    
    // Wait a bit for server to set up receive
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    // Send first message using callback
    std::vector<int> send_buffer = {10, 20, 30, 40, 50};
    std::cout << "Client: Sending first message (tag=42) with callback..." << std::endl;
    
    bool submitted = rdma_manager.submit_tag_send(
        send_buffer.data(),
        send_buffer.size() * sizeof(int),
        42,  // tag
        [&send_buffer](RdmaResult result, size_t bytes_sent) {
            if (result == RdmaResult::SUCCESS) {
                std::cout << "Client: Successfully sent first message via callback!" << std::endl;
                std::cout << "Client: Sent data: ";
                for (const auto& val : send_buffer) {
                    std::cout << val << " ";
                }
                std::cout << std::endl;
            } else {
                std::cout << "Client: Failed to send first message, result=" << static_cast<int>(result) << std::endl;
            }
        }
    );
    
    if (!submitted) {
        std::cerr << "Client: Failed to submit tag send request" << std::endl;
        return;
    }
    
    // Wait for first send to complete
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    // Send second message synchronously
    std::vector<int> send_buffer2 = {100, 200, 300};
    std::cout << "Client: Sending second message (tag=43) synchronously..." << std::endl;
    
    size_t bytes_sent = 0;
    RdmaResult send_result = rdma_manager.tag_send_sync(
        send_buffer2.data(),
        send_buffer2.size() * sizeof(int),
        43,  // tag
        &bytes_sent,
        5000  // 5 second timeout
    );
    
    if (send_result == RdmaResult::SUCCESS) {
        std::cout << "Client: Successfully sent second message synchronously!" << std::endl;
        std::cout << "Client: Sent data: ";
        for (const auto& val : send_buffer2) {
            std::cout << val << " ";
        }
        std::cout << std::endl;
    } else {
        std::cout << "Client: Failed to send second message, result=" << static_cast<int>(send_result) << std::endl;
    }
    
    std::cout << "Client: Shutting down..." << std::endl;
    rdma_manager.shutdown();
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "Usage:" << std::endl;
        std::cout << "  Server mode: " << argv[0] << " server [port]" << std::endl;
        std::cout << "  Client mode: " << argv[0] << " client [server_addr] [port]" << std::endl;
        return 1;
    }
    
    std::string mode = argv[1];
    
    if (mode == "server") {
        uint16_t port = 12345;
        if (argc > 2) {
            port = static_cast<uint16_t>(std::stoi(argv[2]));
        }
        run_server(port);
    } else if (mode == "client") {
        std::string server_addr = "127.0.0.1";
        uint16_t port = 12345;
        
        if (argc > 2) {
            server_addr = argv[2];
        }
        if (argc > 3) {
            port = static_cast<uint16_t>(std::stoi(argv[3]));
        }
        
        run_client(server_addr, port);
    } else {
        std::cerr << "Invalid mode. Use 'server' or 'client'" << std::endl;
        return 1;
    }
    
    return 0;
}
