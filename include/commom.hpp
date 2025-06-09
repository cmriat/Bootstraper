#pragma once
#include "coroutine_rdma_manager.hpp"
using namespace btsp;


class State {
public:
    std::atomic<bool> should_shutdown{false};
    std::shared_ptr<FastChannelManager> rdma_manager;

    void request_shutdown() {
        should_shutdown.store(true);
    }

    bool is_shutdown_requested() const {
        return should_shutdown.load();
    }

    void set_rdma_manager(std::shared_ptr<FastChannelManager> manager) {
        rdma_manager = manager;
    }

    std::shared_ptr<FastChannelManager> get_rdma_manager() {
        return rdma_manager;
    }
};