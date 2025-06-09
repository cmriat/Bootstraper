#pragma once
#include "rpc/coroutine_rdma_manager.hpp"
using namespace btsp;


class State {
public:
    std::atomic<bool> should_shutdown{false};
    std::shared_ptr<CoroutineRdmaManager> rdma_manager;

    void request_shutdown() {
        should_shutdown.store(true);
    }

    bool is_shutdown_requested() const {
        return should_shutdown.load();
    }

    void set_rdma_manager(std::shared_ptr<CoroutineRdmaManager> manager) {
        rdma_manager = manager;
    }

    std::shared_ptr<CoroutineRdmaManager> get_rdma_manager() {
        return rdma_manager;
    }
};