#include <seastar/core/future.hh>
#include <iostream>

seastar::future<int> fast() {
    return seastar::make_ready_future<int>(3);
}

seastar::future<> f() {
    return fast().then([] (int val) {
        std::cout << "Got " << val << "\n";
    });
}
