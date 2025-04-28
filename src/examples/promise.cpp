#include <seastar/core/future.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <iostream>

seastar::future<> f() {
    return seastar::do_with(seastar::promise<>(), [](auto& promise) {
        (void)seastar::async([&promise] {
            using namespace std::chrono_literals;
            for (int i = 0; i < 10; ++i) {
                std::cout << "Tick " << i << std::endl;
                seastar::sleep(1s).get();
            }
            std::cout << std::endl;
            promise.set_value();
        });
        return promise.get_future();
    });
}