#include <iostream>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <string>
class my_service {
public:
    std::string _str;
    my_service(const std::string& str) : _str(str) {}
    seastar::future<> run() {
        std::cerr << "running on " << seastar::this_shard_id() << " with " << _str << "\n";
        return seastar::make_ready_future<>();
    }
    seastar::future<> stop() {
        return seastar::make_ready_future<>();
    }
};

seastar::sharded<my_service> service;

seastar::future<> f() {
    return service.start("hello").then([] {
        return service.invoke_on_all([] (my_service& local_service) {
            return local_service.run();
        });
    }).then([] {
        return service.stop();
    });
}