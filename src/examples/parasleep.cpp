#include <seastar/core/sleep.hh>
#include <iostream>

seastar::future<> f() {
    std::cout << "Sleeping... " << std::flush;
    using namespace std::chrono_literals;
    
    (void)seastar::sleep(200ms).then([] { std::cout << "200ms " << std::flush; });
    (void)seastar::sleep(100ms).then([] { std::cout << "100ms " << std::flush; });
    
    return seastar::sleep(1s).then([] { std::cout << "Done.\n"; });
}
