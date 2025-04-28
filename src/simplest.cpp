#include <cstdint>
#include <ratio>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <iostream>
#include <chrono>

seastar::future<> sleep(std::chrono::duration<int64_t, std::ratio<1, 50>> dur);
int main(int argc, char** argv) {
    seastar::app_template app;
    app.run(argc, argv, [] {
        std::cout << "Hello, world!\n";
        
        return seastar::make_ready_future<>();
    });
    
    return 0;
}