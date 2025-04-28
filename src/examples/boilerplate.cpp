#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/util/log.hh>
#include <iostream>

extern seastar::future<> f();

int main(int argc, char** argv) {
    seastar::app_template app;
    try {
        app.run(argc, argv, f);
    } catch (...) {
        std::cerr << "Could not start application: "
                  << std::current_exception() << "\n";
        return 1;
    }
    return 0;
}
