#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <iostream>
#include "rpc/rpc_common.hpp"

using namespace seastar;

int main(int ac, char** av) {
    app_template app;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(10000), "RPC server port")
        ("server", bpo::value<std::string>(), "Server address")
        ("compress", bpo::value<bool>()->default_value(false), "Compress RPC traffic");
    
    static logger slp_logger("sleep rpc");

    std::cout << "Initializing RPC application...\n";
    try {
        return app.run_deprecated(ac, av, [&app] {
            auto&& config = app.configuration();

            auto& slp_rpc = rpc_context::get_protocol();
            slp_rpc.set_logger(&slp_logger);

            if (config.count("server")) {
                return run_client(config);
            } else {
                return run_server(config);
            }
        });
    } catch (...) {
        std::cerr << "Could not start application: "
                  << std::current_exception() << "\n";
        return 1;
    }
}