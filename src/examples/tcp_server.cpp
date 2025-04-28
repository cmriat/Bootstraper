#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/do_with.hh>
#include <seastar/net/api.hh>
#include <iostream>
#include "bootstrap/unique_id.hpp"

using namespace seastar;

future<> handle_connection(connected_socket s, socket_address a) {
    auto out = s.output();
    auto in = s.input();
    
    // Create a unique ID
    bootstrap::unique_id id = bootstrap::unique_id::create();
    std::cout << "Generated UniqueID: " << id.to_string() << std::endl;
    
    // Send the unique ID to the client
    return do_with(std::move(out), std::move(in), std::move(s), id, 
                  [](output_stream<char>& out, input_stream<char>& in, connected_socket& s, bootstrap::unique_id& id) {
        return out.write(reinterpret_cast<const char*>(id.get_id().data()), bootstrap::unique_id::kIdSize)
            .then([&out] {
                return out.flush();
            }).then([&in] {
                return in.close();
            }).then([&out] {
                return out.close();
            }).then([] {
                std::cout << "Connection handled successfully" << std::endl;
                return make_ready_future<>();
            });
    });
}

future<> tcp_server(uint16_t port) {
    return do_with(listen(make_ipv4_address({port})), [] (auto& listener) {
        return keep_doing([&listener] () {
            return listener.accept().then(
                [] (accept_result ar) {
                    // Handle the connection in a background fiber
                    (void)handle_connection(std::move(ar.connection), ar.remote_address)
                        .handle_exception([] (std::exception_ptr ep) {
                            std::cerr << "Connection handling failed: " << ep << std::endl;
                        });
                    return make_ready_future<>();
                });
        }).handle_exception([] (std::exception_ptr ep) {
            std::cerr << "Error in accept: " << ep << std::endl;
        });
    });
}

int main(int argc, char** argv) {
    app_template app;
    
    app.add_options()
        ("port", boost::program_options::value<uint16_t>()->default_value(10000), "Server port");
    
    return app.run(argc, argv, [&app] {
        auto& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();
        
        std::cout << "Starting server on port " << port << std::endl;
        
        return tcp_server(port).then([] {
            return make_ready_future<int>(0);
        });
    });
}
