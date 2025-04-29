#include <seastar/core/seastar.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/future-util.hh>
#include <seastar/net/api.hh>
#include <seastar/core/app-template.hh>
#include <iostream>
#include <fmt/core.h>

seastar::future<> handle_connection(seastar::connected_socket s) {
    auto out = s.output();
    auto in = s.input();
    return seastar::do_with(std::move(s), std::move(out), std::move(in),
            [] (auto& s, auto& out, auto& in) {
        // repeatly calls read() on input stream
        return seastar::repeat([&out, &in] {
            return in.read().then([&out] (auto buf) {
                // simply send back what received
                if (buf) {
                    return out.write(std::move(buf)).then([&out] {
                        return out.flush();
                    }).then([] {
                        return seastar::stop_iteration::no;
                    });
                // When read() eventually returns an empty buffer, we stop iteration
                } else {
                    return seastar::make_ready_future<seastar::stop_iteration>(
                            seastar::stop_iteration::yes);
                }
            });
        }).then([&out] {
            return out.close();
        });
    });
}

seastar::future<> service_loop() {
    seastar::listen_options lo;
    lo.reuse_address = true;
    
    return seastar::do_with(seastar::listen(seastar::make_ipv4_address({1234}), lo),
            [] (auto& listener) {
        return seastar::keep_doing([&listener] () {
            return listener.accept().then(
                    [] (seastar::accept_result res) {
                // Note we ignore, not return, the future returned by handle_connection()
                // so we do not wait for one connection to be handled before accepting the next one.
                (void)handle_connection(std::move(res.connection)).handle_exception(
                        [] (std::exception_ptr ep) {
                    fmt::print(stderr, "Could not handle connection: {}\n", ep);
                });
            });
        });
    });
}

int main(int argc, char** argv) {
    seastar::app_template app;
    
    return app.run(argc, argv, [] {
        std::cout << "Echo server started on port 1234\n";
        return service_loop().then([] {
            return seastar::make_ready_future<>();
        });
    });
}
