#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/future-util.hh>
#include <seastar/net/api.hh>
#include <seastar/core/app-template.hh>
#include <seastar/util/defer.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/all.hh>
#include <iostream>
#include <fmt/core.h>
#include <csignal>

seastar::future<> handle_connection_coroutine(seastar::connected_socket s) {
    struct connection_ctx {
        seastar::connected_socket socket;
        seastar::output_stream<char> out;
        seastar::input_stream<char> in;

        explicit connection_ctx(seastar::connected_socket&& s) 
            : socket(std::move(s))
            , out(socket.output())
            , in(socket.input()) {}
        
        ~connection_ctx() = default;
    };

    auto ctx = std::make_unique<connection_ctx>(std::move(s));
    auto& out = ctx->out;
    auto& in = ctx->in;
    co_await seastar::repeat([&]() -> seastar::future<seastar::stop_iteration> {
        auto buf = co_await in.read();
        if (!buf) co_return seastar::stop_iteration::yes;
        
        co_await out.write(std::move(buf));
        co_await out.flush();
        co_await seastar::coroutine::maybe_yield();
        co_return seastar::stop_iteration::no;
    });
    co_await out.close();
    co_return;
}

seastar::future<> service_loop_coroutine(uint16_t port = 1234) {
    seastar::listen_options lo;
    lo.reuse_address = true;

    return seastar::do_with(
        seastar::listen(seastar::make_ipv4_address({port}), lo),
        seastar::gate(),
        seastar::abort_source(),
        [port](auto& listener, auto& pending_ops, auto& abort_src) {
            std::cout << "echo server started on port " << port << "\n";
            auto cleanup = seastar::defer([&] {
                // TODO: call_async_cleanup_function()
            });

            return seastar::repeat([&listener, &pending_ops, &abort_src]() -> seastar::future<seastar::stop_iteration> {
                if (abort_src.abort_requested()) {
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                }

                return listener.accept().then_wrapped([&pending_ops, &abort_src](seastar::future<seastar::accept_result> f_res) -> seastar::future<seastar::stop_iteration> {
                    if (f_res.failed()) {
                        auto ep = f_res.get_exception();
                        if (abort_src.abort_requested()) {
                            return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                        }
                        fmt::print(stderr, "accept failed: {}\n", ep);
                        return seastar::sleep(std::chrono::milliseconds(100)).then([] {
                            return seastar::stop_iteration::no;
                        });
                    }
                    auto res = f_res.get();

                    return seastar::with_gate(pending_ops, [conn = std::move(res.connection)]() mutable {
                        return handle_connection_coroutine(std::move(conn)).handle_exception(
                            [](std::exception_ptr ep) {
                                try {
                                    std::rethrow_exception(ep);
                                } catch (const std::exception& e) {
                                    fmt::print(stderr, "connection error: {}\n", e.what());
                                }
                                return seastar::make_ready_future<>();
                            });
                    }).then_wrapped([](auto f) {
                        try {
                            f.get();
                        } catch (...) {
                            // TODO: not ignore
                        }
                        return seastar::stop_iteration::no;
                    });
                });
            }).finally([&listener, &pending_ops] {
                std::cout << "closing listener\n";
                listener.abort_accept();
                return pending_ops.close().then([] {
                    std::cout << "all connections completed\n";
                });
            });
        });
}


int main(int argc, char** argv) {
    seastar::app_template app;

    app.add_options()
        ("port", boost::program_options::value<uint16_t>()->default_value(1234), "TCP server port");

    return app.run(argc, argv, [&app] {
        auto& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();

        auto cleanup = seastar::defer([] {
            // TODO: call_async_cleanup_function()
        });

        return service_loop_coroutine(port).then([] {
            std::cout << "service completed\n";
            return seastar::make_ready_future<>();
        });
    });
}
