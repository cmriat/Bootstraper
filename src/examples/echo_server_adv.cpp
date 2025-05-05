#include <seastar/core/seastar.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/future-util.hh>
#include <seastar/net/api.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <iostream>
#include <fmt/core.h>
#include <csignal>

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

// 使用 Seastar 的 abort_source 来实现可中断的服务器
seastar::future<> service_loop(uint16_t port = 1234) {
    seastar::listen_options lo;
    lo.reuse_address = true;

    return seastar::do_with(
        seastar::listen(seastar::make_ipv4_address({port}), lo),
        seastar::abort_source(),
        seastar::gate(),
        [port] (auto& listener, auto& abort_src, auto& pending_ops) {

            // 设置信号处理 - 使用 app_template 的 run 方法来处理信号

            std::cout << "Echo server started on port " << port << "\n";
            std::cout << "Press Ctrl+C to stop the server\n";

            // 创建接受连接的循环
            return seastar::repeat([&listener, &abort_src, &pending_ops] () -> seastar::future<seastar::stop_iteration> {
                if (abort_src.abort_requested()) {
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                }

                return listener.accept().then_wrapped([&pending_ops, &abort_src] (seastar::future<seastar::accept_result> f_res) {
                    if (abort_src.abort_requested()) {
                        return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                    }

                    try {
                        auto res = f_res.get(); // 使用 get() 而不是 get0()
                        // 使用 gate 跟踪所有活动连接
                        auto fut = seastar::with_gate(pending_ops, [conn = std::move(res.connection)] () mutable {
                            return handle_connection(std::move(conn)).handle_exception(
                                [] (std::exception_ptr ep) {
                                    try {
                                        std::rethrow_exception(ep);
                                    } catch (const std::exception& e) {
                                        fmt::print(stderr, "Connection error: {}\n", e.what());
                                    }
                                    return seastar::make_ready_future<>();
                                });
                        });
                        // 忽略返回值，但避免警告
                        (void)fut;
                        return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                    } catch (const std::exception& e) {
                        if (!abort_src.abort_requested()) {
                            fmt::print(stderr, "Accept failed: {}\n", e.what());
                            return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                        } else {
                            return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                        }
                    }
                });
            }).finally([&listener] {
                std::cout << "Closing listener...\n";
                return listener.abort_accept();
            });
        });
}

int main(int argc, char** argv) {
    seastar::app_template app;

    // 添加命令行选项
    app.add_options()
        ("port", boost::program_options::value<uint16_t>()->default_value(1234), "TCP server port");

    return app.run(argc, argv, [&app] {
        auto& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();

        std::cout << "Echo server starting on port " << port << "\n";
        std::cout << "Press Ctrl+C to stop the server\n";

        // 使用 defer 在程序退出时执行清理操作
        auto cleanup = seastar::defer([] {
            std::cout << "Echo server shutdown complete\n";
        });

        return service_loop(port).then([] {
            std::cout << "Service loop completed\n";
            return seastar::make_ready_future<>();
        });
    });
}
