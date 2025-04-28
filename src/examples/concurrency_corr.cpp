#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/all.hh>

seastar::future<int> read(int key);

seastar::future<int> parallel_sum(int key1, int key2) {
    auto[a, b] = co_await seastar::coroutine::all(
        [&] { return read(key1); },
        [&] { return read(key2); }
    );
    co_return a + b;
}
