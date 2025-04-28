#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/later.hh>

seastar::future<> foo() {
    int n = 3;
    int m = co_await seastar::yield().then(seastar::coroutine::lambda([n] () -> seastar::future<int> {
        co_await seastar::coroutine::maybe_yield();
        co_return n;
    }));
    assert(n == m);
}