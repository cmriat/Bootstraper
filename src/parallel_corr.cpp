

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <vector>
seastar::future<bool> all_exist(std::vector<seastar::sstring> filenames) {
    bool res = true;
    co_await seastar::coroutine::parallel_for_each(filenames, [&res] (const seastar::sstring& name) -> seastar::future<> {
        res &= co_await seastar::file_exists(name);
    });
    co_return res;
}