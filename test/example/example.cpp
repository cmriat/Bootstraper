#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/when_all.hh>
#include <iostream>

using namespace seastar;

future<int> compute_task(int id) {
    std::cout << "Task " << id << " STARTED\n";
    co_await sleep(std::chrono::milliseconds(100 * id));
    std::cout << "Task " << id << " RESUMED after delay\n";
    
    int result = id * 10;
    co_await seastar::coroutine::maybe_yield();
    std::cout << "Task " << id << " RESUMED after yield\n";
    
    co_await sleep(std::chrono::milliseconds(50));
    std::cout << "Task " << id << " COMPLETED\n";
    
    co_return result;
}

future<> run_main() {
    future<int> task1 = compute_task(1);
    future<int> task2 = compute_task(2);
    future<int> task3 = compute_task(3);

    std::cout << "All tasks launched, waiting...\n";
    
    auto [result1, result2, result3] = co_await when_all(
        std::move(task1), 
        std::move(task2), 
        std::move(task3)
    );

    std::cout << "\nRESULTS:\n"
              << "  Task 1: " << result1.get() << "\n"
              << "  Task 2: " << result2.get() << "\n"
              << "  Task 3: " << result3.get() << "\n";
}

int main(int argc, char** argv) {
    app_template app;
    app.run(argc, argv, [] {
        return run_main().then([] {
            std::cout << "Application shutting down\n";
            return make_ready_future<>();
        });
    });
}