#include <iostream>
#include <coroutine>
#include <exception>

template<typename T>
class generator {
public:
    struct promise_type {
        T current_value;
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void unhandled_exception() {
            std::terminate();
        }

        generator get_return_object() {
            return generator{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        
        std::suspend_always yield_value(T value) noexcept {
            current_value = value;
            return {};
        }
        
        void return_void() {}
    };
    
    generator(std::coroutine_handle<promise_type> h) : handle(h) {}
    
    generator(const generator&) = delete;
    generator& operator=(const generator&) = delete;
    generator(generator&& other) noexcept : handle(other.handle) {
        other.handle = nullptr;
    }
    generator& operator=(generator&& other) noexcept {
        if (this != &other) {
            if (handle) handle.destroy();
            handle = other.handle;
            other.handle = nullptr;
        }
        return *this;
    }
    
    ~generator() {
        if (handle) handle.destroy();
    }
    
    T operator()() {
        handle.resume();
        return handle.promise().current_value;
    }
    
    bool done() const {
        return !handle || handle.done();
    }
    
private:
    std::coroutine_handle<promise_type> handle;
};

generator<int> generateNumbers(int begin, int inc = 1) {
    for (int i = begin;; i += inc) {
        co_yield i;
    }
}

int main() {
    auto numbers = generateNumbers(-10);
    for (int i = 1; i <= 20; ++i) {
        std::cout << numbers() << " ";
    }
    std::cout << std::endl;
    return 0;
}
