#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <iostream>

class Resource {
public:
    Resource() {
        std::cout << "Resource created\n";
    }
    
    ~Resource() {
        std::cout << "Resource destroyed\n";
    }
    
    void use() {
        std::cout << "Resource being used\n";
    }
};

// A slow operation that returns a future
seastar::future<> slow_op(Resource& res) {
    res.use();
    // Simulate some asynchronous work
    // IMPORTANT: We capture 'res' by reference here, which is UNSAFE
    // This is to demonstrate a common mistake in asynchronous programming
    std::cout << "Starting sleep - Resource address: " << &res << "\n";
    return seastar::sleep(std::chrono::seconds(2)).then([&res] {
        std::cout << "After sleep - Resource address: " << &res << "\n";
        res.use();  // This is dangerous! The resource might be destroyed by now
        std::cout << "Slow operation completed\n";
    });
}

// INCORRECT way to use do_with - demonstrates a common mistake
seastar::future<> incorrect_example() {
    std::cout << "Starting incorrect_example\n";
    
    // Resource will be created here and destroyed when the future completes
    return seastar::do_with(Resource(), [] (Resource& res) {
        std::cout << "Inside do_with lambda\n";
        return slow_op(res);  // Problem: slow_op captures res by reference
    }).then([] {
        std::cout << "After do_with\n";
    });
}

// CORRECT way to use do_with - proper lifetime management
seastar::future<> correct_example() {
    std::cout << "\nStarting correct_example\n";
    
    // Resource will be created here and destroyed when the future completes
    return seastar::do_with(Resource(), [] (Resource& res) {
        std::cout << "Inside do_with lambda\n";
        
        // Capture the resource by value in the continuation
        return seastar::sleep(std::chrono::seconds(2)).then([res = res] () mutable {
            std::cout << "After sleep in correct example\n";
            res.use();
            std::cout << "Slow operation completed correctly\n";
            return seastar::make_ready_future<>();
        });
    }).then([] {
        std::cout << "After do_with (correct example)\n";
    });
}

int main(int argc, char** argv) {
    seastar::app_template app;
    
    return app.run(argc, argv, [] {
        // First run the incorrect example to demonstrate the problem
        return incorrect_example().then([] {
            // Then run the correct example
            return correct_example();
        }).then([] {
            std::cout << "\nAll done!\n";
            return seastar::make_ready_future<>();
        });
    });
}
