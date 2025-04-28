#include <seastar/core/circular_buffer.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/generator.hh>
#include <utility>

struct Ingredients {
    operator bool() const;
    Ingredients alloc(
        // TODO
    );
};

struct Preprocessed {
    // TODO
};

struct Dish {
    // TODO
};

seastar::future<Preprocessed> prepare_ingredients(Ingredients&&);
seastar::future<Dish> cook_a_dish(Preprocessed&&);
seastar::future<> consume_a_dish(Dish&&);

seastar::coroutine::experimental::generator<Dish, seastar::circular_buffer>
make_dishes(seastar::coroutine::experimental::buffer_size_t max_dishes_on_table,
            Ingredients ingredients) {
    while (ingredients) {
        auto some_ingredients = ingredients.alloc();
        auto preprocessed = co_await prepare_ingredients(std::move(some_ingredients));
        co_yield co_await cook_a_dish(std::move(preprocessed));
    }
}

seastar::future<> have_a_dinner(unsigned max_dishes_on_table) {
    auto ingredients = Ingredients{};
    auto dishes = make_dishes(seastar::coroutine::experimental::buffer_size_t{max_dishes_on_table}, 
                 std::move(ingredients));
    while (auto dish = co_await dishes()) {
        co_await consume_a_dish(std::move(*dish));
    }
}