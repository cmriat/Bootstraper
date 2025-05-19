/**
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: BSD-3-Clause
 */
#pragma once

#include <coroutine>
#include <memory>
#include <optional>
#include <utility>

#include <ucxx/api.h>
#include <ucxx/endpoint.h>
#include <ucxx/request.h>
#include <ucxx/worker.h>

namespace coucxx {
namespace coroutine {

/**
 * @brief Task type for coroutine operations
 *
 * This is a simple task type that can be used to return from coroutines.
 * It handles the promise_type and coroutine handle management.
 */
template <typename T = void>
class task {
public:
    struct promise_type {
        std::optional<T> value;
        std::exception_ptr exception;

        task get_return_object() {
            return task(std::coroutine_handle<promise_type>::from_promise(*this));
        }

        std::suspend_never initial_suspend() noexcept { return {}; }

        auto final_suspend() noexcept {
            struct awaiter {
                bool await_ready() noexcept { return false; }
                void await_resume() noexcept {}
                std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept {
                    return h.done() ? std::coroutine_handle<>{std::noop_coroutine()} : std::coroutine_handle<>{h};
                }
            };
            return awaiter{};
        }

        template <typename U>
        void return_value(U&& value) {
            this->value = std::forward<U>(value);
        }

        void unhandled_exception() {
            exception = std::current_exception();
        }
    };

    task() : handle_(nullptr) {}

    explicit task(std::coroutine_handle<promise_type> handle) : handle_(handle) {}

    task(task&& other) noexcept : handle_(std::exchange(other.handle_, nullptr)) {}

    task& operator=(task&& other) noexcept {
        if (this != &other) {
            if (handle_) {
                handle_.destroy();
            }
            handle_ = std::exchange(other.handle_, nullptr);
        }
        return *this;
    }

    ~task() {
        if (handle_) {
            handle_.destroy();
        }
    }

    bool is_ready() const { return !handle_ || handle_.done(); }

    T get_result() {
        if (!handle_) {
            throw std::runtime_error("Task has no valid handle");
        }
        if (!handle_.done()) {
            throw std::runtime_error("Task is not complete");
        }
        if (handle_.promise().exception) {
            std::rethrow_exception(handle_.promise().exception);
        }
        if constexpr (!std::is_void_v<T>) {
            return std::move(handle_.promise().value.value());
        }
    }

private:
    std::coroutine_handle<promise_type> handle_;
};

// Specialization for void
template<>
struct task<void>::promise_type {
    std::exception_ptr exception;

    task<void> get_return_object() {
        return task<void>(std::coroutine_handle<promise_type>::from_promise(*this));
    }

    std::suspend_never initial_suspend() noexcept { return {}; }

    auto final_suspend() noexcept {
        struct awaiter {
            bool await_ready() noexcept { return false; }
            void await_resume() noexcept {}
            std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept {
                return h.done() ? std::coroutine_handle<>{std::noop_coroutine()} : std::coroutine_handle<>{h};
            }
        };
        return awaiter{};
    }

    void return_void() {}

    void unhandled_exception() {
        exception = std::current_exception();
    }
};

/**
 * @brief Awaitable for tag send operations
 *
 * This class implements the awaitable interface for tag send operations.
 * It can be used with co_await to send data using UCX tag API.
 */
class tag_send_awaitable {
public:
    tag_send_awaitable(std::shared_ptr<ucxx::Endpoint> endpoint,
                      void* buffer,
                      size_t length,
                      ucxx::Tag tag)
        : endpoint_(std::move(endpoint)),
          buffer_(buffer),
          length_(length),
          tag_(tag),
          request_(nullptr) {}

    bool await_ready();
    void await_suspend(std::coroutine_handle<> handle);
    void await_resume();

private:
    std::shared_ptr<ucxx::Endpoint> endpoint_;
    void* buffer_;
    size_t length_;
    ucxx::Tag tag_;
    std::shared_ptr<ucxx::Request> request_;
    std::coroutine_handle<> handle_;

    // Wrapper function that will be used as a callback
    static void callback_wrapper(ucs_status_t status, std::shared_ptr<void> user_data);
};

/**
 * @brief Awaitable for tag receive operations
 *
 * This class implements the awaitable interface for tag receive operations.
 * It can be used with co_await to receive data using UCX tag API.
 */
class tag_recv_awaitable {
public:
    tag_recv_awaitable(std::shared_ptr<ucxx::Worker> worker,
                      void* buffer,
                      size_t length,
                      ucxx::Tag tag,
                      ucxx::TagMask tag_mask = ucxx::TagMaskFull)
        : worker_(std::move(worker)),
          buffer_(buffer),
          length_(length),
          tag_(tag),
          tag_mask_(tag_mask),
          request_(nullptr) {}

    bool await_ready();
    void await_suspend(std::coroutine_handle<> handle);
    size_t await_resume();

    // Get the sender tag from the completed operation
    ucxx::Tag sender_tag() const { return tag_; }

    // Get the received length from the completed operation
    size_t received_length() const { return length_; }

private:
    std::shared_ptr<ucxx::Worker> worker_;
    void* buffer_;
    size_t length_;
    ucxx::Tag tag_;
    ucxx::TagMask tag_mask_;
    std::shared_ptr<ucxx::Request> request_;
    std::coroutine_handle<> handle_;

    // Wrapper function that will be used as a callback
    static void callback_wrapper(ucs_status_t status, std::shared_ptr<void> user_data);
};

/**
 * @brief Helper function to create a tag send awaitable
 *
 * @param endpoint The UCX endpoint to send data through
 * @param buffer Pointer to the data to send
 * @param length Length of the data to send
 * @param tag Tag to use for the send operation
 * @return tag_send_awaitable Awaitable object for the send operation
 */
inline tag_send_awaitable tag_send(std::shared_ptr<ucxx::Endpoint> endpoint,
                                  void* buffer,
                                  size_t length,
                                  ucxx::Tag tag) {
    return tag_send_awaitable(std::move(endpoint), buffer, length, tag);
}

/**
 * @brief Helper function to create a tag receive awaitable
 *
 * @param worker The UCX worker to receive data on
 * @param buffer Pointer to the buffer to receive data into
 * @param length Length of the buffer
 * @param tag Tag to match for the receive operation
 * @param tag_mask Mask to apply to the tag (default: match all bits)
 * @return tag_recv_awaitable Awaitable object for the receive operation
 */
inline tag_recv_awaitable tag_recv(std::shared_ptr<ucxx::Worker> worker,
                                  void* buffer,
                                  size_t length,
                                  ucxx::Tag tag,
                                  ucxx::TagMask tag_mask = ucxx::TagMaskFull) {
    return tag_recv_awaitable(std::move(worker), buffer, length, tag, tag_mask);
}

} // namespace coroutine
} // namespace coucxx
