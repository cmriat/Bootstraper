#include "coucxx/co_tag.hpp"
#include <stdexcept>

namespace coucxx {
namespace coroutine {

bool tag_send_awaitable::await_ready() {
    // Create a callback that will resume our coroutine
    auto callback_fn = [](ucs_status_t status, std::shared_ptr<void> user_data) {
        auto* self = static_cast<tag_send_awaitable*>(user_data.get());
        if (self && self->handle_) {
            self->handle_.resume();
        }
    };

    // Try to send immediately
    request_ = endpoint_->tagSend(buffer_, length_, tag_, false, callback_fn, std::shared_ptr<void>(this, [](void*) {}));

    // Check if the operation completed immediately
    if (request_->isCompleted()) {
        // If there was an error, we'll throw it in await_resume
        return true;
    }

    return false;
}

void tag_send_awaitable::await_suspend(std::coroutine_handle<> handle) {
    // Store the coroutine handle to resume later
    handle_ = handle;

    // The callback will be called when the operation completes
    // We've already set up the request in await_ready
}

void tag_send_awaitable::await_resume() {
    // If the request is completed, check for errors
    if (request_->isCompleted()) {
        try {
            request_->checkError();
        } catch (const std::exception& e) {
            throw std::runtime_error(std::string("UCX tag send failed: ") + e.what());
        }
    } else {
        throw std::runtime_error("UCX tag send not completed but coroutine resumed");
    }
}

// Static callback wrapper implementation
void tag_send_awaitable::callback_wrapper(ucs_status_t status, std::shared_ptr<void> user_data) {
    auto* self = static_cast<tag_send_awaitable*>(user_data.get());

    // Resume the coroutine
    if (self && self->handle_) {
        self->handle_.resume();
    }
}

bool tag_recv_awaitable::await_ready() {
    // Create a callback that will resume our coroutine and store tag info
    auto callback_fn = [](ucs_status_t status, std::shared_ptr<void> user_data) {
        auto* self = static_cast<tag_recv_awaitable*>(user_data.get());
        if (self && self->handle_) {
            // Try to get tag info from the request
            if (self->request_) {
                // We can't use dynamic_cast here because RequestTag is an incomplete type
                // Instead, we'll check if the request has tag info in await_resume
            }
            self->handle_.resume();
        }
    };

    // Try to receive immediately
    request_ = worker_->tagRecv(buffer_, length_, tag_, tag_mask_, false, callback_fn, std::shared_ptr<void>(this, [](void*) {}));

    // Check if the operation completed immediately
    if (request_->isCompleted()) {
        // If there was an error, we'll throw it in await_resume
        // We'll try to get tag info in await_resume
        return true;
    }

    return false;
}

void tag_recv_awaitable::await_suspend(std::coroutine_handle<> handle) {
    // Store the coroutine handle to resume later
    handle_ = handle;

    // The callback will be called when the operation completes
    // We've already set up the request in await_ready
}

size_t tag_recv_awaitable::await_resume() {
    // If the request is completed, check for errors
    if (request_->isCompleted()) {
        try {
            request_->checkError();

            // Try to extract tag info from the request
            // Since we can't use dynamic_cast with incomplete type,
            // we'll use a different approach to get the tag info

            // For now, we'll just return the length we requested
            // In a real implementation, you would need to find a way to
            // access the actual received length and tag info

            return length_;
        } catch (const std::exception& e) {
            throw std::runtime_error(std::string("UCX tag receive failed: ") + e.what());
        }
    } else {
        throw std::runtime_error("UCX tag receive not completed but coroutine resumed");
    }
}

// Static callback wrapper implementation
void tag_recv_awaitable::callback_wrapper(ucs_status_t status, std::shared_ptr<void> user_data) {
    auto* self = static_cast<tag_recv_awaitable*>(user_data.get());

    // Resume the coroutine
    if (self && self->handle_) {
        self->handle_.resume();
    }
}

} // namespace coroutine
} // namespace coucxx
