# UCXX Coroutine Wrapper

This library provides C++20 coroutine wrappers for UCX tag send/receive operations. It allows you to use the `co_await` syntax to perform asynchronous UCX operations in a more readable and maintainable way.

## Overview

The library provides awaitable objects for UCX tag send and receive operations. These awaitables can be used with the `co_await` syntax to perform asynchronous operations without blocking the thread.

The main components are:

- `tag_send_awaitable`: An awaitable for tag send operations
- `tag_recv_awaitable`: An awaitable for tag receive operations
- Helper functions `tag_send` and `tag_recv` to create the awaitables
- `task<T>` template: A simple task type for coroutine operations

## Usage

### Basic Example

```cpp
#include <coucxx/coroutine/tag_operations.hpp>
#include <ucxx/api.h>
#include <vector>

// Coroutine function to send data
coucxx::coroutine::task<void> send_data_coroutine(std::shared_ptr<ucxx::Endpoint> endpoint,
                                                 std::vector<int>& data,
                                                 ucxx::Tag tag) {
  // Send the data using the coroutine awaitable
  co_await coucxx::coroutine::tag_send(endpoint, data.data(), data.size() * sizeof(int), tag);
}

// Coroutine function to receive data
coucxx::coroutine::task<size_t> receive_data_coroutine(std::shared_ptr<ucxx::Worker> worker,
                                                      std::vector<int>& buffer,
                                                      ucxx::Tag tag) {
  // Receive the data using the coroutine awaitable
  auto recv_awaitable = coucxx::coroutine::tag_recv(worker, buffer.data(), buffer.size() * sizeof(int), tag);
  size_t received_bytes = co_await recv_awaitable;

  // You can also get the tag used for matching
  ucxx::Tag tag_used = recv_awaitable.sender_tag();

  co_return received_bytes;
}
```

### Running Coroutines

To run a coroutine, you need to drive its execution by progressing the UCX worker:

```cpp
// Function to drive the coroutine execution
void run_coroutine(coucxx::coroutine::task<void>& task, std::shared_ptr<ucxx::Worker> worker) {
  auto progress = [&worker]() { worker->progress(); };

  // Keep progressing until the task is ready
  while (!task.is_ready()) {
    progress();
  }

  // Get the result (will throw if there was an error)
  try {
    task.get_result();
  } catch (const std::exception& e) {
    std::cerr << "Coroutine failed: " << e.what() << std::endl;
  }
}

// Function to drive the coroutine execution with result
template<typename T>
T run_coroutine_with_result(coucxx::coroutine::task<T>& task, std::shared_ptr<ucxx::Worker> worker) {
  auto progress = [&worker]() { worker->progress(); };

  // Keep progressing until the task is ready
  while (!task.is_ready()) {
    progress();
  }

  // Get the result (will throw if there was an error)
  try {
    return task.get_result();
  } catch (const std::exception& e) {
    std::cerr << "Coroutine failed: " << e.what() << std::endl;
    throw;
  }
}
```

## Building

To build the library, you need a C++20 compatible compiler. The library depends on UCXX.

```bash
# Configure with CMake
cmake -B build

# Build
cmake --build build
```

## Running the Example

The repository includes an example that demonstrates how to use the coroutine-based tag operations:

```bash
# Build the example
cmake --build build --target coroutine_tag_example

# Run the example
./build/bin/coroutine_tag_example
```

## Implementation Details

The implementation follows the approach described in the article about using C++20 coroutines with RDMA operations. The key points are:

1. In the `await_ready` method, we try to perform the operation immediately and return `true` if it completes.
2. In the `await_suspend` method, we store the coroutine handle to resume later when the operation completes.
3. In the `await_resume` method, we check for errors and return the result.

The implementation uses a lambda function as a callback that captures the awaitable object and resumes the coroutine when the operation completes. This approach avoids the need for static callback functions with C-style signatures.

## Limitations

- The library currently only supports tag send and receive operations. Other UCX operations like RMA, AMO, and stream operations are not yet supported.
- The tag receive operation currently doesn't provide the actual received length or sender tag from UCX. It returns the requested length and tag instead.
- The library does not support cancellation of operations.
- The library requires a C++20 compatible compiler.

## Future Work

- Improve the tag receive operation to provide the actual received length and sender tag
- Add support for other UCX operations
- Add support for cancellation
- Add support for timeouts
- Add support for error handling with exceptions
