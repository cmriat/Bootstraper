
#define LIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE 1
#define UCXX_ENABLE_RMM                                1

#include <cassert>
#include <chrono>
#include <format>
#include <iostream>
#include <memory>
#include <numeric>
#include <rmm/cuda_stream_view.hpp>
#include <rmm/device_buffer.hpp>
// #include <rmm/logger_impl/logger_impl.hpp>
#include <rmm/mr/device/device_memory_resource.hpp>
#include <rmm/mr/device/pool_memory_resource.hpp>
#include <thread>
#include <ucs/memory/memory_type.h>
#include <unistd.h>
#include <vector>

#include <chrono>
#include <ucxx/api.h>
#include <ucxx/utils/sockaddr.h>
#include <ucxx/utils/ucx.h>

size_t GiB = 1024 * 1024 * 1024;

int main(int argc, char** argv)
{

  ucxx::ConfigMap config = {
    { "TLS","cuda_copy,cuda_ipc,tcp" }
  };

  RMM_CUDA_TRY(cudaSetDevice(0));
  rmm::mr::cuda_memory_resource cuda_mr;
  auto initial_size = rmm::percent_of_free_device_memory(50);
  rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource> pool_mr{&cuda_mr, initial_size};
  rmm::mr::set_current_device_resource(&pool_mr);
  // Setup: create UCP context, worker, listener and client endpoint.
  auto context  = ucxx::createContext(config, ucxx::Context::defaultFeatureFlags);
  auto worker   = context->createWorker();
  auto endpoint = worker->createEndpointFromHostname("127.0.0.1", 12345);

  // Initialize worker progress
  std::vector<std::shared_ptr<ucxx::Request>> requests;

  // Allocate send buffers
  using rmm::device_buffer;
  using std::shared_ptr;
  std::vector<shared_ptr<device_buffer>> sendBuffers;
  for (int i = 0; i < 3; ++i) {
    auto buffer = std::make_shared<device_buffer>(40960, rmm::cuda_stream_default);
    sendBuffers.push_back(buffer);
  }

  for (auto& buffer : sendBuffers) {
    auto request = endpoint->tagSend(buffer->data(), buffer->size(), ucxx::Tag(0));
    requests.push_back(request);
  }
  rmm::cuda_stream_default.synchronize();
  for (auto& request : requests) {
    while (!request->isCompleted()) {
      worker->progress();
    }
  }

  sendBuffers.clear();
  requests.clear();

  // while (!requests.empty()) {
  //   for (auto it = requests.begin(); it != requests.end();) {
  //     if ((*it)->isCompleted()) {
  //       it = requests.erase(it);
  //     } else {
  //       worker->progress();
  //       ++it;
  //     }
  //   }
  // }

  for (int i = 0; i < 3; ++i) {
    auto buffer = std::make_shared<device_buffer>(GiB, rmm::cuda_stream_default);
    sendBuffers.push_back(buffer);
  }
  auto start = std::chrono::high_resolution_clock::now();
  for (auto& buffer : sendBuffers) {
    auto request = endpoint->tagSend(buffer->data(), buffer->size(), ucxx::Tag(0));
    requests.push_back(request);
  }

  for (auto& request : requests) {
    while (!request->isCompleted()) {
      worker->progress();
    }
  }
  rmm::cuda_stream_default.synchronize();
  auto end = std::chrono::high_resolution_clock::now();
  // 计算持续时间，单位可以选择 milliseconds 或 microseconds
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  // std::cout << "代码段执行时间: " << duration.count() << " mms" << std::endl;
  double speed = (3 * GiB) / (duration.count() / 1000000.0);
  std::cout << "Running bandwidth test from GPU 0 to GPU 1 with UCX CS" << std::endl;
  std::cout << "Bandwidth: " << speed / 1024 / 1024 / 1024 << " GB/s" << std::endl;
  usleep(10000000);
  return 0;
}