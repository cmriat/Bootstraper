cmake_minimum_required(VERSION 3.10)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project (SeastarExample)

# Set C++ standard to match Seastar (C++20)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Define source directories
set(SRC src)
set(EXAMPLES_SRC src/examples)
set(IS_EXAMPLE TRUE)

# Find Seastar package
find_package(Seastar REQUIRED)

# Add global compile definitions for all targets
add_compile_definitions(SEASTAR_SCHEDULING_GROUPS_COUNT=16)

# Add bootstrap library
add_library(bootstrap
  ${SRC}/bootstrap/unique_id_exchange.cpp
  ${SRC}/bootstrap/unique_id_server.cpp
  ${SRC}/bootstrap/unique_id_client.cpp)
target_link_libraries(bootstrap Seastar::seastar)

# Add executables for all cpp files
if(IS_EXAMPLE)
  # Use examples directory
  add_executable(simplest
    ${EXAMPLES_SRC}/simplest.cpp)
  add_executable(continuation
    ${EXAMPLES_SRC}/contiuation.cpp)
  add_executable(parasleep
    ${EXAMPLES_SRC}/boilerplate.cpp
    ${EXAMPLES_SRC}/parasleep.cpp)

  add_executable(fast_vfuture
    ${EXAMPLES_SRC}/boilerplate.cpp
    ${EXAMPLES_SRC}/fast_vfuture.cpp)
  add_executable(slow_vfuture
    ${EXAMPLES_SRC}/boilerplate.cpp
    ${EXAMPLES_SRC}/slow_vfuture.cpp)
  add_executable(generator
    ${EXAMPLES_SRC}/generator.cpp)

  # Add our unique_id_exchange example
  add_executable(unique_id_exchange_example
    ${EXAMPLES_SRC}/unique_id_exchange_example.cpp)
  target_link_libraries(unique_id_exchange_example bootstrap)

  # Add simple server and client examples
  add_executable(simple_server
    ${EXAMPLES_SRC}/simple_server.cpp)
  target_link_libraries(simple_server bootstrap)

  add_executable(simple_client
    ${EXAMPLES_SRC}/simple_client.cpp)
  target_link_libraries(simple_client bootstrap)

  # Add TCP server and client examples
  add_executable(tcp_server
    ${EXAMPLES_SRC}/tcp_server.cpp)
  target_link_libraries(tcp_server bootstrap)

  add_executable(tcp_client
    ${EXAMPLES_SRC}/tcp_client.cpp)
  target_link_libraries(tcp_client bootstrap)

  # Add do_with example
  add_executable(do_with_example
    ${EXAMPLES_SRC}/do_with_example.cpp)
  target_link_libraries(do_with_example Seastar::seastar)

  # Add echo server examples
  add_executable(echo_server
    ${EXAMPLES_SRC}/echo_server.cpp)
  target_link_libraries(echo_server Seastar::seastar)

  # Add coroutine-based echo server example
  add_executable(echo_server_corr
    ${EXAMPLES_SRC}/echo_server_corr.cpp)
  target_link_libraries(echo_server_corr Seastar::seastar)

else()
  # Use regular source directory
  add_executable(simplest
    ${SRC}/simplest.cpp)
  add_executable(continuation
    ${SRC}/contiuation.cpp)
  add_executable(parasleep
    ${SRC}/boilerplate.cpp
    ${SRC}/parasleep.cpp)

  add_executable(fast_vfuture
    ${SRC}/boilerplate.cpp
    ${SRC}/fast_vfuture.cpp)
  add_executable(slow_vfuture
    ${SRC}/boilerplate.cpp
    ${SRC}/slow_vfuture.cpp)
  add_executable(generator
    ${SRC}/generator.cpp)

  # Add our unique_id_exchange example
  add_executable(unique_id_exchange_example
    ${SRC}/examples/unique_id_exchange_example.cpp)
  target_link_libraries(unique_id_exchange_example bootstrap)

endif()

# Link Seastar to all executables
target_link_libraries(simplest Seastar::seastar)
target_link_libraries(continuation Seastar::seastar)
target_link_libraries(parasleep Seastar::seastar)

target_link_libraries(fast_vfuture Seastar::seastar)
target_link_libraries(slow_vfuture Seastar::seastar)

# Add additional flags that might be needed for linking
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-as-needed")

# Add include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Print Seastar information for debugging
# message(STATUS "Seastar include dirs: ${Seastar_INCLUDE_DIRS}")
# message(STATUS "Seastar libraries: ${Seastar_LIBRARIES}")
# message(STATUS "Seastar definitions: ${Seastar_DEFINITIONS}")

# Add UCXX test directory
option(BUILD_UCXX_TEST "Build UCXX installation test" ON)
if(BUILD_UCXX_TEST)
  add_subdirectory(${SRC}/ucxx)
endif()

# Find UCXX package for demo
option(BUILD_UCXX_DEMO "Build UCXX Seastar demo" ON)
if(BUILD_UCXX_DEMO)
  # Find UCX package
  find_package(ucx REQUIRED)

  # Find UCXX package
  find_package(ucxx REQUIRED)

  # Add RDMA Seastar demo
  add_executable(rdma_seastar_demo
    ${SRC}/demo/endpoint_manager.cpp
    ${SRC}/demo/rdma_server.cpp
    ${SRC}/demo/rdma_client.cpp
    ${SRC}/demo/rdma_seastar_demo.cpp)

  target_link_libraries(rdma_seastar_demo
    Seastar::seastar
    ucxx::ucxx)

  # Set output directory
  set_target_properties(rdma_seastar_demo
    PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

  # Add RPC benchmark from src/rpc
  add_executable(rpc_benchmark
    ${SRC}/rpc/rpc_seastar.cpp)

  target_link_libraries(rpc_benchmark
    Seastar::seastar)

  set_target_properties(rpc_benchmark
    PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

  # Add simple RDMA/RPC benchmark
  option(BUILD_SIMPLE_BENCHMARK "Build simple RDMA/RPC benchmark" ON)
  if(BUILD_SIMPLE_BENCHMARK)
    add_executable(simple_benchmark
      ${SRC}/demo/endpoint_manager.cpp
      ${SRC}/demo/rdma_server.cpp
      ${SRC}/demo/rdma_client.cpp
      ${SRC}/demo/simple_benchmark.cpp)

    target_link_libraries(simple_benchmark
      Seastar::seastar
      ucxx::ucxx)

    set_target_properties(simple_benchmark
      PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
  endif()
endif()
