# add_executable(rpc_sleep
#     rpc_runner.cpp
#     rpc_server.cpp
#     rpc_client.cpp
#     tensor_coroutines.cpp
# )

# add_executable(rpc_example
#     eg.cpp
# )

# add_executable(tensor_transfer_example
#     tensor_transfer_backend.cpp
#     rdma_thread_manager.cpp
#     tensor_transfer_example.cpp
# )

# add_executable(tensor_transfer_rdma_example
#     tensor_transfer_backend.cpp
#     rdma_thread_manager.cpp
#     tensor_transfer_rdma_example.cpp
# )

# add_executable(rdma_manager_direct_example
#     tensor_transfer_backend.cpp
#     rdma_thread_manager.cpp
#     rdma_manager_direct_example.cpp
# )

add_executable(independent_rdma_example
    independent_rdma_manager.cpp
    independent_rdma_example.cpp
)

add_executable(tag_matching_example
    independent_rdma_manager.cpp
    tag_matching_example.cpp
)

add_executable(coroutine_rdma_example
    coroutine_rdma_manager.cpp
    coroutine_rdma_example.cpp
)

add_executable(coroutine_tag_matching_example
    coroutine_rdma_manager.cpp
    coroutine_tag_matching_example.cpp
)

add_executable(simple_coroutine_test
    coroutine_rdma_manager.cpp
    simple_coroutine_test.cpp
)

# add_executable(tensor_transfer_rpc
#     tensor_transfer_backend.cpp
#     tensor_transfer_rpc.cpp
# )

add_executable(simple_rpc
    coroutine_rdma_manager.cpp
    simple_rpc.cpp
)

# target_include_directories(rpc_sleep PRIVATE ${CMAKE_SOURCE_DIR}/include)
# target_include_directories(rpc_example PRIVATE ${CMAKE_SOURCE_DIR}/include)
# target_include_directories(tensor_transfer_example PRIVATE ${CMAKE_SOURCE_DIR}/include)
# target_include_directories(tensor_transfer_rdma_example PRIVATE ${CMAKE_SOURCE_DIR}/include)
# target_include_directories(rdma_manager_direct_example PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(independent_rdma_example PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(tag_matching_example PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(coroutine_rdma_example PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(coroutine_tag_matching_example PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(simple_coroutine_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
# target_include_directories(tensor_transfer_rpc PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(simple_rpc PRIVATE ${CMAKE_SOURCE_DIR}/include)

# target_link_libraries(rpc_sleep
#     Seastar::seastar
#     fmt::fmt
#     Boost::program_options
#     ucxx::ucxx
# )

# target_link_libraries(rpc_example
#     Seastar::seastar
#     fmt::fmt
#     Boost::program_options
#     ucxx::ucxx
# )

# target_link_libraries(tensor_transfer_example
#     Seastar::seastar
#     fmt::fmt
#     Boost::program_options
#     ucxx::ucxx
#     Folly::folly
#     glog::glog
# )

# target_link_libraries(tensor_transfer_rdma_example
#     Seastar::seastar
#     fmt::fmt
#     Boost::program_options
#     ucxx::ucxx
#     Folly::folly
#     glog::glog
# )

# target_link_libraries(rdma_manager_direct_example
#     Seastar::seastar
#     fmt::fmt
#     Boost::program_options
#     ucxx::ucxx
#     Folly::folly
#     glog::glog
# )

target_link_libraries(independent_rdma_example
    ucxx::ucxx
    Folly::folly
    glog::glog
)

target_link_libraries(tag_matching_example
    ucxx::ucxx
    Folly::folly
    glog::glog
)

target_link_libraries(coroutine_rdma_example
    ucxx::ucxx
    Folly::folly
    glog::glog
)

target_link_libraries(coroutine_tag_matching_example
    ucxx::ucxx
    Folly::folly
    glog::glog
)

target_link_libraries(simple_coroutine_test
    ucxx::ucxx
    Folly::folly
    glog::glog
)

# target_link_libraries(tensor_transfer_rpc
#     Seastar::seastar
#     fmt::fmt
#     Boost::program_options
#     ucxx::ucxx
#     Folly::folly
#     glog::glog
# )

target_link_libraries(simple_rpc
    Seastar::seastar
    ucxx::ucxx
    fmt::fmt
    Boost::program_options
)

# target_compile_options(rpc_sleep PRIVATE -Wall -Wextra -Wno-unused-parameter)
# target_compile_options(rpc_example PRIVATE -Wall -Wextra -Wno-unused-parameter)
# target_compile_options(tensor_transfer_example PRIVATE -Wall -Wextra -Wno-unused-parameter)
# target_compile_options(tensor_transfer_rdma_example PRIVATE -Wall -Wextra -Wno-unused-parameter)
# target_compile_options(rdma_manager_direct_example PRIVATE -Wall -Wextra -Wno-unused-parameter)
target_compile_options(independent_rdma_example PRIVATE -Wall -Wextra -Wno-unused-parameter)
target_compile_options(tag_matching_example PRIVATE -Wall -Wextra -Wno-unused-parameter)

# Coroutine examples need C++20
target_compile_options(coroutine_rdma_example PRIVATE -Wall -Wextra -Wno-unused-parameter -std=c++20)
target_compile_options(coroutine_tag_matching_example PRIVATE -Wall -Wextra -Wno-unused-parameter -std=c++20)
target_compile_options(simple_coroutine_test PRIVATE -Wall -Wextra -Wno-unused-parameter -std=c++20)
# target_compile_options(tensor_transfer_rpc PRIVATE -Wall -Wextra -Wno-unused-parameter)
target_compile_options(simple_rpc PRIVATE -Wall -Wextra -Wno-unused-parameter)
