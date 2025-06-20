# find_package(benchmark REQUIRED)

# add_executable(buffer_allocation_benchmark
#     buffer_allocation_benchmark.cpp
# )

# target_include_directories(buffer_allocation_benchmark PRIVATE
#     ${CMAKE_SOURCE_DIR}/include
# )

# target_link_libraries(buffer_allocation_benchmark
#     benchmark::benchmark
#     ucxx::ucxx
#     glog::glog
#     fmt::fmt
#     ${CMAKE_DL_LIBS}
# )

# find_package(CUDA QUIET)
# if(CUDA_FOUND)
#     enable_language(CUDA)
#     target_link_libraries(buffer_allocation_benchmark ${CUDA_LIBRARIES})
#     target_include_directories(buffer_allocation_benchmark PRIVATE ${CUDA_INCLUDE_DIRS})
#     target_compile_definitions(buffer_allocation_benchmark PRIVATE CUDA_AVAILABLE)
# endif()

# target_compile_options(buffer_allocation_benchmark PRIVATE
#     -Wall -Wextra -Wno-unused-parameter -g -O2 -std=c++20
# )


# # CUDA IPC Tag Send/Recv Benchmark
# add_executable(cuda_ipc_tag_benchmark
#     cuda_ipc_tag_benchmark.cpp
#     ${CMAKE_SOURCE_DIR}/src/fast_channel_manager.cpp
# )

# target_include_directories(cuda_ipc_tag_benchmark PRIVATE
#     ${CMAKE_SOURCE_DIR}/include
# )

# target_link_libraries(cuda_ipc_tag_benchmark
#     benchmark::benchmark
#     ucxx::ucxx
#     glog::glog
#     fmt::fmt
#     Threads::Threads
#     Folly::folly
#     ${CMAKE_DL_LIBS}
# )

# if(CUDA_FOUND)
#     target_link_libraries(cuda_ipc_tag_benchmark ${CUDA_LIBRARIES})
#     target_include_directories(cuda_ipc_tag_benchmark PRIVATE ${CUDA_INCLUDE_DIRS})
#     target_compile_definitions(cuda_ipc_tag_benchmark PRIVATE CUDA_AVAILABLE)
# endif()

# target_compile_options(cuda_ipc_tag_benchmark PRIVATE
#     -Wall -Wextra -Wno-unused-parameter -g -O2 -std=c++20
# )


# set_target_properties(buffer_allocation_benchmark PROPERTIES
#     RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/build/bin
# )

add_executable(perftest
    perftest.cpp
)

target_link_libraries(perftest
    ucxx::ucxx
    ${CMAKE_DL_LIBS}
)
target_compile_options(perftest PRIVATE -Wall -Wextra -Wno-unused-parameter -g -O2 -std=c++20)

find_package(rmm REQUIRED)
add_executable(test_server
    test_server.cpp
)

target_link_libraries(test_server
    ucxx::ucxx
    rmm::rmm
    ${CMAKE_DL_LIBS}
)
target_compile_options(test_server PRIVATE -Wall -Wextra -Wno-unused-parameter -g -O2 -std=c++20)


add_executable(test_client
    test_client.cpp
)

target_link_libraries(test_client
    ucxx::ucxx
    rmm::rmm
    ${CMAKE_DL_LIBS}
)
target_compile_options(test_client PRIVATE -Wall -Wextra -Wno-unused-parameter -g -O2 -std=c++20)
