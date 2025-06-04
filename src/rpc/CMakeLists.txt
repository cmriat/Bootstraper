# add_executable(rpc_sleep
#     rpc_runner.cpp
#     rpc_server.cpp
#     rpc_client.cpp
#     tensor_coroutines.cpp
# )

# add_executable(rpc_example
#     eg.cpp
# )

add_executable(tensor_transfer_example
    tensor_transfer_backend.cpp
    tensor_transfer_example.cpp
)

# add_executable(tensor_transfer_rpc
#     tensor_transfer_backend.cpp
#     tensor_transfer_rpc.cpp
# )

# add_executable(simple_rpc
#     simple_rpc.cpp
# )

# target_include_directories(rpc_sleep PRIVATE ${CMAKE_SOURCE_DIR}/include)
# target_include_directories(rpc_example PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(tensor_transfer_example PRIVATE ${CMAKE_SOURCE_DIR}/include)
# target_include_directories(tensor_transfer_rpc PRIVATE ${CMAKE_SOURCE_DIR}/include)
# target_include_directories(simple_rpc PRIVATE ${CMAKE_SOURCE_DIR}/include)

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

target_link_libraries(tensor_transfer_example
    Seastar::seastar
    fmt::fmt
    Boost::program_options
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

# target_link_libraries(simple_rpc
#     Seastar::seastar
#     fmt::fmt
#     Boost::program_options
# )

# target_compile_options(rpc_sleep PRIVATE -Wall -Wextra -Wno-unused-parameter)
# target_compile_options(rpc_example PRIVATE -Wall -Wextra -Wno-unused-parameter)
target_compile_options(tensor_transfer_example PRIVATE -Wall -Wextra -Wno-unused-parameter)
# target_compile_options(tensor_transfer_rpc PRIVATE -Wall -Wextra -Wno-unused-parameter)
# target_compile_options(simple_rpc PRIVATE -Wall -Wextra -Wno-unused-parameter)
