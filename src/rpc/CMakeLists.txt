add_executable(rpc_sleep
    rpc_runner.cpp
    rpc_server.cpp
    rpc_client.cpp
    tensor_coroutines.cpp
)

add_executable(rpc_example
    eg.cpp
)

target_include_directories(rpc_sleep PRIVATE ${CMAKE_SOURCE_DIR}/include)

target_link_libraries(rpc_sleep
    Seastar::seastar
    fmt::fmt
    Boost::program_options
    ucxx::ucxx
)

target_link_libraries(rpc_example
    Seastar::seastar
    fmt::fmt
    Boost::program_options
    ucxx::ucxx
)

target_compile_options(rpc_sleep PRIVATE -Wall -Wextra -Wno-unused-parameter)
target_compile_options(rpc_example PRIVATE -Wall -Wextra -Wno-unused-parameter)
