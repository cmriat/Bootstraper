add_executable(server
    coroutine_rdma_manager.cpp
    server.cpp
)

target_include_directories(server PRIVATE ${CMAKE_SOURCE_DIR}/include)

target_link_libraries(server
    Seastar::seastar
    ucxx::ucxx
    fmt::fmt
    Boost::program_options
    Folly::folly
    glog::glog
)

target_compile_options(server PRIVATE -Wall -Wextra -Wno-unused-parameter -g -O0 -std=c++20)