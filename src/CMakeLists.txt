add_executable(server
    fast_channel_manager.cpp
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

# Add CUDA libraries if available
if(CUDA_FOUND)
    target_link_libraries(server ${CUDA_LIBRARIES})
    target_include_directories(server PRIVATE ${CUDA_INCLUDE_DIRS})
endif()

target_compile_options(server PRIVATE -Wall -Wextra -Wno-unused-parameter -g -O0 -std=c++20)

add_executable(tensor_transfer_demo
    fast_channel_manager.cpp
    tensor_transfer_demo.cpp
)

target_include_directories(tensor_transfer_demo PRIVATE ${CMAKE_SOURCE_DIR}/include)

target_link_libraries(tensor_transfer_demo
    Seastar::seastar
    ucxx::ucxx
    fmt::fmt
    Boost::program_options
    Folly::folly
    glog::glog
)

# Add CUDA libraries if available
if(CUDA_FOUND)
    target_link_libraries(tensor_transfer_demo ${CUDA_LIBRARIES})
    target_include_directories(tensor_transfer_demo PRIVATE ${CUDA_INCLUDE_DIRS})
endif()

target_compile_options(tensor_transfer_demo PRIVATE -Wall -Wextra -Wno-unused-parameter -g -O0 -std=c++20)


add_executable(ucxx_test
    ucxx_test.cpp
)

target_include_directories(ucxx_test PRIVATE ${CMAKE_SOURCE_DIR}/include)

target_link_libraries(ucxx_test
    ucxx::ucxx
    fmt::fmt
    Boost::program_options
)

target_compile_options(ucxx_test PRIVATE -Wall -Wextra -Wno-unused-parameter -g -O0 -std=c++20)


add_executable(tensor_transfer
    fast_channel_manager.cpp
    tensor_tansfer.cpp
)

target_include_directories(tensor_transfer PRIVATE ${CMAKE_SOURCE_DIR}/include)

target_link_libraries(tensor_transfer
    ucxx::ucxx
    fmt::fmt
    Boost::program_options
    Folly::folly
    glog::glog
    pthread
)

# Add CUDA libraries if available
if(CUDA_FOUND)
    target_link_libraries(tensor_transfer ${CUDA_LIBRARIES})
    target_include_directories(tensor_transfer PRIVATE ${CUDA_INCLUDE_DIRS})
endif()

target_compile_options(tensor_transfer PRIVATE -Wall -Wextra -Wno-unused-parameter -g -O0 -std=c++20)
