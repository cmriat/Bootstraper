cmake_minimum_required(VERSION 3.10)
project(rdma_rpc_demo)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find packages
find_package(PkgConfig REQUIRED)
pkg_check_modules(SEASTAR REQUIRED seastar)
pkg_check_modules(FMT REQUIRED fmt)

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/include
    ${SEASTAR_INCLUDE_DIRS}
    ${FMT_INCLUDE_DIRS}
)

# Link directories
link_directories(
    ${SEASTAR_LIBRARY_DIRS}
    ${FMT_LIBRARY_DIRS}
)

# Add source files
set(SOURCES
    src/demo/endpoint_manager.cpp
    src/demo/rdma_client.cpp
    src/demo/rdma_server.cpp
    src/demo/p2p_manager.cpp
)

# Add executable
add_executable(p2p_rdma_demo src/demo/p2p_rdma_demo.cpp ${SOURCES})

# Link libraries
target_link_libraries(p2p_rdma_demo ${SEASTAR_LIBRARIES} ${FMT_LIBRARIES} ucxx)

# Set compiler flags
target_compile_options(p2p_rdma_demo PRIVATE ${SEASTAR_CFLAGS} -Wall -Wextra)