cmake_minimum_required(VERSION 3.10)
project(bootstrapper)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/build/bin)

# TODO: change root dir
set(dpdk_INCLUDE_DIR /root/kernel_dev/Bootstraper/.pixi/envs/default/include/seastar)

add_definitions(
    -DSEASTAR_SCHEDULING_GROUPS_COUNT=16
    -DSEASTAR_API_LEVEL=7
    -DBOOST_PROGRAM_OPTIONS_DYN_LINK
    -DBOOST_PROGRAM_OPTIONS_NO_LIB
    -DBOOST_THREAD_DYN_LINK
    -DBOOST_THREAD_NO_LIB
    -DFMT_SHARED
    -DSEASTAR_API_LEVEL=7
    -DSEASTAR_BUILD_SHARED_LIBS
    -DSEASTAR_DEPRECATED_OSTREAM_FORMATTERS
    -DSEASTAR_HAVE_DPDK
    -DSEASTAR_LOGGER_COMPILE_TIME_FMT
    -DSEASTAR_LOGGER_TYPE_STDOUT
    -DSEASTAR_SCHEDULING_GROUPS_COUNT=16
    -DSEASTAR_SSTRING
)

find_package(Boost REQUIRED COMPONENTS program_options)
find_package(fmt REQUIRED)
find_package(Threads REQUIRED)
find_package(ucx REQUIRED)
find_package(ucxx REQUIRED)

find_package (Seastar REQUIRED)
find_package (folly REQUIRED)
find_package (glog REQUIRED)


add_subdirectory(src)
add_subdirectory(test/example)
add_subdirectory(test/benchmark)
