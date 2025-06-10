find_package(benchmark REQUIRED)

add_executable(buffer_allocation_benchmark
    buffer_allocation_benchmark.cpp
)

target_include_directories(buffer_allocation_benchmark PRIVATE 
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(buffer_allocation_benchmark
    benchmark::benchmark
    ucxx::ucxx
    glog::glog
    fmt::fmt
    ${CMAKE_DL_LIBS}
)

find_package(CUDA QUIET)
if(CUDA_FOUND)
    enable_language(CUDA)
    target_link_libraries(buffer_allocation_benchmark ${CUDA_LIBRARIES})
    target_include_directories(buffer_allocation_benchmark PRIVATE ${CUDA_INCLUDE_DIRS})
    target_compile_definitions(buffer_allocation_benchmark PRIVATE CUDA_AVAILABLE)
endif()

target_compile_options(buffer_allocation_benchmark PRIVATE 
    -Wall -Wextra -Wno-unused-parameter -g -O2 -std=c++20
)

set_target_properties(buffer_allocation_benchmark PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/build/bin
)
