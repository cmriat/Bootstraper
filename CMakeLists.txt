cmake_minimum_required(VERSION 3.10)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project (SeastarExample)

# Set C++ standard to match Seastar (C++20)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Define source directory
set(SRC src)

# Find Seastar package
find_package(Seastar REQUIRED)

# Add global compile definitions for all targets
add_compile_definitions(SEASTAR_SCHEDULING_GROUPS_COUNT=16)

# Add executables for all cpp files
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

# Link Seastar to all executables
target_link_libraries(simplest Seastar::seastar)
target_link_libraries(continuation Seastar::seastar)
target_link_libraries(parasleep Seastar::seastar)

target_link_libraries(fast_vfuture Seastar::seastar)
target_link_libraries(slow_vfuture Seastar::seastar)

# Add additional flags that might be needed for linking
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-as-needed")

# Print Seastar information for debugging
# message(STATUS "Seastar include dirs: ${Seastar_INCLUDE_DIRS}")
# message(STATUS "Seastar libraries: ${Seastar_LIBRARIES}")
# message(STATUS "Seastar definitions: ${Seastar_DEFINITIONS}")