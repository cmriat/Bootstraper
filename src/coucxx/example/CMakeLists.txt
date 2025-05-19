# =================================================================================
# SPDX-FileCopyrightText: Copyright (c) 2023-2025, NVIDIA CORPORATION & AFFILIATES.
# SPDX-License-Identifier: BSD 3-Clause License
# =================================================================================

cmake_minimum_required(VERSION 3.10)
project(coucxx_examples LANGUAGES CXX)

# Find UCX package
find_package(ucx REQUIRED)

# Find UCXX package
find_package(ucxx REQUIRED)

# This function takes in an example name and example source and handles setting all of the
# associated properties and linking to build the example
function(ConfigureUCXXTest CMAKE_TEST_NAME)
  add_executable(${CMAKE_TEST_NAME} ${ARGN})
  set_target_properties(
    ${CMAKE_TEST_NAME}
    PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
               CXX_STANDARD 20
               CXX_STANDARD_REQUIRED ON
  )
  target_include_directories(${CMAKE_TEST_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../..)
  target_link_libraries(${CMAKE_TEST_NAME} PRIVATE ucxx::ucxx)
endfunction()

# Configure the UCXX test executable
ConfigureUCXXTest(ucxx_test basic.cpp)

# Add a custom target to run the test
add_custom_target(
  run_ucxx_test
  COMMAND ucxx_test
  DEPENDS ucxx_test
  WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
  COMMENT "Running UCXX installation test"
)

# Add the coroutine tag example directly with the implementation file
add_executable(coroutine_tag_example
  coroutine_tag_example.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/../coroutine/tag_operations.cpp
)
set_target_properties(
  coroutine_tag_example
  PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
             CXX_STANDARD 20
             CXX_STANDARD_REQUIRED ON
)
target_include_directories(coroutine_tag_example PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../..)
target_link_libraries(coroutine_tag_example PRIVATE ucxx::ucxx)

# Add a custom target to run the coroutine example
add_custom_target(
  run_coroutine_tag_example
  COMMAND coroutine_tag_example
  DEPENDS coroutine_tag_example
  WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
  COMMENT "Running UCXX coroutine tag example"
)
