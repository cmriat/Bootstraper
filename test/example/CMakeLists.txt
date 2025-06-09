add_executable(coexample
    example.cpp
)


# target_include_directories(coexample PRIVATE ${CMAKE_SOURCE_DIR}/include)


target_link_libraries(coexample
    Seastar::seastar
)

target_compile_options(coexample PRIVATE -Wall -Wextra -Wno-unused-parameter)
