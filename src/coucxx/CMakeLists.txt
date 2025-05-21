add_executable(ucxx_test
    ucxx_test.cpp
)

add_executable(coucxx_test
    co_tag.cpp
    coucxx_test.cpp
)

target_include_directories(ucxx_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(coucxx_test PRIVATE ${CMAKE_SOURCE_DIR}/include)

target_link_libraries(ucxx_test
    ucxx::ucxx
)
target_link_libraries(coucxx_test
    ucxx::ucxx
)

target_compile_options(ucxx_test PRIVATE -Wall -Wextra -Wno-unused-parameter)
target_compile_options(coucxx_test PRIVATE -Wall -Wextra -Wno-unused-parameter)
