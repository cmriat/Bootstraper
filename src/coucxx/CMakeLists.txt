# add_executable(ucxx_test
#     ucxx_test.cpp
# )

# add_executable(coucxx_test
#     co_tag.cpp
#     coucxx_test.cpp
# )

# add_library(ucxx_thread_manager
#     ucxx_thread_manager.cpp
# )

# target_include_directories(ucxx_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
# target_include_directories(coucxx_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
# target_include_directories(ucxx_thread_manager PRIVATE ${CMAKE_SOURCE_DIR}/include)

# target_link_libraries(ucxx_test
#     ucxx::ucxx
# )
# target_link_libraries(coucxx_test
#     ucxx::ucxx
# )
# target_link_libraries(ucxx_thread_manager
#     ucxx::ucxx
#     Seastar::seastar
# )

# target_compile_options(ucxx_test PRIVATE -Wall -Wextra -Wno-unused-parameter)
# target_compile_options(coucxx_test PRIVATE -Wall -Wextra -Wno-unused-parameter)
# target_compile_options(ucxx_thread_manager PRIVATE -Wall -Wextra -Wno-unused-parameter)
