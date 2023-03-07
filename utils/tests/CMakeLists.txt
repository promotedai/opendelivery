add_executable(utils_tests uuid_tests.cc time_tests.cc network_tests.cc geo_tests.cc)
target_link_libraries(utils_tests GTest::gtest_main utils)

include(GoogleTest)
gtest_discover_tests(utils_tests)
