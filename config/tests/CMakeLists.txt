add_executable(
    config_tests
    json_tests.cc)
target_link_libraries(config_tests GTest::gtest_main GTest::gmock config)

include(GoogleTest)
gtest_discover_tests(config_tests)
