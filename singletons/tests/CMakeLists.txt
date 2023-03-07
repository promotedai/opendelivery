add_executable(
    singletons_tests
    env_tests.cc config_tests.cc user_agent_tests.cc counters_tests.cc feature_tests.cc)
target_link_libraries(singletons_tests GTest::gtest_main GTest::gmock singletons absl::flat_hash_map)
target_compile_definitions(singletons_tests PRIVATE TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/data")

include(GoogleTest)
gtest_discover_tests(singletons_tests)
