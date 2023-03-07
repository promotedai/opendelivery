add_executable(execution_drogon_tests simple_executor_tests.cc)
target_link_libraries(execution_drogon_tests PRIVATE drogon execution stages)
ParseAndAddDrogonTests(execution_drogon_tests)

add_executable(
  execution_tests
  configure_simple_executor_tests.cc feature_context_tests.cc)
target_link_libraries(
  execution_tests
  PRIVATE GTest::gtest_main GTest::gmock execution promoted_protos mock_clients absl::flat_hash_map)

include(GoogleTest)
gtest_discover_tests(execution_tests)
