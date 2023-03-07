add_library(mock_clients)
target_sources(
  mock_clients
  PUBLIC mock_clients.h)
target_link_libraries(
  mock_clients
    PRIVATE stages
    PUBLIC GTest::gmock stages)

add_executable(
  stages_tests
  stage_tests.cc write_to_delivery_log_tests.cc paging_tests.cc init_tests.cc respond_tests.cc read_from_feature_store_tests.cc counters_tests.cc
  read_from_personalize_tests.cc init_features_tests.cc flatten_tests.cc exclude_user_features_tests.cc
  compute_time_features_tests.cc compute_distribution_features_tests.cc write_out_stranger_features_tests.cc compute_query_features_tests.cc
  compute_ratio_features_tests.cc read_from_request_tests.cc write_to_monitoring_tests.cc)
target_link_libraries(
  stages_tests
  PRIVATE GTest::gtest_main GTest::gmock stages execution promoted_protos mock_clients hash_utils absl::flat_hash_map)

include(GoogleTest)
gtest_discover_tests(stages_tests)
