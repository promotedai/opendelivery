add_executable(
    cloud_tests
    kafka_delivery_log_writer_tests.cc dynamodb_feature_store_reader_tests.cc)
target_link_libraries(cloud_tests GTest::gtest_main GTest::gmock cloud)

include(GoogleTest)
gtest_discover_tests(cloud_tests)
