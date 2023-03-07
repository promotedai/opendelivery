find_package(AWSSDK REQUIRED COMPONENTS s3 dynamodb personalize-runtime sqs monitoring)

add_library(cloud)
target_sources(
    cloud
    PRIVATE kafka_delivery_log_writer.cc dynamodb_feature_store_reader.cc
            aws_personalize_client.cc aws_sqs_client.cc cloudwatch_monitoring_client.cc sw_redis_client.cc
    PUBLIC kafka_delivery_log_writer.h dynamodb_feature_store_reader.h
           aws_personalize_client.h aws_sqs_client.h cloudwatch_monitoring_client.h sw_redis_client.h)
target_link_libraries(
    cloud
    PRIVATE ${PROTOBUF_LIBRARIES} absl::strings absl::flat_hash_map utils redis++
    PUBLIC ${AWSSDK_LINK_LIBRARIES} stages promoted_protos modern-cpp-kafka-api drogon)

add_subdirectory(tests)
