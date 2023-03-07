# These should only be depended on by:
# 1. Main
# 2. Controllers
#
# If you need this information in an execution, pass it in from a controller.

find_package(AWSSDK REQUIRED COMPONENTS s3 dynamodb personalize-runtime sqs monitoring)

add_library(singletons)
target_sources(
    singletons
    PRIVATE config.cc user_agent.cc counters.cc feature.cc paging.cc redis_client_array.cc
    PUBLIC singleton.h aws.h env.h config.h cache.h user_agent.h counters.h feature.h paging.h redis_client_array.h)
target_link_libraries(
    singletons
    PRIVATE drogon utils
    PUBLIC ${AWSSDK_LINK_LIBRARIES} absl::flat_hash_set absl::flat_hash_map config modern-cpp-kafka-api lru_cache stages promoted_protos
           stages uap_cpp cloud redis++)
target_include_directories(
    singletons
    PUBLIC ${gtest_SOURCE_DIR}/include)

add_subdirectory(tests)
