# This gets the latest commit hash for server versioning.
execute_process(
    COMMAND git rev-parse HEAD
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_COMMIT_HASH
    OUTPUT_STRIP_TRAILING_WHITESPACE)

add_library(stages)
target_sources(
    stages
    PRIVATE write_to_delivery_log.cc paging.cc respond.cc init.cc read_from_feature_store.cc counters.cc read_from_personalize.cc
            init_features.cc flatten.cc exclude_user_features.cc compute_time_features.cc compute_distribution_features.cc
            write_out_stranger_features.cc compute_query_features.cc compute_ratio_features.cc read_from_request.cc
            write_to_monitoring.cc
    PUBLIC write_to_delivery_log.h stage.h paging.h redis_client.h respond.h init.h feature_store_client.h read_from_feature_store.h cache.h counters.h
           personalize_client.h read_from_personalize.h init_features.h flatten.h exclude_user_features.h compute_time_features.h
           compute_distribution_features.h sqs_client.h write_out_stranger_features.h compute_query_features.h compute_ratio_features.h
           read_from_request.h monitoring_client.h write_to_monitoring.h)
# date-tz is from the hashlib submodule.
target_link_libraries(
    stages
    PRIVATE promoted_protos ${PROTOBUF_LIBRARIES} execution utils hash_utils absl::strings absl::flat_hash_set utils date::date-tz
    PUBLIC ${PROTOBUF_LIBRARIES} config lru_cache absl::flat_hash_map)
target_compile_definitions(stages PUBLIC GIT_COMMIT_HASH="${GIT_COMMIT_HASH}")

add_subdirectory(tests)
