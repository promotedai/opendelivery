add_subdirectory(stages)

add_library(execution)
target_sources(
    execution
    PRIVATE simple_executor.cc feature_context.cc
    PUBLIC context.h executor.h simple_executor.h paging_context.h counters_context.h user_agent.h feature_context.h merge_maps.h)
target_link_libraries(
    execution
    PRIVATE drogon absl::strings utils
    PUBLIC promoted_protos stages config absl::flat_hash_map)

add_subdirectory(tests)
