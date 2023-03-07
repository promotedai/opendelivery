find_package(jsoncpp REQUIRED)

add_library(config)
target_sources(
    config
    PRIVATE platform_config.cc
    PUBLIC platform_config.h json.h paging_config.h execution_config.h feature_store_config.h counters_config.h personalize_config.h
           feature_config.h)
target_link_libraries(
    config
    PUBLIC jsoncpp_lib)

add_subdirectory(tests)

