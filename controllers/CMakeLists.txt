# If this fails, make sure your machine has libprotobuf-dev.
include(FindProtobuf)
find_package(Protobuf REQUIRED)

# This target needs to publicize the .cc files for Drogon to find them while
# compiling. In general this leaks implementation details, but no one else
# should be depending on this target anyway.
add_library(controllers)
target_sources(controllers PUBLIC deliver.cc healthz.cc)
target_link_libraries(
    controllers
    PRIVATE absl::flat_hash_set
    PUBLIC drogon promoted_protos execution stages cloud singletons config)

add_subdirectory(tests)
