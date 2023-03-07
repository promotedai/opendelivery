add_library(utils)
target_sources(
    utils
    PRIVATE uuid.cc time.cc network.cc geo.cc
    PUBLIC uuid.h time.h network.h math.h geo.h)
target_link_libraries(
    utils
    PRIVATE drogon absl::strings)

add_subdirectory(tests)
