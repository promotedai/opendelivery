set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)
set(ORIGINAL_BUILD_TESTING BUILD_TESTING)
set(BUILD_TESTING OFF)
set(BUILD_CTL OFF)
set(BUILD_EXAMPLES OFF)
set(BUILD_POSTGRESQL OFF)
set(BUILD_MYSQL OFF)
set(BUILD_SQLITE OFF)
add_subdirectory(drogon EXCLUDE_FROM_ALL)
set(BUILD_TESTING ORIGINAL_BUILD_TESTING)
unset(BUILD_CTL)
unset(BUILD_EXAMPLES)
unset(BUILD_POSTGRESQL)
unset(BUILD_MYSQL)
unset(BUILD_SQLITE)

# Keep this before hashlib-cpp (or make this conditional) to avoid redundant
# targets. A recursive update will still cause Git to do a redundant download
# but I haven't found a good way around this.
add_subdirectory(schema-internal)
set(ORIGINAL_BUILD_TESTS BUILD_TESTS)
set(BUILD_TESTS OFF)
add_subdirectory(hashlib-cpp EXCLUDE_FROM_ALL)
set(BUILD_TESTS ORIGINAL_BUILD_TESTS)

# The AWS SDK is not intended to be used as a subdirectory.
# Instead it's made, installed, and used as a package.

find_package(TBB REQUIRED)
# We can trust thread-safe-lru because it's just a smaller wrapper around
# https://github.com/oneapi-src/oneTBB.
file(GLOB_RECURSE LRU_CACHE_HDRS thread-safe-lru/thread-safe-lru/*.h)
add_library(lru_cache ${LRU_CACHE_HDRS})
set_target_properties(lru_cache PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(lru_cache PUBLIC TBB::tbb)
target_include_directories(lru_cache PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/thread-safe-lru)

# https://github.com/edenhill/librdkafka is the canonical C++ Kafka repo, but
# it strives to maintain C++98 compatibility. modern-cpp-kafka is a wrapper
# that supports C++17 concepts. It's owned by Morgan Stanley, so it's
# probably safe.
add_subdirectory(modern-cpp-kafka/include EXCLUDE_FROM_ALL)

# re2 and yaml-cpp are required by uap-cpp below.
include(FetchContent)
FetchContent_Declare(
    re2
    URL https://github.com/google/re2/archive/4be240789d5b322df9f02b7e19c8651f3ccbf205.zip)
set(RE2_BUILD_TESTING OFF)
FetchContent_GetProperties(re2)
if(NOT re2_POPULATED)
  message(STATUS "Fetching 're2'...")
  FetchContent_Populate(re2)
  add_subdirectory(${re2_SOURCE_DIR} ${re2_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

find_package(yaml-cpp REQUIRED)
# uap-cpp is the C++ version of the repo used for user agent parsing by go-delivery.
# Their CMake brings along baggage we don't want, so we just hand-roll our own target here.
add_library(uap_cpp)
target_sources(
    uap_cpp
    PRIVATE uap-cpp/UaParser.cpp uap-cpp/internal/AlternativeExpander.cpp
            uap-cpp/internal/Pattern.cpp uap-cpp/internal/ReplaceTemplate.cpp
            uap-cpp/internal/SnippetIndex.cpp
    PUBLIC uap-cpp/UaParser.h)
target_link_libraries(
    uap_cpp
    PRIVATE re2 yaml-cpp)
target_include_directories(
    uap_cpp
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/uap-cpp)

# This is being trusted as it's the most popular C++ Redis client.
# It's recommended by the official Redis site.
set(REDIS_PLUS_PLUS_BUILD_ASYNC "libuv")
set(REDIS_PLUS_PLUS_BUILD_TEST OFF)
add_subdirectory(redis-plus-plus)
