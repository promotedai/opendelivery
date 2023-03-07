# This minimum was taken from the LLVM docs.
cmake_minimum_required(VERSION 3.13.4)
project(delivery-cpp CXX)

if ("${CMAKE_CXX_STANDARD}" STREQUAL "")
    set(CMAKE_CXX_STANDARD 17)
endif ()
message(STATUS "Using C++${CMAKE_CXX_STANDARD}")

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(INSTALL_GTEST OFF)
set(INSTALL_GMOCK OFF)

# Enabling project-based #include paths as per
# https://google.github.io/styleguide/cppguide.html#Names_and_Order_of_Includes
# But it's not strictly required (e.g. for submodules).
add_compile_options(-I${PROJECT_SOURCE_DIR})

if(ENABLE_ASAN)
    message(STATUS "ASan enabled")
    add_compile_options(-g -O1 -fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-g -O1 -fsanitize=address -fno-omit-frame-pointer)
endif()

if(ENABLE_TSAN)
    message(STATUS "TSan enabled")
    add_compile_options(
        -g -O1 -fsanitize=thread -fno-omit-frame-pointer -fPIC)
    add_link_options(
        -g -O1 -fsanitize=thread -fno-omit-frame-pointer -fPIC)
endif()

enable_testing()

include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip)
FetchContent_MakeAvailable(googletest)
FetchContent_Declare(
    abseil
    URL https://github.com/abseil/abseil-cpp/archive/8c0b94e793a66495e0b1f34a5eb26bd7dc672db0.zip)
FetchContent_MakeAvailable(abseil)
# This is technically owned by Google even though the URL doesn't look like it.
FetchContent_Declare(
    backward
    URL https://github.com/bombela/backward-cpp/archive/3bb9240cb15459768adb3e7d963a20e1523a6294.zip)
if(NOT backward_POPULATED)
    message(STATUS "Fetching 'backward'...")
    FetchContent_Populate(backward)
    add_subdirectory(${backward_SOURCE_DIR} ${backward_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

# submodules first to get Drogon testing in other directories.
add_subdirectory(submodules)
add_subdirectory(cloud)
add_subdirectory(config)
add_subdirectory(controllers)
add_subdirectory(execution)
add_subdirectory(singletons)
add_subdirectory(utils)

# Backward automatically gives us nice stack traces if we crash.
add_executable(main main.cc ${BACKWARD_ENABLE})
add_backward(main)
# libdw is needed to create detailed stack traces.
target_compile_definitions(main PRIVATE BACKWARD_HAS_DW=1)
target_link_libraries(
    main
    PRIVATE drogon controllers cloud singletons config dw)
