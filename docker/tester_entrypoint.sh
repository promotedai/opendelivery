#!/bin/bash

# Support presence of this folder as the path is different
# in devcontainer for VS Code
[[ -d src ]] && cd src

# Terminate on bad exits.
set -e

cmake -S . -B build -G "Unix Makefiles"                \
    -DCMAKE_BUILD_TYPE=DEBUG                           \
    -DBUILD_TESTING=on                                 \
    -DCMAKE_CXX_STANDARD=17                            \
    -DCMAKE_C_COMPILER:FILEPATH=/usr/bin/clang-15      \
    -DCMAKE_CXX_COMPILER:FILEPATH=/usr/bin/clang++-15  \
    -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=TRUE          \
    $1

if [[ $2 = on ]]; then
    python3 run-clang-tidy.py
fi

if [[ $3 = on ]]; then
    cd build
    make -j $(nproc) && make install
    ctest -j $(nproc) --output-on-failure
    cd -
fi
