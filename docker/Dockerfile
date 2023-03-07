FROM ubuntu:22.04 AS base
WORKDIR /
# Copy only the necessary part of the repo for better caching.
COPY /submodules/aws-sdk-cpp/ src/submodules/aws-sdk-cpp/
COPY /submodules/hiredis/ src/submodules/hiredis/
RUN export DEBIAN_FRONTEND=noninteractive && \
    apt update && \
    apt-get install -y \
        wget \
        lsb-release \
        software-properties-common \
        cmake \
        make \
        python3 \
        clang-tidy \
        # We use Clang. This is just to simplify Protobuf installation.
        g++ \
        # For building the AWS SDK.
        libssl-dev \
        libcurl4-openssl-dev \
        # For Kafka.
        librdkafka-dev \
        # For thread-safe-lru.
        libtbb-dev \
        # For uap-cpp.
        libyaml-cpp-dev \
        # For nice stack traces.
        libdw-dev \
        # Delivery dependencies.
        libjsoncpp-dev \
        uuid-dev \
        zlib1g-dev \
        tzdata \
        git \
        libuv1-dev \
        && \
    # This specific version of Protobuf is needed to work with our schema repo.
    wget https://github.com/protocolbuffers/protobuf/releases/download/v3.15.3/protobuf-cpp-3.15.3.tar.gz && \
    tar -xzf protobuf-cpp-3.15.3.tar.gz && \
    cd protobuf-3.15.3 && \
    ./configure && \
    make -j $(nproc) && make install && \
    cd - && \
    wget -O - https://storage.googleapis.com/tensorflow/libtensorflow/libtensorflow-cpu-linux-x86_64-2.11.0.tar.gz | \
    tar -C /usr/local -xz && \
    ldconfig && \
    wget https://apt.llvm.org/llvm.sh && \
    chmod +x llvm.sh && \
    ./llvm.sh 15 && \
    apt-get install -y libomp-15-dev && \
    rm -rf /var/lib/apt/lists/* && \
    cmake -S src/submodules/aws-sdk-cpp/ -B build-aws -G "Unix Makefiles"             \
        -DCMAKE_BUILD_TYPE=RELEASE                                                    \
        -DCMAKE_CXX_STANDARD=17                                                       \
        -DCMAKE_C_COMPILER:FILEPATH=/usr/bin/clang-15                                 \
        -DCMAKE_CXX_COMPILER:FILEPATH=/usr/bin/clang++-15                             \
        -DBUILD_ONLY="s3;dynamodb;personalize-runtime;sqs;monitoring"                 \
        -DENABLE_TESTING=OFF                                                          \
        && \
    cd build-aws && \
    make -j $(nproc) && make install && \
    cd - && \
    # For async redis-plus-plus, we need a newer version of hiredis than is
    # available via apt-get.
    cmake -S src/submodules/hiredis/ -B build-hiredis -G "Unix Makefiles"             \
        -DCMAKE_BUILD_TYPE=RELEASE                                                    \
        -DCMAKE_C_COMPILER:FILEPATH=/usr/bin/clang-15                                 \
        -DDISABLE_TESTS=ON                                                            \
        && \
    cd build-hiredis && \
    make -j $(nproc) && make install && \
    cd - && \
    git clone -b v1.5.2 --recursive https://github.com/dmlc/xgboost /tmp/xgboost-1.5.2 && \
    mkdir /tmp/xgboost-1.5.2/build && cd /tmp/xgboost-1.5.2/build && \
    cmake .. \
        -DBUILD_STATIC_LIB=ON                              \
        -DCMAKE_C_COMPILER:FILEPATH=/usr/bin/clang-15      \
        -DCMAKE_CXX_COMPILER:FILEPATH=/usr/bin/clang++-15  \
        && \
    make -j$(nproc) && make install && cd - && rm -rf /tmp/xgboost-1.5.2
# Now copy the whole repo.
COPY / src/
    
FROM base AS tester
WORKDIR /
RUN chmod +x src/docker/tester_entrypoint.sh
ENTRYPOINT ["src/docker/tester_entrypoint.sh"]

FROM base AS builder
WORKDIR /
RUN cmake -S src/ -B build -G "Unix Makefiles"             \
        # Debug info is necessary for helpful stack traces when crashing.
        # This does not negatively impact performance.
        -DCMAKE_BUILD_TYPE=RELWITHDEBINFO                  \
        -DCMAKE_CXX_STANDARD=17                            \
        -DCMAKE_C_COMPILER:FILEPATH=/usr/bin/clang-15      \
        -DCMAKE_CXX_COMPILER:FILEPATH=/usr/bin/clang++-15  \
        && \
    cd build && \
    make -j $(nproc) main && \
    # Give everything an absolute path to simplify copying in the next stage.
    mkdir /hold && \
    cp main /hold/main && \
    # This copies all shared libraries that the binary depends on. This will
    # let us shrink our image 95%. A static build would get us even lower,
    # but has its own risks (e.g. https://stackoverflow.com/a/57478728). That
    # said, containers mitigate these risks so we can re-evaluate later.
    ldd main | grep "=> /" | awk '{print $3}' | xargs -I '{}' cp -v '{}' /hold

# Note that this image will not inline code segments into the stack trace
# because we don't copy over the source files. It's feasible but annoying
# to do so without also copying over the AWS SDK and ballooning the image
# size. It will still have file names and line numbers though.
FROM ubuntu:22.04 AS minimal
WORKDIR /
# This doesn't get linked in but is still necessary for the AWS SDK.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
# This doesn't get linked in but is still necessary for how we parse user agents.
COPY --from=builder /src/singletons/user_agent.yaml /src/singletons/user_agent.yaml
# This doesn't get linked in but is still necessary for timezone logic for time features.
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /hold /hold
# This allows the executable to find the shared libs it depends on.
ENV LD_LIBRARY_PATH=/hold
CMD ["./hold/main"]
