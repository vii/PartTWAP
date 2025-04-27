FROM ubuntu:rolling 

RUN apt-get update
RUN apt-get install -y libboost-all-dev libthrift-dev cmake \
    build-essential \
    git \
    wget \
    python3 \
    python3-pip \
    python3-dev \
    libssl-dev \
    libgflags-dev \
    libgoogle-glog-dev \
    libcurl4-openssl-dev \
    libbz2-dev \
    liblz4-dev \
    libzstd-dev \
    libsnappy-dev \
    libprotobuf-dev \
    protobuf-compiler \
    libgtest-dev \
    libgrpc-dev \
    libgrpc++-dev \
    protobuf-compiler-grpc \
    libre2-dev

# Clone and build Arrow
WORKDIR /tmp
RUN git clone https://github.com/apache/arrow.git && \
    cd arrow/cpp && \
    mkdir build && \
    cd build && \
    cmake .. \
        -DARROW_BUILD_STATIC=ON \
        -DARROW_BUILD_SHARED=OFF \
        -DARROW_BUILD_TESTS=OFF \
        -DARROW_PARQUET=ON \
        -DCMAKE_BUILD_TYPE=Release && \
    make -j$(nproc) && \
    make install && \
    ldconfig && \
    cd /tmp && \
    rm -rf arrow

