cmake_minimum_required(VERSION 3.26)
project(partvwap)

set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "-O3 -ggdb")


# Include FetchContent for downloading dependencies
include(FetchContent)
# Fetch and configure GoogleTest
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)
FetchContent_MakeAvailable(googletest)

# Fetch and configure Google Benchmark
FetchContent_Declare(
    benchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG v1.8.3
)
set(BENCHMARK_ENABLE_TESTING OFF)
FetchContent_MakeAvailable(benchmark)
# Fetch and configure Apache Arrow
FetchContent_Declare(
    arrow
    GIT_REPOSITORY https://github.com/apache/arrow.git
    GIT_TAG apache-arrow-19.0.1
    SOURCE_SUBDIR cpp
)
set(ARROW_BUILD_STATIC ON)
set(ARROW_BUILD_SHARED OFF) 
set(ARROW_BUILD_TESTS OFF)
set(ARROW_WITH_SNAPPY ON)
set(ARROW_WITH_ZLIB ON)
set(ARROW_WITH_ZSTD ON)
set(ARROW_WITH_LZ4 ON)
set(ARROW_WITH_BROTLI ON)
set(ARROW_PARQUET ON)
FetchContent_MakeAvailable(arrow)


# Fetch and configure Abseil
FetchContent_Declare(
    absl
    GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
    GIT_TAG "20250127.0"
)
FetchContent_MakeAvailable(absl)


# Add partvwap test executable
add_executable(partvwap_test partvwap.cc)
target_link_libraries(partvwap_test
    GTest::gtest_main
    GTest::gmock_main
    absl::flat_hash_map
    absl::strings
    benchmark::benchmark
)
target_compile_features(partvwap_test PRIVATE cxx_std_20)

enable_testing()
add_test(NAME partvwap_test COMMAND partvwap_test)
