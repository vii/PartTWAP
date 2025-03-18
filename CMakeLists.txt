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


set(ARROW_INSTALL_DIR ${CMAKE_BINARY_DIR}/arrow-install)
set(EP_LOG_LEVEL DEBUG)
set(CMAKE_VERBOSE_MAKEFILE ON)
# Clone and build Apache Arrow
include(FetchContent)

FetchContent_Declare(
    arrow
    GIT_REPOSITORY https://github.com/apache/arrow.git
    GIT_TAG main
    SOURCE_SUBDIR cpp
)

set(ARROW_BUILD_STATIC ON)
set(ARROW_BUILD_SHARED OFF) 
set(ARROW_BUILD_TESTS OFF)
set(ARROW_PARQUET ON)

FetchContent_MakeAvailable(arrow)

/*
ExternalProject_Add(arrow_project
        GIT_REPOSITORY "https://github.com/apache/arrow.git"
        GIT_TAG "main"  # Change this to the specific version you want
        SOURCE_DIR ${CMAKE_BINARY_DIR}/arrow-src
        BINARY_DIR ${CMAKE_BINARY_DIR}/arrow-build
        SOURCE_SUBDIR "cpp"
        CMAKE_ARGS
        -DARROW_BUILD_STATIC=ON
        -DARROW_BUILD_SHARED=OFF
        -DARROW_BUILD_TESTS=OFF
        -DARROW_PARQUET=ON
        -DCMAKE_INSTALL_PREFIX=${ARROW_INSTALL_DIR}
        -DCMAKE_INSTALL_LIBDIR=lib64
        -DCMAKE_INSTALL_CMAKEDIR=lib64/cmake
        BUILD_COMMAND cmake --build .
        INSTALL_COMMAND cmake --install . --prefix ${ARROW_INSTALL_DIR}
        UPDATE_DISCONNECTED 1
        LOG_DOWNLOAD ON
        LOG_CONFIGURE ON
        LOG_BUILD ON
        LOG_INSTALL ON
)

ExternalProject_Get_Property(arrow_project install_dir)
*/

set(Arrow_DIR ${install_dir}/lib64/cmake/Arrow)
set(Parquet_DIR ${install_dir}/lib64/cmake/Parquet)

# Find Arrow and Parquet packages
find_package(Arrow QUIET)
find_package(Parquet QUIET)

# Fetch and configure Abseil
FetchContent_Declare(
    absl
    GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
    GIT_TAG "20250127.0"
)
FetchContent_MakeAvailable(absl)


# Add partvwap test executable
add_executable(partvwap_test partvwap.cc)
add_dependencies(partvwap_test arrow_project)
target_include_directories(partvwap_test PRIVATE ${ARROW_INSTALL_DIR}/include)
target_link_directories(partvwap_test PRIVATE ${ARROW_INSTALL_DIR}/lib64)
target_link_libraries(partvwap_test
    GTest::gtest_main
    GTest::gmock_main
    absl::flat_hash_map
    absl::strings
    absl::cleanup
    absl::time
    benchmark::benchmark
    Arrow::arrow_static
    Parquet::parquet_static
)
target_compile_features(partvwap_test PRIVATE cxx_std_20)
target_compile_definitions(partvwap_test PRIVATE 
    ARROW_DEPRECATION_WARNINGS=0
)

enable_testing()
add_test(NAME partvwap_test COMMAND partvwap_test)
