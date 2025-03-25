cmake_minimum_required(VERSION 3.26)
project(partvwap)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -ggdb")
set(CMAKE_CXX_FLAGS_DEBUG "-ggdb -fsanitize=address -fno-omit-frame-pointer")
# Set linker flags for ASan in debug mode
set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} -fsanitize=address")


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
include(ExternalProject)

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
        BUILD_BYPRODUCTS ${ARROW_INSTALL_DIR}/lib64/cmake/Arrow/ArrowConfig.cmake ${ARROW_INSTALL_DIR}/lib64/cmake/Arrow/ArrowTargets.cmake ${ARROW_INSTALL_DIR}/lib64/cmake/Parquet/ParquetConfig.cmake 
)
# Fetch and configure TurboPFor
FetchContent_Declare(
    TurboPFor
    GIT_REPOSITORY https://github.com/powturbo/TurboPFor-Integer-Compression.git
    GIT_TAG master
)

# Set TurboPFor build options
set(TURBOPFOR_BUILD_STATIC ON CACHE BOOL "Build static TurboPFor library")
set(TURBOPFOR_BUILD_SHARED OFF CACHE BOOL "Build shared TurboPFor library")

FetchContent_MakeAvailable(TurboPFor)

# Create an interface library for TurboPFor
add_library(turbopfor_interface INTERFACE)
FetchContent_GetProperties(TurboPFor SOURCE_DIR TURBOPFOR_SOURCE_DIR)

add_custom_command(
    OUTPUT ${TURBOPFOR_SOURCE_DIR}/libic.a
    COMMAND make OPT="-fpermissive" libic.a -j 8
    WORKING_DIRECTORY ${TURBOPFOR_SOURCE_DIR}
    COMMENT "Building TurboPFor libic.a"
)

add_custom_target(build_ic ALL DEPENDS ${TURBOPFOR_SOURCE_DIR}/libic.a)

add_dependencies(turbopfor_interface build_ic)
target_include_directories(turbopfor_interface INTERFACE ${TURBOPFOR_SOURCE_DIR})
target_link_libraries(turbopfor_interface INTERFACE ${TURBOPFOR_SOURCE_DIR}/libic.a rt m)



set(Arrow_DIR ${ARROW_INSTALL_DIR}/lib64/cmake/Arrow)
set(Parquet_DIR ${ARROW_INSTALL_DIR}/lib64/cmake/Parquet)

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
target_link_libraries(partvwap_test
    GTest::gtest_main
    GTest::gmock_main
    absl::flat_hash_map
    absl::strings
    absl::cleanup
    absl::status
    absl::time
    benchmark::benchmark
)




add_executable(parquet_test parquet_test.cc)
add_dependencies(parquet_test arrow_project)
target_include_directories(parquet_test PRIVATE ${ARROW_INSTALL_DIR}/include)
target_link_directories(parquet_test PRIVATE ${ARROW_INSTALL_DIR}/lib64)
target_link_libraries(parquet_test
    GTest::gtest_main
    GTest::gmock_main
    absl::flat_hash_map
    absl::strings
    absl::cleanup
    absl::status
    absl::time
    benchmark::benchmark
    $<$<BOOL:${Arrow_FOUND}>:Arrow::arrow_static>
    $<$<BOOL:${Parquet_FOUND}>:Parquet::parquet_static>
)

target_compile_definitions(parquet_test PRIVATE 
    ARROW_DEPRECATION_WARNINGS=0
)


add_executable(turbo_test turbo_test.cc)
add_dependencies(turbo_test turbopfor_interface)
target_include_directories(turbo_test PRIVATE ${TURBOPFOR_SOURCE_DIR}/include)
target_link_libraries(turbo_test
    GTest::gtest_main
    GTest::gmock_main
    absl::flat_hash_map
    absl::strings
    absl::cleanup
    absl::status
    absl::time
    benchmark::benchmark
    turbopfor_interface
)


enable_testing()


add_test(NAME partvwap_test COMMAND partvwap_test)
add_test(NAME parquet_test COMMAND parquet_test)
add_test(NAME turbo_test COMMAND turbo_test)