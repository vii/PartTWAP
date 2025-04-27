cmake_minimum_required(VERSION 3.26)
project(partvwap)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")


set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -ggdb")
set(CMAKE_CXX_FLAGS_DEBUG "-ggdb -fsanitize=address -fno-omit-frame-pointer")
set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} -fsanitize=address")

include(arrow_interface)
include(turbopfor_interface)

find_package(Arrow QUIET)
find_package(Parquet QUIET)

include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)
FetchContent_MakeAvailable(googletest)

FetchContent_Declare(
    benchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG v1.8.3
)
set(BENCHMARK_ENABLE_TESTING OFF)
FetchContent_MakeAvailable(benchmark)

FetchContent_Declare(
    absl
    GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
    GIT_TAG "20250127.0"
)
FetchContent_MakeAvailable(absl)

add_executable(partvwap_test partvwap_test.cc)
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


if (PARQUET_FOUND) # this will fail the first time because the arrow and parquet libraries are not built, then succeeds the next time
    add_executable(partvwap_parquet_test partvwap_parquet_test.cc partvwap_parquet.cc)
    add_dependencies(partvwap_parquet_test arrow_project)
    target_include_directories(partvwap_parquet_test PRIVATE ${ARROW_INSTALL_DIR}/include)
    target_link_directories(partvwap_parquet_test PRIVATE ${ARROW_INSTALL_DIR}/lib64)
    target_link_libraries(partvwap_parquet_test
        GTest::gtest_main
        GTest::gmock_main
        absl::flat_hash_map
        absl::strings
        absl::cleanup
        absl::status
        absl::time
        benchmark::benchmark
        Parquet::parquet_static
        Arrow::arrow_static
    )

    target_compile_definitions(partvwap_parquet_test PRIVATE 
        ARROW_DEPRECATION_WARNINGS=0
    )
endif()

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
add_test(NAME partvwap_parquet_test COMMAND partvwap_parquet_test)
add_test(NAME turbo_test COMMAND turbo_test)