cmake_minimum_required(VERSION 3.26)
project(partvwap)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")


set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -ggdb -march=native")
set(CMAKE_CXX_FLAGS_DEBUG "-ggdb -fsanitize=address -fno-omit-frame-pointer")
set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} -fsanitize=address")

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

enable_testing()


if (Parquet_FOUND) # this will fail the first time because the arrow and parquet libraries are not built, then succeeds the next time
    add_executable(partvwap_parquet_test partvwap_parquet_test.cc partvwap_parquet.cc)
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
        Arrow::arrow_static
        Parquet::parquet_static
    )

    target_compile_definitions(partvwap_parquet_test PRIVATE
        ARROW_DEPRECATION_WARNINGS=0
    )

    add_executable(partvwap_parquet_io partvwap_parquet_io.cc partvwap_parquet.cc)
    target_include_directories(partvwap_parquet_io PRIVATE ${ARROW_INSTALL_DIR}/include)
    target_link_directories(partvwap_parquet_io PRIVATE ${ARROW_INSTALL_DIR}/lib64)
    target_link_libraries(partvwap_parquet_io
        absl::flat_hash_map
        absl::strings
        absl::cleanup
        absl::status
        absl::time
        Arrow::arrow_static
        Parquet::parquet_static
        absl::flags
        absl::flags_parse
        absl::flags_usage
    )

    add_executable(create_test_parquet create_test_parquet.cc partvwap_parquet.cc)
    target_include_directories(create_test_parquet PRIVATE ${ARROW_INSTALL_DIR}/include)
    target_link_directories(create_test_parquet PRIVATE ${ARROW_INSTALL_DIR}/lib64)
    target_link_libraries(create_test_parquet
        absl::flat_hash_map
        absl::strings
        absl::cleanup
        absl::status
        absl::time
        Arrow::arrow_static
        Parquet::parquet_static
    )

    target_compile_definitions(create_test_parquet PRIVATE
        ARROW_DEPRECATION_WARNINGS=0
    )

    add_executable(partvwap_parquet_integration_test
        partvwap_parquet_integration_test.cc
        run_command_for_test.cc
    )
    target_include_directories(partvwap_parquet_integration_test PRIVATE ${ARROW_INSTALL_DIR}/include)
    target_link_directories(partvwap_parquet_integration_test PRIVATE ${ARROW_INSTALL_DIR}/lib64)
    target_link_libraries(partvwap_parquet_integration_test
        GTest::gtest_main
        GTest::gmock_main
        absl::flat_hash_map
        absl::strings
        absl::cleanup
        absl::status
        absl::time
        Arrow::arrow_static
        Parquet::parquet_static
    )

    target_compile_definitions(partvwap_parquet_integration_test PRIVATE
        ARROW_DEPRECATION_WARNINGS=0
    )



    add_executable(parquet_to_turbo parquet_to_turbo.cc partvwap_parquet.cc)
    target_include_directories(parquet_to_turbo PRIVATE ${TURBOPFOR_SOURCE_DIR}/include)
    target_include_directories(parquet_to_turbo PRIVATE ${ARROW_INSTALL_DIR}/include)
    target_link_directories(parquet_to_turbo PRIVATE ${ARROW_INSTALL_DIR}/lib64)
    target_link_libraries(parquet_to_turbo
        turbopfor_interface
        Arrow::arrow_static
        Parquet::parquet_static
        absl::flat_hash_map
        absl::strings
        absl::cleanup
        absl::status
        absl::time
        absl::flags
        absl::flags_parse
        absl::flags_usage
    )

    add_executable(parquet_to_turbo_integration_test
        parquet_to_turbo_integration_test.cc
        run_command_for_test.cc
        partvwap_parquet.cc
    )
    target_include_directories(parquet_to_turbo_integration_test PRIVATE ${TURBOPFOR_SOURCE_DIR}/include)
    target_include_directories(parquet_to_turbo_integration_test PRIVATE ${ARROW_INSTALL_DIR}/include)
    target_link_directories(parquet_to_turbo_integration_test PRIVATE ${ARROW_INSTALL_DIR}/lib64)
    target_link_libraries(parquet_to_turbo_integration_test
        turbopfor_interface
        GTest::gtest_main
        GTest::gmock_main
        absl::flat_hash_map
        absl::strings
        absl::cleanup
        absl::status
        absl::time
        Arrow::arrow_static
        Parquet::parquet_static
    )

    target_compile_definitions(parquet_to_turbo_integration_test PRIVATE
        ARROW_DEPRECATION_WARNINGS=0
    )
    add_test(NAME parquet_to_turbo_integration_test COMMAND parquet_to_turbo_integration_test)


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

add_executable(perf_counter_scope_test perrf_counter_scope_test.cc)
target_link_libraries(perf_counter_scope_test
    GTest::gtest_main
    GTest::gmock_main
    absl::flat_hash_map
    absl::strings
    absl::cleanup
    absl::status
    absl::time
    benchmark::benchmark
)

add_test(NAME partvwap_test COMMAND partvwap_test)
add_test(NAME partvwap_parquet_test COMMAND partvwap_parquet_test)
add_test(NAME partvwap_parquet_integration_test COMMAND partvwap_parquet_integration_test)
add_test(NAME turbo_test COMMAND turbo_test)
add_test(NAME perf_counter_scope_test COMMAND perf_counter_scope_test)