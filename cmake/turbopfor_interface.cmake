include(FetchContent)

FetchContent_Declare(
    TurboPFor
    GIT_REPOSITORY https://github.com/powturbo/TurboPFor-Integer-Compression.git
    GIT_TAG master
)

FetchContent_MakeAvailable(TurboPFor)

# Create an interface library for TurboPFor
add_library(turbopfor_interface INTERFACE)
FetchContent_GetProperties(TurboPFor SOURCE_DIR TURBOPFOR_SOURCE_DIR)

add_custom_command(
    OUTPUT ${TURBOPFOR_SOURCE_DIR}/libic.a
    COMMAND make AVX2=1 STATIC=1 OPT="-fpermissive" CC="${CMAKE_C_COMPILER}" CXX="${CMAKE_CXX_COMPILER}" libic.a -j8
    WORKING_DIRECTORY ${TURBOPFOR_SOURCE_DIR}
    COMMENT "Building TurboPFor libic.a"
)

add_custom_target(build_turbopfor_interface ALL DEPENDS ${TURBOPFOR_SOURCE_DIR}/libic.a)

add_dependencies(turbopfor_interface build_turbopfor_interface)
target_include_directories(turbopfor_interface INTERFACE ${TURBOPFOR_SOURCE_DIR})
target_link_libraries(turbopfor_interface INTERFACE ${TURBOPFOR_SOURCE_DIR}/libic.a rt m)
