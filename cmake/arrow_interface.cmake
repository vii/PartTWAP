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


set(Arrow_DIR ${ARROW_INSTALL_DIR}/lib64/cmake/Arrow)
set(Parquet_DIR ${ARROW_INSTALL_DIR}/lib64/cmake/Parquet)

find_package(Arrow QUIET)
find_package(Parquet QUIET)

