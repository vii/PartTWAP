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
        -DCMAKE_CXX_STANDARD=${CMAKE_CXX_STANDARD}
        -DCMAKE_CXX_STANDARD_REQUIRED=${CMAKE_CXX_STANDARD_REQUIRED}
        -DCMAKE_CXX_EXTENSIONS=${CMAKE_CXX_EXTENSIONS}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        BUILD_COMMAND ${CMAKE_COMMAND} --build .
        INSTALL_COMMAND ${CMAKE_COMMAND} --install . --prefix ${ARROW_INSTALL_DIR}
        UPDATE_DISCONNECTED 1
        LOG_DOWNLOAD ON
        LOG_CONFIGURE ON
        LOG_BUILD ON
        LOG_INSTALL ON
        BUILD_BYPRODUCTS ${ARROW_INSTALL_DIR}/lib64/cmake/Arrow/ArrowConfig.cmake ${ARROW_INSTALL_DIR}/lib64/cmake/Arrow/ArrowTargets.cmake ${ARROW_INSTALL_DIR}/lib64/cmake/Parquet/ParquetConfig.cmake 
)


set(Arrow_DIR ${ARROW_INSTALL_DIR}/lib64/cmake/Arrow)
set(Parquet_DIR ${ARROW_INSTALL_DIR}/lib64/cmake/Parquet)

