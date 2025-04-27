
include(FetchContent)
FetchContent_Declare(
  Thrift
  GIT_REPOSITORY https://github.com/apache/thrift.git
  GIT_TAG v0.21.0
)
FetchContent_MakeAvailable(Thrift)