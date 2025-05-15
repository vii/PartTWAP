#include "partvwap.hh"
#include "partvwap_parquet.hh"
#include "run_command_for_test.hh"
#include "temp_file_for_test.hh"
#include <absl/cleanup/cleanup.h>
#include <absl/container/flat_hash_map.h>
#include <absl/status/status.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/string_view.h>
#include <absl/time/time.h>
#include <array>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/testing/gtest_util.h>
#include <cstdlib>
#include <filesystem>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iostream>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <string>
#include <sys/wait.h>
#include <vector>

TEST(ParquetIOIntegration, EndToEndTest) {
  TempDirectoryForTest test_dir;

  std::string cmd =
      "./create_test_parquet " + std::string(test_dir.tmp_dirname) + " 3";
  std::string cmd_output = RunCommandForTest(cmd.c_str());

  int count = 0;
  for (const auto &entry :
       std::filesystem::directory_iterator(test_dir.tmp_dirname)) {
    std::cout << "Found file: " << entry.path() << std::endl;
    count++;
  }
  ASSERT_EQ(count, 3) << "Expected 3 parquet files, but found " << count;

  TempFileForTest parquet_twap_file;

  cmd = "./partvwap_parquet_io " + std::string(test_dir.tmp_dirname) + " " +
        parquet_twap_file.tmp_filename;
  cmd_output = RunCommandForTest(cmd.c_str());
  std::cout << "Command output: " << cmd_output << std::endl;

  ASSERT_TRUE(std::filesystem::exists(parquet_twap_file.tmp_filename));
  ASSERT_GT(std::filesystem::file_size(parquet_twap_file.tmp_filename), 0);
}