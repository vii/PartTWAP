#include "partvwap.hh"
#include "partvwap_parquet.hh"
#include "partvwap_turbo.hh"
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

TEST(ParquetTurboIntegration, EndToEndTest) {
  TempDirectoryForTest test_dir;

  // Create test parquet files
  std::string cmd =
      "./create_test_parquet " + std::string(test_dir.tmp_dirname) + " 3";
  std::string cmd_output = RunCommandForTest(cmd.c_str());
  std::vector<std::string> parquet_files =
      FindAndSortParquetFiles(test_dir.tmp_dirname);
  ASSERT_EQ(parquet_files.size(), 3)
      << "Expected 3 parquet files, but found " << parquet_files.size();

  TempFileForTest turbo_file;
  TempFileForTest output_parquet_file;

  cmd = "./parquet_to_turbo --repeat_turbo_decode_duration=30s " +
        std::string(test_dir.tmp_dirname) + " " + turbo_file.tmp_filename +
        " " + output_parquet_file.tmp_filename;
  cmd_output = RunCommandForTest(cmd.c_str());
  std::cout << "Command output: " << cmd_output << std::endl;

  EXPECT_TRUE(std::filesystem::exists(turbo_file.tmp_filename));
  EXPECT_GT(std::filesystem::file_size(turbo_file.tmp_filename), 0);
  EXPECT_TRUE(std::filesystem::exists(output_parquet_file.tmp_filename));
  EXPECT_GT(std::filesystem::file_size(output_parquet_file.tmp_filename), 0);

  // Read all rows from the turbo file into a vector
  std::vector<InputRow> rows_from_turbo;
  ReadTurboPForFromInputRows(bitnunpack128v64, bitnxunpack256v32,
                             turbo_file.tmp_filename.c_str(),
                             [&rows_from_turbo](const InputRow &row) {
                               rows_from_turbo.push_back(row);
                             });

  NameToId providers;
  NameToId symbols;

  // read all rows from the input parquet files into a vector
  std::vector<InputRow> rows_from_parquet;
  arrow::Status read_status = ReadManyParquetFiles(
      parquet_files,
      [&rows_from_parquet](const InputRow &row) {
        rows_from_parquet.push_back(row);
      },
      providers, symbols);
  ASSERT_TRUE(read_status.ok())
      << "Error reading parquet files: " << read_status.ToString();

  std::cout << "Rows from turbo: " << rows_from_turbo.size() << std::endl;
  std::cout << "Rows from parquet: " << rows_from_parquet.size() << std::endl;

  // verify that the rows from the turbo file and the rows from the parquet
  // files are the same
  ASSERT_EQ(rows_from_turbo.size(), rows_from_parquet.size());
  for (size_t i = 0; i < rows_from_turbo.size(); ++i) {
    EXPECT_EQ(rows_from_turbo[i], rows_from_parquet[i])
        << "Row " << i << " is different";
  }
}