#include "partvwap.hh"
#include "partvwap_parquet.hh"
#include <absl/container/flat_hash_map.h>
#include <absl/status/status.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/string_view.h>
#include <absl/time/time.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <filesystem>
#include <iostream>
#include <vector>

// Declare the main function that will be used in integration tests
int create_test_parquet_main(int argc, char **argv) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <output_dir> <num_files>"
              << std::endl;
    std::cerr << "This program creates test parquet files in <output_dir> with "
                 "sample price data."
              << std::endl;
    return 1;
  }

  std::string output_dir = argv[1];
  int64_t num_files;
  if (!absl::SimpleAtoi(argv[2], &num_files)) {
    std::cerr << "Error: Invalid number of files: " << argv[2] << std::endl;
    return 1;
  }

  if (!std::filesystem::exists(output_dir)) {
    std::filesystem::create_directories(output_dir);
  }

  if (!std::filesystem::is_directory(output_dir)) {
    std::cerr << "Error: Output path is not a directory: " << output_dir
              << std::endl;
    return 1;
  }

  NameToId providers;
  NameToId symbols;

  // Create test data with multiple providers and symbols
  for (int64_t file_idx = 0; file_idx < num_files; ++file_idx) {
    std::vector<InputRow> input_rows;
    input_rows.reserve(1000); // 1000 rows per file

    // Create rows with varying timestamps, providers, symbols and prices
    for (int64_t i = 0; i < 3 * 1000 * 1000; i++) {
      input_rows.push_back(InputRow{
          1000000000000 + i * 1000000, // Timestamps 1ms apart
          providers.IDFromName("provider" +
                               std::to_string(i % 3)),            // 3 providers
          symbols.IDFromName("symbol" + std::to_string(i % 103)), // 103 symbols
          static_cast<double>(1 + (i % 17)) // Prices varying from 1-17
      });
    }

    std::string output_file = std::filesystem::path(output_dir) /
                              absl::StrFormat("test_%09d.parquet", file_idx);

    auto status =
        WriteParquetFromInputRows(output_file, input_rows, providers, symbols);
    if (!status.ok()) {
      std::cerr << "Error writing parquet file '" << output_file
                << "': " << status.ToString() << std::endl;
      return 1;
    }

    std::cout << "Created test file: " << output_file << std::endl;
  }

  return 0;
}

int main(int argc, char **argv) { return create_test_parquet_main(argc, argv); }
