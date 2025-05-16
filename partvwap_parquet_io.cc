#include "partvwap.hh"
#include "partvwap_parquet.hh"
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <filesystem>
#include <iostream>
#include <vector>

int main(int argc, char **argv) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <input_dir> <output_file>"
              << std::endl;
    std::cerr << "This program reads parquet files from <input_dir>, computes "
                 "volume-weighted average prices (VWAP)"
              << std::endl;
    std::cerr << "for each provider and symbol combination, and writes the "
                 "results to <output_file> in parquet format."
              << std::endl;
    return 1;
  }

  std::string input_dir = argv[1];
  std::string output_file = argv[2];

  if (!std::filesystem::exists(input_dir) ||
      !std::filesystem::is_directory(input_dir)) {
    std::cerr << "Error: Input directory does not exist or is not a directory: "
              << input_dir << std::endl;
    return 1;
  }

  std::vector<std::string> parquet_files;
  for (const auto &entry : std::filesystem::directory_iterator(input_dir)) {
    parquet_files.push_back(entry.path().string());
  }

  if (parquet_files.empty()) {
    std::cerr << "Error: No files found in directory: " << input_dir
              << std::endl;
    return 1;
  }

  NameToId providers;
  NameToId symbols;
  ParquetOutputWriter writer(providers, symbols);

  auto open_status = writer.OpenOutputFile(output_file);
  if (!open_status.ok()) {
    std::cerr << "Error opening output file '" << output_file
              << "': " << open_status.ToString() << std::endl;
    return 1;
  }

  arrow::Status read_status;
  arrow::Status write_status;
  absl::Time start_time = absl::Now();
  int64_t input_rows = 0;
  int64_t output_rows = 0;
  ComputeTWAP(
      [&](auto &&row_acceptor) {
        // Read all parquet files and process the data
        read_status &= ReadManyParquetFiles(
            parquet_files,
            [&](const InputRow &row) -> arrow::Status {
              row_acceptor(row);
              input_rows++;
              return arrow::Status::OK();
            },
            providers, symbols);
      },
      [&](const OutputRow &row) {
        output_rows++;
        write_status &= writer.AppendOutputRow(row);
      });
  absl::Time end_time = absl::Now();
  if (!write_status.ok()) {
    std::cerr << "Error writing output file '" << output_file
              << "': " << write_status.ToString() << std::endl;
    return 1;
  }

  if (!read_status.ok()) {
    std::cerr << "Error reading parquet files from directory '" << input_dir
              << "': " << read_status.ToString() << std::endl;
    return 1;
  }

  auto close_status = writer.CloseOutputFile();
  if (!close_status.ok()) {
    std::cerr << "Error closing output file '" << output_file
              << "': " << close_status.ToString() << std::endl;
    return 1;
  }

  std::cout << "Successfully processed " << input_rows << " rows; wrote "
            << output_rows << " results to " << output_file << std::endl;
  std::cout << "Time taken to compute TWAP: "
            << absl::FormatDuration(end_time - start_time) << std::endl
            << "Per input row " << (end_time - start_time) / input_rows
            << std::endl
            << "Total seconds " << absl::ToDoubleSeconds(end_time - start_time)
            << std::endl;

  return 0;
}
