#include "partvwap.hh"
#include "partvwap_parquet.hh"
#include "partvwap_turbo.hh"
#include "perf_counter_scope.hh"

#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <absl/time/time.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <filesystem>
#include <iostream>
#include <vector>

ABSL_FLAG(absl::Duration, repeat_turbo_decode_duration, absl::ZeroDuration(),
          "Duration to keep repreating the turbo decode so a profile can be "
          "collected");

int main(int argc, char **argv) {
  std::vector<char *> args = absl::ParseCommandLine(argc, argv);

  if (args.size() != 4) {
    std::cerr << "Usage: " << args[0]
              << " <input_dir> <output_turbo_file> <output_parquet_file>"
              << std::endl;
    std::cerr
        << "This program reads parquet files from <input_dir> and writes a "
           "turbo file to <output_file>; and then writes the VWAP results to "
           "<output_parquet_file>."
        << std::endl;
    return 1;
  }

  std::string input_dir = args[1];
  const char *output_turbo_file = args[2];
  std::string output_parquet_file = args[3];

  if (!std::filesystem::exists(input_dir) ||
      !std::filesystem::is_directory(input_dir)) {
    std::cerr << "Error: Input directory does not exist or is not a directory: "
              << input_dir << std::endl;
    return 1;
  }

  std::vector<std::string> parquet_files = FindAndSortParquetFiles(input_dir);

  if (parquet_files.empty()) {
    std::cerr << "Error: No files found in directory: " << input_dir
              << std::endl;
    return 1;
  }

  NameToId providers;
  NameToId symbols;
  std::vector<InputRow> rows;

  arrow::Status read_status = ReadManyParquetFiles(
      parquet_files, [&](const InputRow &row) { rows.push_back(row); },
      providers, symbols);

  if (!read_status.ok()) {
    std::cerr << "Error reading parquet files from directory '" << input_dir
              << "': " << read_status.ToString() << std::endl;
    return 1;
  }

  if (!std::filesystem::exists(output_turbo_file) ||
      std::filesystem::file_size(output_turbo_file) == 0) {
    absl::Time turbo_start_time = absl::Now();
    WriteTurboPForFromInputRows(bitnpack128v64, bitnxpack256v32,
                                output_turbo_file, rows, providers, symbols);
    absl::Time turbo_end_time = absl::Now();

    std::cout << "Successfully converted " << rows.size()
              << " rows to turbo file " << output_turbo_file << std::endl;
    std::cout << "Time taken to write turbo file: "
              << absl::FormatDuration(turbo_end_time - turbo_start_time)
              << std::endl;
  } else {
    std::cout << "Turbo file already exists: " << output_turbo_file
              << std::endl;
  }

  absl::Time turbo_decode_start_time = absl::Now();
  absl::Time start_time;
  absl::Time end_time;
  int64_t input_rows;
  int64_t output_rows;
  do {

    ParquetOutputWriter writer(providers, symbols);

    auto open_status = writer.OpenOutputFile(output_parquet_file);
    if (!open_status.ok()) {
      std::cerr << "Error opening output file '" << output_parquet_file
                << "': " << open_status.ToString() << std::endl;
      return 1;
    }

    arrow::Status write_status;
    start_time = absl::Now();
    input_rows = 0;
    output_rows = 0;
    {
      PerfCounterScope perf_monitor("ComputeTWAP");
      ComputeTWAP(
          [&](auto &&row_acceptor) {
            ReadTurboPForFromInputRows(bitnunpack128v64, bitnxunpack256v32,
                                       output_turbo_file,
                                       [&](const InputRow &row) {
                                         row_acceptor(row);
                                         input_rows++;
                                       });
          },
          [&](const OutputRow &row) {
            output_rows++;
            write_status &= writer.AppendOutputRow(row);
          });
      perf_monitor.IncrementNumRows(input_rows);
      end_time = absl::Now();
    }
    if (!write_status.ok()) {
      std::cerr << "Error writing output file '" << output_parquet_file
                << "': " << write_status.ToString() << std::endl;
      return 1;
    }
    auto close_status = writer.CloseOutputFile();
    if (!close_status.ok()) {
      std::cerr << "Error closing output file '" << output_parquet_file
                << "': " << close_status.ToString() << std::endl;
      return 1;
    }
  } while (absl::Now() - turbo_decode_start_time <
           absl::GetFlag(FLAGS_repeat_turbo_decode_duration));

  std::cout << "Successfully processed " << input_rows << " rows; wrote "
            << output_rows << " results to " << output_parquet_file
            << std::endl;
  std::cout << "Time taken to compute TWAP: "
            << absl::FormatDuration(end_time - start_time) << std::endl
            << "Per input row " << (end_time - start_time) / input_rows
            << std::endl
            << "Total seconds " << absl::ToDoubleSeconds(end_time - start_time)
            << std::endl;

  return 0;
}