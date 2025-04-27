#include "partvwap.hh"
#include "partvwap_parquet.hh"
#include "temp_file_for_test.hh"
#include <absl/cleanup/cleanup.h>
#include <absl/container/flat_hash_map.h>
#include <absl/status/status.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/string_view.h>
#include <absl/time/time.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/testing/gtest_util.h>
#include <benchmark/benchmark.h>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iomanip>
#include <iostream>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <vector>

TEST(ComputeVWAP, BasicThroughParquet) {
  TempFileForTest tmp_file;
  NameToId providers;
  NameToId symbols;
  std::vector<InputRow> input_rows = {
      InputRow{1000000000001, providers.IDFromName("provider1"),
               symbols.IDFromName("symbol1"), 100.0}};
  ASSERT_OK(WriteParquetFromInputRows(tmp_file.tmp_filename, input_rows,
                                      providers, symbols));

  // Read back and compute VWAP
  std::vector<OutputRow> output_rows;
  ComputeVWAP(
      [&](auto &&f) {
        ASSERT_OK(ReadManyParquetFiles(
            std::vector<std::string>{tmp_file.tmp_filename}, f));
      },
      [&](const OutputRow &output_row) { output_rows.push_back(output_row); });

  // Verify results
  EXPECT_THAT(output_rows,
              testing::ElementsAre(OutputRow{1005000000000, 0, 0, 100.0}));
};

static void BM_ComputeVWAPThroughParquet(benchmark::State &state) {
  TempFileForTest tmp_file;
  NameToId providers;
  NameToId symbols;
  std::vector<InputRow> input_rows;
  input_rows.reserve(1000000);

  // Create 1000 rows with varying timestamps, providers, symbols and prices
  for (int64_t i = 0; i < input_rows.capacity(); i++) {
    input_rows.push_back(InputRow{
        1000000000000 + i * 1000000, // Timestamps 1ms apart
        providers.IDFromName("provider" +
                             std::to_string(i % 10)),           // 10 providers
        symbols.IDFromName("symbol" + std::to_string(i % 100)), // 100 symbols
        100.0 + (i % 10) // Prices varying from 100-109
    });
  }
  ASSERT_OK(WriteParquetFromInputRows(tmp_file.tmp_filename, input_rows,
                                      providers, symbols));

  for (auto _ : state) {
    // Read back and compute VWAP
    double sum_twap = 0;
    ComputeVWAP(
        [&](auto &&f) {
          ASSERT_OK(ReadManyParquetFiles(
              std::vector<std::string>{tmp_file.tmp_filename}, f));
        },
        [&](const OutputRow &output_row) { sum_twap += output_row.twap; });
    benchmark::DoNotOptimize(sum_twap);
  }

  state.SetItemsProcessed(state.iterations() * input_rows.size());
}
BENCHMARK(BM_ComputeVWAPThroughParquet);

TEST(Benchmarks, RunAll) { ::benchmark::RunSpecifiedBenchmarks("all"); }
