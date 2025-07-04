#include "ic.h"
#include <absl/container/flat_hash_map.h>
#include <absl/status/status.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/string_view.h>
#include <absl/time/time.h>
#include <benchmark/benchmark.h>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iomanip>
#include <iostream>
#include <vector>

#include "partvwap.hh"
#include "partvwap_turbo.hh"
#include "temp_file_for_test.hh"

TEST(ComputeTWAP, InputRowsRoundTrip) {
  std::vector<InputRow> input_rows;
  TempFileForTest tmp_file;
  input_rows.reserve(1000);
  for (int64_t i = 0; i < 1000; ++i) {
    input_rows.push_back(
        InputRow{1000000000000 + i * 1000000, 0, 0, 100.0 + (i % 10)});
  }
  WriteTurboPForFromInputRows(bitnpack128v64, bitnpack256v32,
                              tmp_file.tmp_filename.c_str(), input_rows,
                              NameToId{}, NameToId{});
  std::vector<InputRow> out_rows;
  ReadTurboPForFromInputRows(
      bitnunpack128v64, bitnunpack256v32, tmp_file.tmp_filename.c_str(),
      [&](const InputRow &row) { out_rows.push_back(row); });
  EXPECT_EQ(out_rows.size(), input_rows.size());
  ASSERT_TRUE(!out_rows.empty());
  ASSERT_TRUE(!input_rows.empty());
  EXPECT_EQ(out_rows[0], input_rows[0]);
  EXPECT_THAT(out_rows, testing::ElementsAreArray(input_rows));
}

static void BM_TurboPForCompression(benchmark::State &state) {
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
  WriteTurboPForFromInputRows(bitnpack128v64, bitnpack256v32,
                              tmp_file.tmp_filename.c_str(), input_rows,
                              providers, symbols);

  for (auto _ : state) {
    // Read back and compute VWAP
    double sum_twap = 0;
    ComputeTWAP(
        [&](auto &&f) {
          ReadTurboPForFromInputRows(bitnunpack128v64, bitnunpack256v32,
                                     tmp_file.tmp_filename.c_str(), f);
        },
        [&](const OutputRow &output_row) { sum_twap += output_row.twap; });
    benchmark::DoNotOptimize(sum_twap);
  }

  state.SetItemsProcessed(state.iterations() * input_rows.size());
}
BENCHMARK(BM_TurboPForCompression);

TEST(Benchmarks, RunAll) { ::benchmark::RunSpecifiedBenchmarks("all"); }
