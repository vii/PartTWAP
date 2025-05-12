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
#include "temp_file_for_test.hh"

TEST(ComputeTWAP, Basic) {
  std::vector<OutputRow> output_rows;
  ComputeTWAP(
      [&](auto &&f) { f(InputRow{1000000000001, 17, 23, 100.0}); },
      [&](const OutputRow &output_row) { output_rows.push_back(output_row); });
  EXPECT_THAT(output_rows,
              testing::ElementsAre(OutputRow{1005000000000, 17, 23, 100.0}));
};

static void BM_ComputeTWAP(benchmark::State &state) {
  for (auto _ : state) {
    double sum_price = 0;
    ComputeTWAP(
        [&](auto &&f) {
          // Simulate processing multiple price updates
          for (int i = 0; i < 1000; i++) {
            f(InputRow{1000000000000 + i * 1000000,
                       static_cast<uint32_t>(i % 10),  // 10 different providers
                       static_cast<uint32_t>(i % 100), // 100 different symbols
                       100.0 + (i % 10)});             // Varying prices
          }
        },
        [&](const OutputRow &output_row) { sum_price += output_row.twap; });
    benchmark::DoNotOptimize(sum_price);
  }
  state.SetItemsProcessed(state.iterations() * 1000);
}
BENCHMARK(BM_ComputeTWAP);

TEST(Benchmarks, RunAll) { ::benchmark::RunSpecifiedBenchmarks("all"); }

int real_main() {
  NameToId providers;
  NameToId symbols;
  ComputeTWAP(
      [&](auto &&f) {
        f(InputRow{1000000000000, providers.IDFromName("provider1"),
                   symbols.IDFromName("symbol1"), 100.0});
      },
      [&](const OutputRow &output_row) {
        std::cout << output_row.ts_nanos << ","
                  << providers.id_to_name[output_row.provider_id] << ","
                  << symbols.id_to_name[output_row.symbol_id] << ","
                  << output_row.twap << "\n";
      });
  return 0;
}