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
#include "temp_file_for_test.hh"

void LittleEndianInt64(std::ostream &os, int64_t value) {
  for (int i = 0; i < 8; ++i) {
    os.put(static_cast<char>((value >> (i * 8)) & 0xFF));
  }
}

int64_t ReadLittleEndianInt64(std::istream &is) {
  int64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value |= static_cast<int64_t>(static_cast<unsigned char>(is.get()))
             << (i * 8);
  }
  return value;
}

void WriteTurboPForFromInputRows(std::string filename,
                                 const std::vector<InputRow> &rows,
                                 const NameToId &providers,
                                 const NameToId &symbols,
                                 int64_t chunk = 1024 * 1024) {
  std::ofstream f(filename);
  if (!f.good()) {
    throw std::runtime_error(absl::StrCat("Failed to open file: ", filename));
  }

  LittleEndianInt64(f, rows.size());
  size_t buffer_size =
      std::max(bitnbound256v32(std::min(chunk, int64_t(rows.size()))),
               bitnbound128v64(std::min(chunk, int64_t(rows.size()))));
  std::vector<unsigned char> buffer(buffer_size);

  for (int64_t i = 0; i < rows.size();) {
    int64_t chunk_size = std::min(chunk, int64_t(rows.size()) - i);
    LittleEndianInt64(f, chunk_size);

    std::vector<int64_t> timestamp_chunk;
    std::vector<uint32_t> provider_chunk;
    std::vector<uint32_t> symbol_chunk;
    std::vector<double> price_chunk;

    timestamp_chunk.reserve(chunk_size);
    provider_chunk.reserve(chunk_size);
    symbol_chunk.reserve(chunk_size);
    price_chunk.reserve(chunk_size);

    for (int64_t j = 0; j < chunk_size; ++j, ++i) {
      timestamp_chunk.push_back(rows[i].ts_nanos);
      provider_chunk.push_back(rows[i].provider_id);
      symbol_chunk.push_back(rows[i].symbol_id);
      price_chunk.push_back(rows[i].price);
    }

    auto write_buffer = [&](auto &chunk) {
      size_t ts_actual_size;
      if constexpr (sizeof(typename std::remove_cvref_t<
                           decltype(chunk)>::value_type) == 8) {
        ts_actual_size = bitnpack128v64(reinterpret_cast<uint64_t *>(
                                            chunk.data()),
                                        chunk.size(), buffer.data());
      } else {
        ts_actual_size = bitnpack256v32(reinterpret_cast<uint32_t *>(
                                            chunk.data()),
                                        chunk.size(), buffer.data());
      }
      LittleEndianInt64(f, ts_actual_size);
      f.write(reinterpret_cast<const char *>(buffer.data()), ts_actual_size);
    };

    write_buffer(timestamp_chunk);
    write_buffer(provider_chunk);
    write_buffer(symbol_chunk);
    write_buffer(price_chunk);
  }

  if (!f.good()) {
    throw std::runtime_error(
        absl::StrCat("Failed to write TurboPFor data to file: ", filename));
  }
  f.close();
  if (f.fail()) {
    throw std::runtime_error(absl::StrCat("Failed to close file: ", filename));
  }
}

void ReadTurboPForFromInputRows(
    const std::string &filename,
    std::function<void(const InputRow &)> row_callback) {
  std::ifstream f(filename);
  if (!f.good()) {
    throw std::runtime_error(absl::StrCat("Failed to open file: ", filename));
  }
  int64_t num_rows = ReadLittleEndianInt64(f);

  std::vector<int64_t> timestamp_chunk;
  std::vector<uint32_t> provider_chunk;
  std::vector<uint32_t> symbol_chunk;
  std::vector<double> price_chunk;

  std::vector<unsigned char> buffer;
  while (num_rows > 0) {
    int64_t chunk_size = ReadLittleEndianInt64(f);
    if (!f.good()) {
      throw std::runtime_error(
          absl::StrCat("Failed to read chunk size from file: ", filename));
    }

    timestamp_chunk.resize(chunk_size);
    provider_chunk.resize(chunk_size);
    symbol_chunk.resize(chunk_size);
    price_chunk.resize(chunk_size);

    auto read_buffer = [&](auto &chunk) {
      int64_t ts_actual_size = ReadLittleEndianInt64(f);
      if (!f.good()) {
        throw std::runtime_error(absl::StrCat(
            "Failed to read timestamp chunksize from file: ", filename));
      }

      buffer.clear();
      buffer.resize(ts_actual_size);
      f.read(reinterpret_cast<char *>(buffer.data()), ts_actual_size);
      if (!f.good()) {
        throw std::runtime_error(absl::StrCat(
            "Failed to read timestamp data from file: ", filename));
      }
      if constexpr (sizeof(typename std::remove_cvref_t<
                           decltype(chunk)>::value_type) == 8) {
        bitnunpack128v64(buffer.data(), chunk.size(),
                        reinterpret_cast<uint64_t *>(chunk.data()));
      } else {
        static_assert(
            sizeof(typename std::remove_cvref_t<decltype(chunk)>::value_type) ==
            4);
        bitnunpack256v32(buffer.data(), chunk.size(),
                        reinterpret_cast<uint32_t *>(chunk.data()));
      }
    };

    read_buffer(timestamp_chunk);
    read_buffer(provider_chunk);
    read_buffer(symbol_chunk);
    read_buffer(price_chunk);

    for (int64_t j = 0; j < chunk_size; ++j) {
      row_callback(InputRow{timestamp_chunk[j], provider_chunk[j],
                            symbol_chunk[j], price_chunk[j]});
    }

    num_rows -= chunk_size;
  }
}

TEST(ComputeVWAP, InputRowsRoundTrip) {
  std::vector<InputRow> input_rows;
  TempFileForTest tmp_file;
  input_rows.reserve(1000);
  for (int64_t i = 0; i < 1000; ++i) {
    input_rows.push_back(
        InputRow{1000000000000 + i * 1000000, 0, 0, 100.0 + (i % 10)});
  }
  WriteTurboPForFromInputRows(tmp_file.tmp_filename, input_rows, NameToId{},
                              NameToId{});
  std::vector<InputRow> out_rows;
  ReadTurboPForFromInputRows(tmp_file.tmp_filename, [&](const InputRow &row) {
    out_rows.push_back(row);
  });
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
  WriteTurboPForFromInputRows(tmp_file.tmp_filename, input_rows, providers,
                              symbols);

  for (auto _ : state) {
    // Read back and compute VWAP
    double sum_twap = 0;
    ComputeVWAP(
        [&](auto &&f) { ReadTurboPForFromInputRows(tmp_file.tmp_filename, f); },
        [&](const OutputRow &output_row) { sum_twap += output_row.twap; });
    benchmark::DoNotOptimize(sum_twap);
  }

  state.SetItemsProcessed(state.iterations() * input_rows.size());
}
BENCHMARK(BM_TurboPForCompression);

TEST(Benchmarks, RunAll) { ::benchmark::RunSpecifiedBenchmarks("all"); }
