#include "ic.h"
#include <absl/cleanup/cleanup.h>
#include <absl/container/flat_hash_map.h>
#include <absl/status/status.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/string_view.h>
#include <absl/time/time.h>
#include <cmath>
#include <cstdint>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include "partvwap.hh"

namespace {
void LittleEndianInt64(std::ostream &os, int64_t value) {
  for (int i = 0; i < 8; ++i) {
    os.put(static_cast<char>((value >> (i * 8)) & 0xFF));
  }
}
} // namespace

void WriteTurboPForFromInputRows(const char *filename,
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
        ts_actual_size =
            bitnpack128v64(reinterpret_cast<uint64_t *>(chunk.data()),
                           chunk.size(), buffer.data());
      } else {
        ts_actual_size =
            bitnpack256v32(reinterpret_cast<uint32_t *>(chunk.data()),
                           chunk.size(), buffer.data());
      }
      LittleEndianInt64(f, ts_actual_size);
      f.write(reinterpret_cast<const char *>(buffer.data()), ts_actual_size);
    };

    write_buffer(timestamp_chunk);
    write_buffer(price_chunk);
    write_buffer(provider_chunk);
    write_buffer(symbol_chunk);
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
