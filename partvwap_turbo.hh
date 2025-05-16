#pragma once

#include "ic.h"
#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
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

// Write input rows to a file using TurboPFor compression
void WriteTurboPForFromInputRows(const char *filename,
                                 const std::vector<InputRow> &rows,
                                 const NameToId &providers,
                                 const NameToId &symbols,
                                 int64_t chunk = 512 * 1024);

// Read input rows from a file using TurboPFor compression
template <typename RowCallback>
void ReadTurboPForFromInputRows(const char *filename,
                                RowCallback &&row_callback) {

  int fd = open(filename, O_RDONLY);
  if (fd == -1) {
    throw std::runtime_error(absl::StrCat("Failed to open file '", filename,
                                          "': ", strerror(errno)));
  }
  absl::Cleanup fd_closer = [fd] { close(fd); };

  struct stat sb;
  if (fstat(fd, &sb) == -1) {
    throw std::runtime_error(absl::StrCat("Failed to fstat file '", filename,
                                          "': ", strerror(errno)));
  }
  const size_t file_size = sb.st_size;

  void *mmap_base_ptr = nullptr;

  if (file_size > 0) {
    mmap_base_ptr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mmap_base_ptr == MAP_FAILED || mmap_base_ptr == nullptr) {
      throw std::runtime_error(absl::StrCat("Failed to mmap file '", filename,
                                            "': ", strerror(errno)));
    }
  }

  absl::Cleanup unmapper = [&] {
    if (munmap(mmap_base_ptr, file_size) != 0) {
      throw std::runtime_error(absl::StrCat("Failed to munmap file '", filename,
                                            "': ", strerror(errno)));
    }
  };

  const char *p = static_cast<const char *>(mmap_base_ptr);
  size_t remaining = file_size;

  auto ConsumeBytes = [&](size_t n) {
    if (remaining < n) {
      throw std::runtime_error(absl::StrCat("Needed ", n, " bytes from file '",
                                            filename, " size ", file_size,
                                            " remaining ", remaining));
    }
    remaining -= n;
    const char *old_p = p;
    p += n;
    return old_p;
  };
  auto ReadLittleEndianInt64 = [&]() {
    const char *data = ConsumeBytes(8);
    int64_t value = 0;
    for (int i = 0; i < 8; ++i) {
      value |= static_cast<int64_t>(static_cast<unsigned char>(data[i]))
               << (i * 8);
    }
    return value;
  };

  int64_t num_rows = ReadLittleEndianInt64();

  std::vector<int64_t> timestamp_chunk;
  std::vector<uint32_t> provider_chunk;
  std::vector<uint32_t> symbol_chunk;
  std::vector<double> price_chunk;

  std::vector<unsigned char> buffer;
  while (num_rows > 0) {
    int64_t chunk_size = ReadLittleEndianInt64();
    timestamp_chunk.resize(chunk_size);
    provider_chunk.resize(chunk_size);
    symbol_chunk.resize(chunk_size);
    price_chunk.resize(chunk_size);

    auto read_buffer = [&](auto &chunk) {
      int64_t ts_actual_size = ReadLittleEndianInt64();

      if constexpr (sizeof(typename std::remove_cvref_t<
                           decltype(chunk)>::value_type) == 8) {
        bitnunpack128v64(reinterpret_cast<unsigned char *>(
                             const_cast<char *>(ConsumeBytes(ts_actual_size))),
                         chunk.size(),
                         reinterpret_cast<uint64_t *>(chunk.data()));
      } else {
        static_assert(
            sizeof(typename std::remove_cvref_t<decltype(chunk)>::value_type) ==
            4);
        bitnunpack256v32(reinterpret_cast<unsigned char *>(
                             const_cast<char *>(ConsumeBytes(ts_actual_size))),
                         chunk.size(),
                         reinterpret_cast<uint32_t *>(chunk.data()));
      }
    };

    read_buffer(timestamp_chunk);
    read_buffer(price_chunk);
    read_buffer(provider_chunk);
    read_buffer(symbol_chunk);

    for (int64_t j = 0; j < chunk_size; ++j) {
      row_callback(InputRow{timestamp_chunk[j], provider_chunk[j],
                            symbol_chunk[j], price_chunk[j]});
    }

    num_rows -= chunk_size;
  }
}
