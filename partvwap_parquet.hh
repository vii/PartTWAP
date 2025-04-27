#pragma once

#include "partvwap.hh"
#include <arrow/api.h>

#include <cassert>
#include <limits>
#include <cstdint>

struct ParquetChunk {
  int64_t num_rows;
  const arrow::Int32Array *provider_indices;
  const arrow::Int32Array *symbol_indices;
  const arrow::Int64Array *timestamp_array;
  const arrow::DoubleArray *price_array;

  const NameToId &providers;
  const NameToId &symbols;
};

arrow::Status WriteParquetFromInputRows(std::string filename,
                                        const std::vector<InputRow> &rows,
                                        const NameToId &providers,
                                        const NameToId &symbols);

arrow::Status
ReadParquetToInputRows(const std::string &filename,
                       std::function<arrow::Status(ParquetChunk)> f);

template <typename FilenameContainer, typename RowCallback>
arrow::Status ReadManyParquetFiles(const FilenameContainer &filenames,
                                   RowCallback &&f) {
  int64_t last_ts = std::numeric_limits<int64_t>::min();
  for (const auto &filename : filenames) {
    ARROW_RETURN_NOT_OK(
        ReadParquetToInputRows(filename, [&](ParquetChunk chunk) -> arrow::Status {
          for (int64_t i = 0; i < chunk.num_rows; i++) {
            int64_t ts = chunk.timestamp_array->Value(i);
            assert(ts >= last_ts);
            last_ts = ts;
            InputRow row{
                ts,
                static_cast<uint32_t>(chunk.provider_indices->Value(i)),
                static_cast<uint32_t>(chunk.symbol_indices->Value(i)),
                chunk.price_array->Value(i)
            };
            f(row);
          }
          return arrow::Status::OK();
        }));
  }
  return arrow::Status::OK();
}
