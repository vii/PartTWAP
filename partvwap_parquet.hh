#pragma once

#include "partvwap.hh"
#include <arrow/api.h>

#include <cassert>
#include <cstdint>
#include <limits>

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
    ARROW_RETURN_NOT_OK(ReadParquetToInputRows(
        filename, [&](ParquetChunk chunk) -> arrow::Status {
          for (int64_t i = 0; i < chunk.num_rows; i++) {
            int64_t ts = chunk.timestamp_array->Value(i);
            assert(ts >= last_ts);
            last_ts = ts;
            InputRow row{
                ts, static_cast<uint32_t>(chunk.provider_indices->Value(i)),
                static_cast<uint32_t>(chunk.symbol_indices->Value(i)),
                chunk.price_array->Value(i)};
            f(row);
          }
          return arrow::Status::OK();
        }));
  }
  return arrow::Status::OK();
}

struct ParquetOutputWriter {
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  const NameToId &providers;
  const NameToId &symbols;
  int64_t buffered_rows = 0;

  arrow::StringBuilder provider_builder;
  arrow::StringBuilder symbol_builder;
  arrow::Int64Builder timestamp_builder;
  arrow::DoubleBuilder twap_builder;

  std::shared_ptr<arrow::StringArray> provider_array;
  std::shared_ptr<arrow::StringArray> symbol_array;
  std::shared_ptr<arrow::Int64Array> timestamp_array;
  std::shared_ptr<arrow::DoubleArray> twap_array;

  arrow::Status OpenOutputFile(std::string filename);
  arrow::Status AppendOutputRow(const OutputRow &row);
  arrow::Status OutputRowChunk();
  arrow::Status CloseOutputFile();
};
