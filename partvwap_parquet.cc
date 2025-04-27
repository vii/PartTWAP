#include "partvwap_parquet.hh"
#include "partvwap.hh"
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

arrow::Status WriteParquetFromInputRows(std::string filename,
                                        const std::vector<InputRow> &rows,
                                        const NameToId &providers,
                                        const NameToId &symbols) {
  // Create schema
  auto schema = arrow::schema({arrow::field("provider", arrow::utf8()),
                               arrow::field("symbol", arrow::utf8()),
                               arrow::field("timestamp", arrow::int64()),
                               arrow::field("price", arrow::float64())});

  // Create builders
  arrow::StringBuilder provider_builder;
  arrow::StringBuilder symbol_builder;
  arrow::Int64Builder timestamp_builder;
  arrow::DoubleBuilder price_builder;

  // Append data
  for (const auto &row : rows) {
    ARROW_RETURN_NOT_OK(provider_builder.Append(providers[row.provider_id]));
    ARROW_RETURN_NOT_OK(symbol_builder.Append(symbols[row.symbol_id]));
    ARROW_RETURN_NOT_OK(timestamp_builder.Append(row.ts_nanos));
    ARROW_RETURN_NOT_OK(price_builder.Append(row.price));
  }

  // Create arrays
  std::shared_ptr<arrow::Array> provider_array;
  std::shared_ptr<arrow::Array> symbol_array;
  std::shared_ptr<arrow::Array> timestamp_array;
  std::shared_ptr<arrow::Array> price_array;

  ARROW_RETURN_NOT_OK(provider_builder.Finish(&provider_array));
  ARROW_RETURN_NOT_OK(symbol_builder.Finish(&symbol_array));
  ARROW_RETURN_NOT_OK(timestamp_builder.Finish(&timestamp_array));
  ARROW_RETURN_NOT_OK(price_builder.Finish(&price_array));

  // Create table
  auto table = arrow::Table::Make(
      schema, {provider_array, symbol_array, timestamp_array, price_array});

  // Write to Parquet file
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  ARROW_RETURN_NOT_OK(
      arrow::io::FileOutputStream::Open(filename).Value(&outfile));

  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), outfile, 65536));

  return arrow::Status::OK();
}

arrow::Status ReadParquetToInputRows(
    const std::string &filename,
    std::function<arrow::Status(ParquetChunk)> chunk_callback) {
  auto reader_props = parquet::ArrowReaderProperties();

  reader_props.set_read_dictionary(0, true); // provider column
  reader_props.set_read_dictionary(1, true); // symbol column

  parquet::arrow::FileReaderBuilder reader_builder;
  ARROW_RETURN_NOT_OK(reader_builder.OpenFile(filename));
  reader_builder.memory_pool(arrow::default_memory_pool());
  reader_builder.properties(reader_props);

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  ARROW_ASSIGN_OR_RAISE(arrow_reader, reader_builder.Build());

  std::shared_ptr<arrow::RecordBatchReader> rb_reader;
  ARROW_RETURN_NOT_OK(arrow_reader->GetRecordBatchReader(&rb_reader));

  // Create name to ID maps
  NameToId providers;
  NameToId symbols;
  // Process record batches
  std::shared_ptr<arrow::RecordBatch> batch;
  while (rb_reader->ReadNext(&batch).ok() && batch != nullptr) {
    // Get column arrays for this batch
    auto provider_array =
        std::static_pointer_cast<arrow::DictionaryArray>(batch->column(0));
    auto symbol_array =
        std::static_pointer_cast<arrow::DictionaryArray>(batch->column(1));
    auto timestamp_array =
        std::static_pointer_cast<arrow::Int64Array>(batch->column(2));
    auto price_array =
        std::static_pointer_cast<arrow::DoubleArray>(batch->column(3));
    // Unpack provider dictionary
    auto provider_dict = std::static_pointer_cast<arrow::StringArray>(
        provider_array->dictionary());
    auto provider_ids = std::vector<int64_t>(provider_dict->length());
    for (int64_t i = 0; i < provider_dict->length(); i++) {
      provider_ids[i] = providers.IDFromName(provider_dict->GetView(i));
    }
    ARROW_ASSIGN_OR_RAISE(auto provider_indices_view,
                          provider_array->indices()->View(arrow::int32()));
    auto provider_indices =
        std::static_pointer_cast<arrow::Int32Array>(provider_indices_view);

    // Unpack symbol dictionary
    auto symbol_dict = std::static_pointer_cast<arrow::StringArray>(
        symbol_array->dictionary());
    auto symbol_ids = std::vector<int64_t>(symbol_dict->length());
    for (int64_t i = 0; i < symbol_dict->length(); i++) {
      symbol_ids[i] = symbols.IDFromName(symbol_dict->GetView(i));
    }
    ARROW_ASSIGN_OR_RAISE(auto symbol_indices_view,
                          symbol_array->indices()->View(arrow::int32()));
    auto symbol_indices =
        std::static_pointer_cast<arrow::Int32Array>(symbol_indices_view);

    ParquetChunk chunk{.num_rows = batch->num_rows(),
                       .provider_indices = provider_indices.get(),
                       .symbol_indices = symbol_indices.get(),
                       .timestamp_array = timestamp_array.get(),
                       .price_array = price_array.get(),
                       .providers = providers,
                       .symbols = symbols};

    ARROW_RETURN_NOT_OK(chunk_callback(chunk));
  }

  return arrow::Status::OK();
}

arrow::Status ParquetOutputWriter::OpenOutputFile(std::string filename) {
  ARROW_RETURN_NOT_OK(arrow::io::FileOutputStream::Open(filename).Value(&outfile));
  return arrow::Status::OK();
}

arrow::Status ParquetOutputWriter::AppendOutputRow(const OutputRow &row) {
  ARROW_RETURN_NOT_OK(provider_builder.Append(providers[row.provider_id]));
  ARROW_RETURN_NOT_OK(symbol_builder.Append(symbols[row.symbol_id]));
  ARROW_RETURN_NOT_OK(timestamp_builder.Append(row.ts_nanos));
  ARROW_RETURN_NOT_OK(twap_builder.Append(row.twap));
  buffered_rows++;

  if (buffered_rows >= 1024*1024) {
    ARROW_RETURN_NOT_OK(OutputRowChunk());
  }

  return arrow::Status::OK();
}

arrow::Status ParquetOutputWriter::OutputRowChunk() {
  if (buffered_rows == 0) {
    return arrow::Status::OK();
  }

  std::shared_ptr<arrow::Array> provider_array;
  std::shared_ptr<arrow::Array> symbol_array;
  std::shared_ptr<arrow::Array> timestamp_array;
  std::shared_ptr<arrow::Array> twap_array;

  ARROW_RETURN_NOT_OK(provider_builder.Finish(&provider_array));
  ARROW_RETURN_NOT_OK(symbol_builder.Finish(&symbol_array));
  ARROW_RETURN_NOT_OK(timestamp_builder.Finish(&timestamp_array));
  ARROW_RETURN_NOT_OK(twap_builder.Finish(&twap_array));
  auto schema = arrow::schema({arrow::field("provider", arrow::utf8()),
                    arrow::field("symbol", arrow::utf8()),
                    arrow::field("timestamp", arrow::int64()),
                    arrow::field("twap", arrow::float64())});
  
  auto batch = arrow::RecordBatch::Make(schema, provider_array->length(),
                    {provider_array, symbol_array, timestamp_array, twap_array});
  
  std::shared_ptr<parquet::arrow::FileWriter> writer;
  ARROW_ASSIGN_OR_RAISE(
      writer,
      parquet::arrow::FileWriter::Open(
          *schema, 
          arrow::default_memory_pool(), 
          outfile, 
          parquet::WriterProperties::Builder().build(),
          parquet::ArrowWriterProperties::Builder().build()));
  
  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  ARROW_RETURN_NOT_OK(writer->Close());
  
  // Reset builders for next chunk
  provider_builder.Reset();
  symbol_builder.Reset();
  timestamp_builder.Reset();
  twap_builder.Reset();
  
  buffered_rows = 0;
  return arrow::Status::OK();
}

arrow::Status ParquetOutputWriter::CloseOutputFile() {
  ARROW_RETURN_NOT_OK(OutputRowChunk());
  ARROW_RETURN_NOT_OK(outfile->Close());
  return arrow::Status::OK();
}
