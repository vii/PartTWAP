#include <cstdint>
#include <cmath>
#include <absl/container/flat_hash_map.h>
#include <absl/strings/string_view.h>
#include <iostream>
#include <vector>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <iomanip>
#include <absl/time/time.h>
#include <benchmark/benchmark.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/arrow/reader.h>
#include <arrow/testing/gtest_util.h>
#include <absl/cleanup/cleanup.h>

struct InputRow {
    int64_t ts_nanos;
    uint32_t provider_id;
    uint32_t symbol_id;
    double price;
};

struct OutputRow {
    int64_t ts_nanos;
    uint32_t provider_id;
    uint32_t symbol_id;
    double twap;

    bool operator==(const OutputRow& other) const = default;
};

std::ostream& operator<<(std::ostream& os, const OutputRow& row) {
    auto time = absl::FromUnixNanos(row.ts_nanos);
    os << "OutputRow{" << absl::FormatTime(time, absl::UTCTimeZone()) << ", "
       << row.provider_id << ", " << row.symbol_id << ", " << row.twap << "}";
    return os;
}

struct TWAPState {
    int64_t last_ts_nanos = 0;
    double last_price = std::nan("");
    double price_nanos_sum = 0;
    int64_t nanos_sum = 0;

    bool Empty() const {
        return last_ts_nanos == 0;
    }

    void AddPrice(int64_t ts_nanos, double price) {
        if (!Empty()) {
            int64_t time_delta_nanos = ts_nanos - last_ts_nanos;
            price_nanos_sum += last_price * time_delta_nanos;
            nanos_sum += time_delta_nanos;
            last_ts_nanos = ts_nanos;
        }
        last_price = price;
        last_ts_nanos = ts_nanos;
    }

    double ComputeTWAP(int64_t ts_nanos) {
        AddPrice(ts_nanos, last_price);
        return price_nanos_sum / nanos_sum;
    }
};

template <typename InputRowProvider, typename OutputRowSink>
void ComputeVWAP(InputRowProvider&& input_row_provider, OutputRowSink&& output_row_sink, int64_t window_nanos = 15ll*1000*1000*1000) {
    std::vector<std::vector<TWAPState>> provider_to_symbol_to_twap;
    int64_t next_report_nanos = 0;

    auto Report = [&] () {
        for (uint32_t provider = 0; provider < provider_to_symbol_to_twap.size(); ++provider) {
            for (uint32_t symbol = 0; symbol < provider_to_symbol_to_twap[provider].size(); ++symbol) {
                auto& twap_state = provider_to_symbol_to_twap[provider][symbol];
                if (twap_state.Empty()) {
                    continue;
                }
                output_row_sink(OutputRow{next_report_nanos, provider, symbol, twap_state.ComputeTWAP(next_report_nanos)});
            }
        }
        next_report_nanos += window_nanos;
    };

    input_row_provider([&] (const InputRow& input_row) {
        if (next_report_nanos == 0) {
            next_report_nanos = ((input_row.ts_nanos + window_nanos) / window_nanos) * window_nanos;
        } else while (input_row.ts_nanos >= next_report_nanos) {
            Report();
        }
        if (input_row.provider_id >= provider_to_symbol_to_twap.size()) {
            provider_to_symbol_to_twap.resize(input_row.provider_id+1);
        }
        if (input_row.symbol_id >= provider_to_symbol_to_twap[input_row.provider_id].size()) {
            provider_to_symbol_to_twap[input_row.provider_id].resize(input_row.symbol_id+1);
        }
        provider_to_symbol_to_twap[input_row.provider_id][input_row.symbol_id].AddPrice(input_row.ts_nanos, input_row.price);
    });

    Report();
}

struct NameToId {
    absl::flat_hash_map<std::string, uint32_t> name_to_id;
    std::vector<std::string> id_to_name;

    uint32_t IDFromName(absl::string_view name) {
        auto it = name_to_id.find(name);
        if (it == name_to_id.end()) {
            it = name_to_id.emplace(std::string(name), id_to_name.size()).first;
            id_to_name.push_back(std::string(name));
        }
        return it->second;
    }

    absl::string_view operator[](uint32_t id) const {
        return id_to_name[id];
    }
};

TEST(ComputeVWAP, Basic) {
    std::vector<OutputRow> output_rows;
    ComputeVWAP(
        [&] (auto&& f) {
            f(InputRow{1000000000001, 17, 23, 100.0});
        },
        [&] (const OutputRow& output_row) {
            output_rows.push_back(output_row);
        }
    );
    EXPECT_THAT(output_rows, testing::ElementsAre(OutputRow{1005000000000, 17, 23, 100.0}));
};

arrow::Status WriteParquetFromInputRows(std::string filename, const std::vector<InputRow>& rows, const NameToId& providers, const NameToId& symbols) {
    // Create schema
    auto schema = arrow::schema({
        arrow::field("provider", arrow::utf8()),
        arrow::field("symbol", arrow::utf8()),
        arrow::field("timestamp", arrow::int64()),
        arrow::field("price", arrow::float64())
    });

    // Create builders
    arrow::StringBuilder provider_builder;
    arrow::StringBuilder symbol_builder;
    arrow::Int64Builder timestamp_builder;
    arrow::DoubleBuilder price_builder;

    // Append data
    for (const auto& row : rows) {
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
    auto table = arrow::Table::Make(schema, {provider_array, symbol_array, timestamp_array, price_array});

    // Write to Parquet file
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_RETURN_NOT_OK(arrow::io::FileOutputStream::Open(filename).Value(&outfile));

    ARROW_RETURN_NOT_OK(
        parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 65536)
    );

    return arrow::Status::OK();
}

arrow::Status ReadParquetToInputRows(const std::string& filename, std::function<void(const InputRow&)> row_callback) {
    // Open Parquet file
    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_RETURN_NOT_OK(arrow::io::ReadableFile::Open(filename).Value(&infile));

    // Create a ParquetFileReader instance
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<parquet::arrow::FileReader> reader,
        parquet::arrow::OpenFile(infile, arrow::default_memory_pool()));

    // Get number of row groups
    int num_row_groups = reader->num_row_groups();

    // Create name to ID maps
    NameToId providers;
    NameToId symbols;

    // Process each row group
    for (int r = 0; r < num_row_groups; r++) {
        std::shared_ptr<arrow::Table> row_group_table;
        ARROW_RETURN_NOT_OK(reader->RowGroup(r)->ReadTable(&row_group_table));

        // Get column arrays for this row group
        auto provider_array = std::static_pointer_cast<arrow::StringArray>(row_group_table->column(0)->chunk(0));
        auto symbol_array = std::static_pointer_cast<arrow::StringArray>(row_group_table->column(1)->chunk(0));
        auto timestamp_array = std::static_pointer_cast<arrow::Int64Array>(row_group_table->column(2)->chunk(0));
        auto price_array = std::static_pointer_cast<arrow::DoubleArray>(row_group_table->column(3)->chunk(0));

        // Process each row in the row group
        for (int64_t i = 0; i < row_group_table->num_rows(); i++) {
            InputRow row{
                timestamp_array->Value(i),
                providers.IDFromName(provider_array->GetView(i)),
                symbols.IDFromName(symbol_array->GetView(i)),
                price_array->Value(i)
            };
            row_callback(row);
        }
    }

    return arrow::Status::OK();
}


TEST(ComputeVWAP, BasicThroughParquet) {
    char tmp_file_template[] = "/tmp/partvwap_test_XXXXXX";
    int fd = mkstemp(tmp_file_template);
    if (fd == -1) {
        throw std::runtime_error("Failed to create temporary file");
    }
    absl::Cleanup close_fd = [fd] { close(fd); };
    std::string tmp_file(tmp_file_template);
    NameToId providers;
    NameToId symbols;
    std::vector<InputRow> input_rows = {
        InputRow{1000000000001, providers.IDFromName("provider1"), symbols.IDFromName("symbol1"), 100.0}
    };
    ASSERT_OK(WriteParquetFromInputRows(tmp_file, input_rows, providers, symbols));

    // Read back and compute VWAP
    std::vector<OutputRow> output_rows;
    ComputeVWAP(
        [&](auto&& f) {
            ASSERT_OK(ReadParquetToInputRows(tmp_file, f));
        },
        [&](const OutputRow& output_row) {
            output_rows.push_back(output_row);
        }
    );

    // Verify results
    EXPECT_THAT(output_rows, testing::ElementsAre(OutputRow{1005000000000, 0, 0, 100.0}));

    // Delete the parquet file
    std::remove(tmp_file.c_str());
};



static void BM_ComputeVWAP(benchmark::State& state) {
    for (auto _ : state) {
        double sum_price = 0;
        ComputeVWAP(
            [&] (auto&& f) {
                // Simulate processing multiple price updates
                for (int i = 0; i < 1000; i++) {
                    f(InputRow{1000000000000 + i * 1000000, 
                             static_cast<uint32_t>(i % 10), // 10 different providers
                             static_cast<uint32_t>(i % 100), // 100 different symbols
                             100.0 + (i % 10)}); // Varying prices
                }
            },
            [&] (const OutputRow& output_row) {
                sum_price += output_row.twap;
            }
        );
        benchmark::DoNotOptimize(sum_price);
    }
    state.SetItemsProcessed(state.iterations() * 1000);
}
BENCHMARK(BM_ComputeVWAP);

static void BM_ComputeVWAPThroughParquet(benchmark::State& state) {
    char tmp_file_template[] = "/tmp/partvwap_bench_XXXXXX";
    int fd = mkstemp(tmp_file_template);
    if (fd == -1) {
        throw std::runtime_error("Failed to create temporary file");
    }
    absl::Cleanup close_fd = [fd] { close(fd); };
    std::string tmp_file(tmp_file_template);
    NameToId providers;
    NameToId symbols;
    std::vector<InputRow> input_rows;
    input_rows.reserve(1000000);

    // Create 1000 rows with varying timestamps, providers, symbols and prices
    for (int64_t i = 0; i < input_rows.capacity(); i++) {
        input_rows.push_back(InputRow{
            1000000000000 + i * 1000000,  // Timestamps 1ms apart
            providers.IDFromName("provider" + std::to_string(i % 10)),  // 10 providers
            symbols.IDFromName("symbol" + std::to_string(i % 100)),     // 100 symbols
            100.0 + (i % 10)  // Prices varying from 100-109
        });
    }
    ASSERT_OK(WriteParquetFromInputRows(tmp_file, input_rows, providers, symbols));

    for (auto _ : state) {
        // Read back and compute VWAP
        double sum_twap = 0;
        ComputeVWAP(
            [&](auto&& f) {
                ASSERT_OK(ReadParquetToInputRows(tmp_file, f));
            },
            [&](const OutputRow& output_row) {
                sum_twap += output_row.twap;
            }
        );
        benchmark::DoNotOptimize(sum_twap);
    }

    state.SetItemsProcessed(state.iterations() * input_rows.size());

    // Delete the parquet file
    std::remove(tmp_file.c_str());
}



TEST(Benchmarks, RunAll) {
    ::benchmark::RunSpecifiedBenchmarks("all");
}



int real_main() {
    NameToId providers;
    NameToId symbols;
    ComputeVWAP(
        [&] (auto&& f) {
            f(InputRow{1000000000000, providers.IDFromName("provider1"), symbols.IDFromName("symbol1"), 100.0});
        },
        [&] (const OutputRow& output_row) {
            std::cout << output_row.ts_nanos << "," << providers.id_to_name[output_row.provider_id] << "," << symbols.id_to_name[output_row.symbol_id] << "," << output_row.twap << "\n";
        }
    );
    return 0;
}