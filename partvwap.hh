#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/strings/string_view.h>
#include <absl/time/time.h>
#include <cmath>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

struct InputRow {
  int64_t ts_nanos;
  uint32_t provider_id;
  uint32_t symbol_id;
  double price;

  bool operator==(const InputRow &other) const = default;
  auto operator<=>(const InputRow &other) const = default;
};
inline std::ostream &operator<<(std::ostream &os, const InputRow &row) {
  auto time = absl::FromUnixNanos(row.ts_nanos);
  os << "InputRow{" << absl::FormatTime(time, absl::UTCTimeZone()) << ", "
     << row.provider_id << ", " << row.symbol_id << ", " << row.price << "}";
  return os;
}

struct OutputRow {
  int64_t ts_nanos;
  uint32_t provider_id;
  uint32_t symbol_id;
  double twap;

  bool operator==(const OutputRow &other) const = default;
};

inline std::ostream &operator<<(std::ostream &os, const OutputRow &row) {
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

  bool Empty() const { return last_ts_nanos == 0; }

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
void ComputeVWAP(InputRowProvider &&input_row_provider,
                 OutputRowSink &&output_row_sink,
                 int64_t window_nanos = 15ll * 1000 * 1000 * 1000) {
  std::vector<std::vector<TWAPState>> provider_to_symbol_to_twap;
  int64_t next_report_nanos = 0;

  auto Report = [&]() {
    for (uint32_t provider = 0; provider < provider_to_symbol_to_twap.size();
         ++provider) {
      for (uint32_t symbol = 0;
           symbol < provider_to_symbol_to_twap[provider].size(); ++symbol) {
        auto &twap_state = provider_to_symbol_to_twap[provider][symbol];
        if (twap_state.Empty()) {
          continue;
        }
        output_row_sink(OutputRow{next_report_nanos, provider, symbol,
                                  twap_state.ComputeTWAP(next_report_nanos)});
      }
    }
    next_report_nanos += window_nanos;
  };

  input_row_provider([&](const InputRow &input_row) {
    if (next_report_nanos == 0) {
      next_report_nanos =
          ((input_row.ts_nanos + window_nanos) / window_nanos) * window_nanos;
    } else
      while (input_row.ts_nanos >= next_report_nanos) {
        Report();
      }
    if (input_row.provider_id >= provider_to_symbol_to_twap.size()) {
      provider_to_symbol_to_twap.resize(input_row.provider_id + 1);
    }
    if (input_row.symbol_id >=
        provider_to_symbol_to_twap[input_row.provider_id].size()) {
      provider_to_symbol_to_twap[input_row.provider_id].resize(
          input_row.symbol_id + 1);
    }
    provider_to_symbol_to_twap[input_row.provider_id][input_row.symbol_id]
        .AddPrice(input_row.ts_nanos, input_row.price);
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

  absl::string_view operator[](uint32_t id) const { return id_to_name[id]; }
};
