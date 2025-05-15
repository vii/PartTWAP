#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "partvwap.hh"

// Write input rows to a file using TurboPFor compression
void WriteTurboPForFromInputRows(const char *filename,
                                 const std::vector<InputRow> &rows,
                                 const NameToId &providers,
                                 const NameToId &symbols,
                                 int64_t chunk = 1024 * 1024);

// Read input rows from a file using TurboPFor compression
void ReadTurboPForFromInputRows(
    const char *filename, std::function<void(const InputRow &)> row_callback);
