#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "partvwap.hh"

// Write input rows to a file using TurboPFor compression
void WriteTurboPForFromInputRows(std::string filename,
                                 const std::vector<InputRow> &rows,
                                 const NameToId &providers,
                                 const NameToId &symbols,
                                 int64_t chunk = 1024 * 1024);

// Read input rows from a file using TurboPFor compression
void ReadTurboPForFromInputRows(
    const std::string &filename,
    std::function<void(const InputRow &)> row_callback);
