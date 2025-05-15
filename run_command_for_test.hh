#pragma once

#include <string>

// Run a shell command and return its output as a string.
// Throws std::runtime_error if the command fails.
std::string RunCommandForTest(const char* cmd); 