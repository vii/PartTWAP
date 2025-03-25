#pragma once

#include <absl/strings/string_view.h>
#include <iostream>
#include <unistd.h>
#include <vector>

struct TempFileForTest {
  int tmp_fd;
  std::string tmp_filename;

  TempFileForTest() {
    char tmp_file_template[] = "/tmp/partvwap_test_XXXXXX";
    tmp_fd = mkstemp(tmp_file_template);
    if (tmp_fd == -1) {
      throw std::runtime_error("Failed to create temporary file");
    }
    tmp_filename = tmp_file_template;
  }

  ~TempFileForTest() {
    if (close(tmp_fd) == -1) {
      std::cerr << "Error closing temporary file " << tmp_filename << ": "
                << strerror(errno) << std::endl;
    }
    if (!tmp_filename.empty()) {
      std::remove(tmp_filename.c_str());
    }
  }
};
