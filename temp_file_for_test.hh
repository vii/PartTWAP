#pragma once

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <exception>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unistd.h>

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

  operator std::string_view() const { return tmp_filename; }
};

struct TempDirectoryForTest {
  std::string tmp_dirname;

  TempDirectoryForTest() {
    char tmp_dir_template[] = "/tmp/partvwap_test_dir_XXXXXX";
    if (mkdtemp(tmp_dir_template) == nullptr) {
      throw std::runtime_error("Failed to create temporary directory");
    }
    tmp_dirname = tmp_dir_template;
  }

  ~TempDirectoryForTest() {
    if (!tmp_dirname.empty()) {
      try {
        std::filesystem::remove_all(tmp_dirname);
      } catch (const std::exception &e) {
        std::cerr << "Error removing temporary directory " << tmp_dirname
                  << ": " << e.what() << std::endl;
      }
    }
  }

  operator std::string_view() const { return tmp_dirname; }
};
