#include "run_command_for_test.hh"
#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <array>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <sys/wait.h>

std::string RunCommandForTest(const char *cmd) {
  std::cout << "Running command: " << cmd << std::endl;
  std::array<char, 4096> buffer;
  std::string result;

  // Open pipe to command
  FILE *pipe = popen(cmd, "r");
  if (!pipe) {
    throw std::runtime_error(absl::StrCat("Failed to run command: ", cmd,
                                          " error: ", strerror(errno),
                                          " (errno: ", errno, ")"));
  }
  absl::Cleanup pipe_closer = [pipe] { pclose(pipe); };

  size_t bytes_read;
  while ((bytes_read = fread(buffer.data(), 1, buffer.size(), pipe)) > 0) {
    result.append(buffer.data(), bytes_read);
  }

  if (std::ferror(pipe)) {
    throw std::runtime_error(absl::StrCat("Error reading from command: ", cmd,
                                          " error: ", strerror(errno),
                                          " (errno: ", errno, ")"));
  }
  std::move(pipe_closer).Cancel();

  int cmd_status = pclose(pipe);
  if (cmd_status == -1) {
    throw std::runtime_error(absl::StrCat("pclose failed: ", strerror(errno)));
  }

  if (!WIFEXITED(cmd_status)) {
    throw std::runtime_error(absl::StrCat("Command ", cmd,
                                          " did not exit normally",
                                          " (status: ", cmd_status, ")"));
  }
  if (WEXITSTATUS(cmd_status) != 0) {
    throw std::runtime_error(
        absl::StrCat("Command ", cmd, " exited with non-zero status: ",
                     WEXITSTATUS(cmd_status), "\n", result));
  }
  return result;
}