#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <errno.h>
#include <iomanip>
#include <iostream>
#include <linux/perf_event.h>
#include <source_location>
#include <stdexcept>
#include <string>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <utility>

struct FileForPerfEvents {
  int fd = -1;

  FileForPerfEvents() = delete;
  FileForPerfEvents(const FileForPerfEvents &) = delete;
  FileForPerfEvents &operator=(const FileForPerfEvents &) = delete;
  FileForPerfEvents(FileForPerfEvents &&other) noexcept : fd(other.fd) {
    other.fd = -1;
  }
  FileForPerfEvents &operator=(FileForPerfEvents &&other) noexcept {
    if (this != &other) {
      if (fd != -1) {
        close(fd);
      }
      fd = other.fd;
      other.fd = -1;
    }
    return *this;
  }

  FileForPerfEvents(int fd) : fd(fd) {}

  void reset(int new_fd) {
    if (fd != -1) {
      close(fd);
    }
    fd = new_fd;
  }

  uint64_t read_counter() {
    uint64_t value;
    ssize_t ret = read(fd, &value, sizeof(value));
    if (ret != sizeof(value)) {
      throw std::runtime_error("Error reading counter for perf events: " +
                               std::string(strerror(errno)));
    }
    return value;
  }

  ~FileForPerfEvents() {
    if (fd != -1) {
      int ret = close(fd);
      if (ret != 0) {
        std::cerr << "Error closing file descriptor for perf events: "
                  << strerror(errno) << std::endl;
      }
      fd = -1;
    }
  }

  bool operator!() const { return fd == -1; }
};

// Performance monitoring class using Linux perf events with RAII scope
class PerfCounterScope {
  FileForPerfEvents fd_cycles{-1};
  FileForPerfEvents fd_instructions{-1};
  FileForPerfEvents fd_branch_misses{-1};
  FileForPerfEvents fd_l1_dcache_misses{-1};
  FileForPerfEvents fd_stalled_cycles_frontend{-1};
  std::string scope_name;
  uint64_t num_rows = 0;

  int OpenPerfEventFd(uint32_t type, uint64_t config, unsigned long flags= 0) {
    struct perf_event_attr attr;
    std::memset(&attr, 0, sizeof(attr));
    attr.type = type;
    attr.config = config;
    attr.size = sizeof(attr);
    attr.disabled = !!fd_cycles;
    attr.exclude_kernel = 1;
    attr.exclude_hv = 1;
    pid_t pid = 0;
    int cpu = -1;
    return syscall(__NR_perf_event_open, &attr, pid, cpu, fd_cycles.fd, flags);
  }

public:
  struct PerfCounts {
    uint64_t cycles;
    uint64_t instructions;
    uint64_t branch_misses;
    uint64_t l1_dcache_misses;
    uint64_t stalled_cycles_frontend;
  };

  explicit PerfCounterScope(
      std::string name = "",
      std::source_location location = std::source_location::current())
      : scope_name(name.empty() ? std::string(location.file_name()) + ":" +
                                      std::to_string(location.line())
                                : std::move(name)) {
    int cycles_fd = OpenPerfEventFd(PERF_TYPE_HARDWARE,
                                    PERF_COUNT_HW_CPU_CYCLES);
    if (cycles_fd == -1) {
      throw std::runtime_error("Failed to open cycles perf event: " +
                               std::string(strerror(errno)));
    }

    fd_cycles = FileForPerfEvents(cycles_fd);

    int instructions_fd = OpenPerfEventFd(
        PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS);
    if (instructions_fd == -1) {
      throw std::runtime_error("Failed to open instructions perf event: " +
                               std::string(strerror(errno)));
    }
    fd_instructions = FileForPerfEvents(instructions_fd);

    int branch_misses_fd = OpenPerfEventFd(
        PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES);
    if (branch_misses_fd == -1) {
      throw std::runtime_error("Failed to open branch misses perf event: " +
                               std::string(strerror(errno)));
    }
    fd_branch_misses = FileForPerfEvents(branch_misses_fd);

    uint64_t l1_cache_config = (PERF_COUNT_HW_CACHE_L1D << 0) |
                               (PERF_COUNT_HW_CACHE_OP_READ << 8) |
                               (PERF_COUNT_HW_CACHE_RESULT_MISS << 16);
    int l1_dcache_misses_fd = OpenPerfEventFd(
        PERF_TYPE_HW_CACHE, l1_cache_config);
    if (l1_dcache_misses_fd == -1) {
      throw std::runtime_error("Failed to open L1 cache misses perf event: " +
                               std::string(strerror(errno)));
    }
    fd_l1_dcache_misses = FileForPerfEvents(l1_dcache_misses_fd);

    int stalled_frontend_fd = OpenPerfEventFd(
        PERF_TYPE_HARDWARE, PERF_COUNT_HW_STALLED_CYCLES_FRONTEND);
    if (stalled_frontend_fd == -1) {
      throw std::runtime_error("Failed to open stalled frontend perf event: " +
                               std::string(strerror(errno)));
    }
    fd_stalled_cycles_frontend = FileForPerfEvents(stalled_frontend_fd);

    start();
  }

  void IncrementNumRows(uint64_t amt = 1) { num_rows += amt; }

  ~PerfCounterScope() {
    stop();
    print_summary();
  }

private:
  void start() {
    int ret = ioctl(fd_cycles.fd, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
    if (ret == -1) {
      throw std::runtime_error("Failed to reset perf events: " +
                               std::string(strerror(errno)));
    }

    ret = ioctl(fd_cycles.fd, PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP);
    if (ret == -1) {
      throw std::runtime_error("Failed to enable perf events: " +
                               std::string(strerror(errno)));
    }
  }

  void stop() {
    int ret = ioctl(fd_cycles.fd, PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
    if (ret == -1) {
      throw std::runtime_error("Failed to disable perf events: " +
                               std::string(strerror(errno)));
    }
  }

  PerfCounts read() {
    return {
        .cycles = fd_cycles.read_counter(),
        .instructions = fd_instructions.read_counter(),
        .branch_misses = fd_branch_misses.read_counter(),
        .l1_dcache_misses = fd_l1_dcache_misses.read_counter(),
        .stalled_cycles_frontend = fd_stalled_cycles_frontend.read_counter(),
    };
  }

  void print_summary(std::ostream &os = std::cout) {
    try {
      PerfCounts counts = read();

      if (counts.cycles == 0) {
        os << "Warning: No performance data collected for scope '" << scope_name
           << "'" << std::endl;
        return;
      }

      double ipc = (double)counts.instructions / counts.cycles;
      double branch_miss_rate =
          (double)counts.branch_misses / counts.instructions * 100.0;
      double l1_miss_rate =
          (double)counts.l1_dcache_misses / counts.instructions * 100.0;
      double frontend_stall_pct =
          (double)counts.stalled_cycles_frontend / counts.cycles * 100.0;

      os << "\n=== PERFORMANCE METRICS [" << scope_name << "] ===" << std::endl;

      if (num_rows > 0) {
        os << "  Cycles per row: " << (double)counts.cycles / num_rows
           << std::endl;
        os << "  Instructions per row: "
           << (double)counts.instructions / num_rows << std::endl;
        os << "  Branch mispredictions per row: "
           << (double)counts.branch_misses / num_rows << std::endl;
        os << "  L1 cache misses per row: "
           << (double)counts.l1_dcache_misses / num_rows << std::endl;
      }

      os << "  IPC (Instructions per cycle): " << std::fixed
         << std::setprecision(3) << ipc << std::endl;
      os << "  Branch misprediction rate: " << std::fixed
         << std::setprecision(3) << branch_miss_rate << "%" << std::endl;
      os << "  L1 cache miss rate: " << std::fixed << std::setprecision(3)
         << l1_miss_rate << "%" << std::endl;
      os << "  Frontend stall percentage: " << std::fixed
         << std::setprecision(3) << frontend_stall_pct << "%" << std::endl;
      os << "================================\n" << std::endl;
    } catch (const std::exception &e) {
      os << "Error reading performance data: " << e.what() << std::endl;
    }
  }
};