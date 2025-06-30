#include "perf_counter_scope.hh"
#include <iostream>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

TEST(PerfCounterScopeTest, Constructor) {
    PerfCounterScope scope;
}
