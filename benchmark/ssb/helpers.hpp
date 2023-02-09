#ifndef SQLITE_PERFORMANCE_SSB_HELPERS_HPP
#define SQLITE_PERFORMANCE_SSB_HELPERS_HPP

#include <array>
#include <chrono>
#include <cxxopts.hpp>
#include <string>

template <typename F> double time(F &&f) {
  auto t0 = std::chrono::high_resolution_clock::now();
  f();
  auto t1 = std::chrono::high_resolution_clock::now();
  return std::chrono::duration<double>(t1 - t0).count();
}

cxxopts::Options ssb_options(const std::string &program,
                             const std::string &help_string = "") {
  cxxopts::Options options(program, help_string);
  cxxopts::OptionAdder adder = options.add_options();
  adder("help", "Print help");
  adder("path", "Path", cxxopts::value<std::string>()->default_value("/mnt/pmem0/scheinost/benchmark.db"));
  adder("sf", "the scale factor", cxxopts::value<std::string>()->default_value("1"));
  adder("pmem", "Pmem", cxxopts::value<std::string>()->default_value("PMem"));
  adder("cache_size", "Cache size", cxxopts::value<std::string>()->default_value("0"));
  adder("sync", "Pmem", cxxopts::value<std::string>()->default_value("FULL"));
  adder("bloom_filter", "Use Bloom filters", cxxopts::value<bool>()->default_value("false"));
  return options;
}

#endif // SQLITE_PERFORMANCE_SSB_HELPERS_HPP
