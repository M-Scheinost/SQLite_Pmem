#ifndef SQLITE_PERFORMANCE_BLOB_HELPERS_HPP
#define SQLITE_PERFORMANCE_BLOB_HELPERS_HPP

cxxopts::Options blob_options(const std::string &program,
                              const std::string &help_string = "") {
  cxxopts::Options options(program, help_string);
  cxxopts::OptionAdder adder = options.add_options();
  adder("load", "Load the database");
  adder("run", "Run the benchmark");
  adder("size", "Size of the blob in bytes",
        cxxopts::value<size_t>()->default_value("1000"));
  adder("mix", "Read transaction fraction",
        cxxopts::value<float>()->default_value("0.5"));
  adder("warmup", "Warmup duration in seconds",
        cxxopts::value<size_t>()->default_value("10"));
  adder("measure", "Measure duration in seconds",
        cxxopts::value<size_t>()->default_value("60"));
  adder("help", "Print help");

  adder("path", "Path", cxxopts::value<std::string>()->default_value("/mnt/pmem0/scheinost/benchmark.db"));
  adder("pmem", "Pmem", cxxopts::value<std::string>()->default_value("PMem"));
  adder("cache_size", "Cache size", cxxopts::value<std::string>()->default_value("0"));
  adder("sync", "Pmem", cxxopts::value<std::string>()->default_value("FULL"));

  return options;
}

#endif // SQLITE_PERFORMANCE_BLOB_HELPERS_HPP
