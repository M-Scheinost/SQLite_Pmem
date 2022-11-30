#include "Common.hpp"
#include "NonVolatileMemory.hpp"
#include "Random.hpp"
#include <set>
#include <cstring>
#include <inttypes.h>
#include <unordered_map>
#include <vector>
#include <functional>
#include <chrono>
#include <memory>

using namespace std;

const static ub8 KILO = 1024;
const static ub8 MEGA = 1024 * 1024;
const static ub8 GIGA = 1024 * 1024 * 1024;

#include "PMDK_log.hpp"
#include "PopLog.hpp"

namespace {

ub8 RunWithTiming(function<void()> foo)
{
   auto begin = chrono::high_resolution_clock::now();
   foo();
   auto end = chrono::high_resolution_clock::now();
   return chrono::duration_cast<chrono::nanoseconds>(end - begin).count();
}

vector<ub1> RandomizedMemory(ub8 size, Random &ranny)
{
   vector<ub1> memory(size);
   for (ub8 i = 0; i<size; i += 8) {
      ((ub8 &) memory[i]) = ranny.Rand();
   }
   return memory;
}

}

// Config
ub4 MIN_LOG_ENTRY_SIZE;
ub4 MAX_LOG_ENTRY_SIZE;
ub8 LOG_PAYLOAD_SIZE;
ub8 NVM_SIZE;
ub4 RUNS;
string NVM_FILE;
bool TABLE_VIEW;

void PrintResult(string name, ub4 entry_size, double ns_per_entry, uint64_t written_byte_count)
{
   //@formatter:off
   cout << "res:";
   cout << " technique: " << name;
   cout << " written_byte_count(byte): " << written_byte_count;
   cout << " log_payload_size(MB): " << LOG_PAYLOAD_SIZE / 1000 / 1000;
   cout << " runs: " << RUNS;
   cout << " entry_size: " << entry_size;
   cout << " ns_per_entry(ns): " << ns_per_entry;
   cout << endl;
   //@formatter:on
}

// clang++ -g0 -O3 -DNDEBUG=1 -march=native -std=c++17 logging/logging.cpp -Invml/src/include/ nvml/src/nondebug/libpmem.a nvml/src/nondebug/libpmemlog.a -lpthread -lndctl -ldaxctl && ./a.out 128 128 1000000000 3 /mnt/pmem0/renen/file_0 0
int main(int argc, char **argv)
{
   Random ranny;

   if (argc != 7) {
      cout << "usage: " << argv[0] << " min_log_entry_size max_log_entry_size total_size repetitions path table_view" << endl;
      throw;
   }

   // Config
   MIN_LOG_ENTRY_SIZE = stof(argv[1]); // Min log entry size
   MAX_LOG_ENTRY_SIZE = stof(argv[2]); // Max log entry size
   LOG_PAYLOAD_SIZE = stof(argv[3]); // How much total log payload content
   NVM_SIZE = LOG_PAYLOAD_SIZE * 4;
   RUNS = stoi(argv[4]); // Number of runs for each algorithm
   NVM_FILE = argv[5]; // Path to the nvm file
   TABLE_VIEW = stoi(argv[6]);
   assert(MIN_LOG_ENTRY_SIZE % 8 == 0);
   assert(MAX_LOG_ENTRY_SIZE % 8 == 0);

   NonVolatileMemory nvm(NVM_FILE, NVM_SIZE);

   cout << "Config" << endl;
   cout << "------" << endl;
   cout << "min_log_entry_size  " << MIN_LOG_ENTRY_SIZE << endl;
   cout << "max_log_entry_size  " << MAX_LOG_ENTRY_SIZE << endl;
   cout << "log_payload_size    " << LOG_PAYLOAD_SIZE / 1000 / 1000 << "MB" << endl;
   cout << "nvm_size            " << NVM_SIZE / 1000 / 1000 << "MB" << endl;
   cout << "runs                " << RUNS << endl;
   cout << "nvm_file            " << NVM_FILE << endl;
#ifndef NDEBUG
   cout << "NDEBUG              " << "debug" << endl;
#else
   cout << "NDEBUG              " << "release" << endl;
#endif
   cout << "nvm                 " << (nvm.IsNvm() ? "yes" : "no") << endl;
   cout << "------" << endl;

   if (TABLE_VIEW) {
      printf("%-20s%20s%20s%20s%20s%20s%20s%20s%20s%20s%20s%20s%20s%20s\n", "entry_size", "libPmem", "classic", "classicCached", "classicAligned", "header", "headerAligned", "headerDanc", "headerAligDanc", "zero", "zeroAligned", "zeroBlocked", "zeroSimd", "Mnemosyne");
   }

   vector<ub1> memory = RandomizedMemory(NVM_SIZE, ranny);
   for (ub4 entry_size = MIN_LOG_ENTRY_SIZE; entry_size<=MAX_LOG_ENTRY_SIZE; entry_size += 8) {
      memset(nvm.Data(), 0, nvm.GetByteCount());
      if (TABLE_VIEW) {
         printf("%-20i", entry_size);
         fflush(stdout);
      }

    // PmemLib
    //{
    //   LogWriterPMemLib wal(NVM_FILE, NVM_SIZE);
    //   vector<LogWriterPMemLib::Entry *> entries = LogWriterPMemLib::CreateRandomEntries(memory, entry_size / 8, entry_size / 8, LOG_PAYLOAD_SIZE, ranny);
    //   ub8 ns_spend = RunWithTiming([&]() {
    //      for (LogWriterPMemLib::Entry *entry : entries) {
    //         wal.AddLogEntry(*entry);
    //      }
    //   });
    //   if (TABLE_VIEW) {
    //      printf("%20f", ns_spend * 1.0 / entries.size());
    //      fflush(stdout);
    //   } else {
    //      PrintResult("libPmem", entry_size, ns_spend * 1.0 / entries.size(), wal.GetWrittenByteCount());
    //   }
    //}

      // Zero
      {
         LogWriterZero wal(nvm);
         vector<LogWriterZero::Entry *> entries = LogWriterZero::CreateRandomEntries(memory, entry_size / 8, entry_size / 8, LOG_PAYLOAD_SIZE, ranny);
         ub8 ns_spend = RunWithTiming([&]() {
            for (LogWriterZero::Entry *entry : entries) {
               wal.AddLogEntry(*entry);
            }
         });
         if (TABLE_VIEW) {
            printf("%20f", ns_spend * 1.0 / entries.size());
            fflush(stdout);
         } else {
            PrintResult("zero", entry_size, ns_spend * 1.0 / entries.size(), wal.GetWrittenByteCount());
         }
      }

      if (TABLE_VIEW) {
         printf("\n");
      }
   }

   return 0;
}