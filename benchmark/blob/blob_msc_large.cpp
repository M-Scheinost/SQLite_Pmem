#include "cxxopts.hpp"
#include "dbbench/runner.hpp"
#include "helpers.hpp"

#include <atomic>
#include <iostream>
#include <random>
#include <thread>
#include <fstream>

#include "../msc_large_helper.hpp"

using namespace std;


int step(sqlite3_stmt *stmt, size_t &count){
  while(sqlite3_step(stmt) == SQLITE_ROW){
    count++;
  }
  return sqlite3_reset(stmt);
}

int step_single(sqlite3_stmt *stmt){
  size_t count;
  return step(stmt, count);
}

class Worker {
public:
  Worker(sqlite3*db_, size_t size, float mix) : db(db_), size_(size), blob_(malloc(size)),
        dis_({mix, 1.0 - mix}), gen_(std::random_device()()) {
    int rc = sqlite3_prepare_v2(db, "SELECT a FROM t", -1, &select_stmt_, NULL);
     if(rc){cout << "SELECT: " << rc << endl;}
    rc = sqlite3_prepare_v2(db, "UPDATE t SET a = ?", -1, &update_stmt_, NULL);
     if(rc){cout << "UPDATE: " << rc << endl;}
  }

  ~Worker() { free(blob_); }

  bool operator()() {
    int type = dis_(gen_);
    int rc;
    if (type == 0) {
      rc = step_single(select_stmt_);
      if(rc){cout << "step: " << rc << endl;}
    } else {
      rc = sqlite3_exec(db, "BEGIN EXCLUSIVE;", NULL,NULL,NULL);
      if(rc){cout << "Begin trans: " << rc << endl;}

      rc = sqlite3_bind_blob(update_stmt_,1,blob_, (int) size_, NULL);
      if(rc){cout << "bind blob: " << rc << endl;}
      rc = step_single(update_stmt_);
      if(rc){cout << "step: " << rc << endl;}

      rc = sqlite3_exec(db, "COMMIT;", NULL,NULL,NULL);
      if(rc){cout <<"load commit: " << stat << endl;}
    }

    return true;
  }

private:
  sqlite3* db;
  sqlite3_stmt *select_stmt_;
  sqlite3_stmt *update_stmt_;
  size_t size_;
  void *blob_;
  std::discrete_distribution<int> dis_;
  std::minstd_rand gen_;
};


int main(int argc, char **argv) {
  cxxopts::Options options =
      blob_options("blob_sqlite3", "Blob benchmark on SQLite3");
  cxxopts::OptionAdder adder = options.add_options("SQLite3");
  adder("path", "Path", cxxopts::value<std::string>()->default_value("/mnt/pmem0/scheinost/benchmark.db"));
  adder("pmem", "Pmem", cxxopts::value<std::string>()->default_value("PMem"));

  auto result = options.parse(argc, argv);

  if (result.count("help")) {
    std::cout << options.help();
    return 0;
  }

  auto size = result["size"].as<size_t>();
  auto mix = result["mix"].as<float>();
  string path = result["path"].as<std::string>();
  string pmem = result["pmem"].as<string>();
  int rc;
  if (result.count("load")) {
    sqlite3* db = open_db(path.c_str(),pmem);

    rc = sqlite3_exec(db,"DROP TABLE IF EXISTS t", NULL,NULL,NULL);
     if(rc){cout << "DROP: " << rc << endl;}
    rc = sqlite3_exec(db,"CREATE TABLE t (a BLOB)", NULL,NULL,NULL);
     if(rc){cout << "CREATE: " << rc << endl;}

    void *blob = malloc(size);
    rc = sqlite3_exec(db, "BEGIN EXCLUSIVE;", NULL,NULL,NULL);
    if(rc){cout << "Begin trans: " << rc << endl;}
    sqlite3_stmt *stmnt;
    rc = sqlite3_prepare_v2(db, "INSERT INTO t VALUES (?)",-1, &stmnt, NULL);
    if(rc){cout << "Insert blob: " << rc << endl;}
    rc = sqlite3_bind_blob(stmnt,1,blob, (int) size, NULL);
    if(rc){cout << "bind blob: " << rc << endl;}
    rc = step_single(stmnt);
    if(rc){cout << "step: " << rc << endl;}

    rc = sqlite3_exec(db, "COMMIT;", NULL,NULL,NULL);
    if(rc){cout <<"load commit: " << rc << endl;}

   int *frames_saved;
   rc = sqlite3_wal_checkpoint_v2(db, NULL, SQLITE_CHECKPOINT_FULL, NULL, frames_saved);
   if(rc){cout <<"WAL-Checkpoint: " << rc << endl;}

    close_db(db);
    free(blob);
  }

  if (result.count("run")) {
    sqlite3* db = open_db(path.c_str(),pmem);

    // rc = sqlite3_exec(db,"PRAGMA cache_size=-1000000", NULL,NULL,NULL);
    // if (rc != SQLITE_OK) {throw std::runtime_error(sqlite3_errmsg(db));}
    
    std::vector<Worker> workers;
    workers.emplace_back(db, size, mix);

    double throughput = dbbench::run(workers, result["warmup"].as<size_t>(),
                                     result["measure"].as<size_t>());

    ofstream result_file {"../../results/master_results.csv", std::ios::app};

    result_file <<"\"BLOB\",\"SQLite\",\"msc-large\",\""
                << pmem
                << "\",\"evaluation\",\""
                << size
                << "\",\""
                << throughput
                << "\",\"tps\",\""
                << mix
                << "\",\"1\",\"\""
                << endl;

     close_db(db);
  }

  return 0;
}
