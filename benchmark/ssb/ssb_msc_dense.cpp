#include "cxxopts.hpp"
#include "helpers.hpp"
#include "../readfile.hpp"
#include "../msc_dense_helper.hpp"


using namespace std;

void exec(sqlite3* db, const string &stmnt, const string &query){
  int rc = sqlite3_exec(db, stmnt.c_str(), NULL,NULL,NULL);
  if (rc != SQLITE_OK) {cout << "error querry: " << query << "\t" << rc << endl;}
}


int main(int argc, char **argv) {
  cxxopts::Options options = ssb_options("ssb_sqlite3", "SSB on SQLite3");

  cxxopts::ParseResult result = options.parse(argc, argv);

  std::string path = result["path"].as<std::string>();
  std::string pmem = result["pmem"].as<string>();
  auto sf = result["sf"].as<std::string>();
  std::string sync = result["sync"].as<string>();
  std::string cache_size = result["cache_size"].as<string>();


  if (result.count("help")) {
    std::cout << options.help();
    return 0;
  }

  sqlite3* db = open_db(path.c_str(), pmem, sync, cache_size);

  uint64_t mask = result["bloom_filter"].as<bool>() ? 0 : 0x00080000;
  int rc = sqlite3_test_control(SQLITE_TESTCTRL_OPTIMIZATIONS, db,mask);
  if (rc != SQLITE_OK) {cout << "test control not working " << rc << endl;}

  rc = sqlite3_exec(db,"ANALYZE", NULL,NULL,NULL);
  if (rc != SQLITE_OK) {cout << "ANALYZE: " << rc << endl;}
  rc = sqlite3_exec(db,"SELECT * FROM lineorder", NULL,NULL,NULL);
  if (rc != SQLITE_OK) {cout << "SELECT 1 " << rc << endl;}
  rc = sqlite3_exec(db,"SELECT * FROM part", NULL,NULL,NULL);
  if (rc != SQLITE_OK) {cout << "SELECT 2 " << rc << endl;}
  rc = sqlite3_exec(db,"SELECT * FROM supplier", NULL,NULL,NULL);
  if (rc != SQLITE_OK) {cout << "SELECT 3 " << rc << endl;}
  rc = sqlite3_exec(db,"SELECT * FROM customer", NULL,NULL,NULL);
  if (rc != SQLITE_OK) {cout << "SELECT 4 " << rc << endl;}
  rc = sqlite3_exec(db,"SELECT * FROM date", NULL,NULL,NULL);
  if (rc != SQLITE_OK) {cout << "SELECT 5 " << rc << endl;}

  std::ofstream result_file {"../../results/master_results.csv", std::ios::app};

  for (const std::string &query :
       {"q1.1", "q1.2", "q1.3", "q2.1", "q2.2", "q2.3", "q3.1", "q3.2", "q3.3",
        "q3.4", "q4.1", "q4.2", "q4.3"}) {
    const std::string sql = readfile("sql/" + query + ".sql");

    //cout << sql << endl;

    //std::cout << time([&] { conn.execute(sql).expect(SQLITE_OK); });
    result_file <<"\"SSB\",\"SQLite\",\"msc-dense\",\""
                << pmem
                << "\",\"evaluation\",\""
                << sf
                << "\",\""
                << time([&] { exec(db,sql,query); })
                << "\",\"s\",\""
                << query
                << "\",\"1\",\""
                << result["bloom_filter"].as<bool>()
                << "\""
                << std::endl;
    
  }

  close_db(db);
  return 0;
}
