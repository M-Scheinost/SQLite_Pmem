#include "../sqlite/sqlite3.h"
#include "helper.h"
#include <iostream>
#include "dbbench/benchmarks/tatp.hpp"
#include "dbbench/runner.hpp"

using namespace std;
template <class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template <class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

static int callback(void *NotUsed, int argc, char **argv, char **azColName){
  int i;
  for(i=0; i<argc; i++){
    printf("%s | ", argv[i] ? argv[i] : "NULL");
  }
  printf("\n");
  return 0;
}

void load_db(sqlite3 *db, size_t db_size){
  dbbench::tatp::RecordGenerator record_generator(SIZE_FACTOR_SMALL);
  int i = 0;
  while(auto record = record_generator.next()){
    cout << i++ << flush;
    std::visit(
        overloaded{
            [&](const dbbench::tatp::SubscriberRecord &r) {
              std::string insert_stmnt = "INSERT INTO subscriber VALUES (";
              insert_stmnt += to_string(r.s_id) + ",";
              insert_stmnt += r.sub_nbr + ",";

              for (int i = 0; i < 10; ++i) {
                insert_stmnt += std::to_string(r.bit[i]) + ",";
              }
              for (int i = 0; i < 10; ++i) {
                insert_stmnt += std::to_string(r.hex[i]) + ",";
              }
              for (int i = 0; i < 10; ++i) {
                insert_stmnt += std::to_string(r.byte2[i]) + ",";
              }
              insert_stmnt += std::to_string(r.msc_location) + ",";
              insert_stmnt += std::to_string(r.vlr_location) + ");";
              char* err_msg = NULL;
              cout << insert_stmnt << endl;
              int status = sqlite3_exec(db, sqlite_init, NULL, NULL, &err_msg);
              if(status){printf("load failed:\t%i\t%s\n", status, err_msg);}
            },

            [&](const dbbench::tatp::AccessInfoRecord &r) {
              std::string insert_stmnt = "INSERT INTO access_info VALUES (";
              insert_stmnt += to_string(r.s_id) + ",";
              insert_stmnt += to_string(r.ai_type) + ",";
              insert_stmnt += to_string(r.data1) + ",";
              insert_stmnt += to_string(r.data2) + ",";
              insert_stmnt += r.data3 + ",";
              insert_stmnt += r.data4 + ");";
              char* err_msg = NULL;
              cout << insert_stmnt << endl;
              int status = sqlite3_exec(db, sqlite_init, NULL, NULL, &err_msg);
              if(status){printf("load failed:\t%i\t%s\n", status, err_msg);}
            },

            [&](const dbbench::tatp::SpecialFacilityRecord &r) {
              std::string insert_stmnt = "INSERT INTO special_facility VALUES (";
              insert_stmnt += to_string(r.s_id) + ",";
              insert_stmnt += to_string(r.sf_type) + ",";
              insert_stmnt += to_string(r.is_active) + ",";
              insert_stmnt += to_string(r.error_cntrl) + ",";
              insert_stmnt += to_string(r.data_a) + ",";
              insert_stmnt += r.data_b + ");";
              char* err_msg = NULL;
              cout << insert_stmnt << endl;
              int status = sqlite3_exec(db, sqlite_init, NULL, NULL, &err_msg);
              if(status){printf("load failed:\t%i\t%s\n", status, err_msg);}
            },

            [&](const dbbench::tatp::CallForwardingRecord &r) {
              std::string insert_stmnt = "INSERT INTO call_forwarding VALUES (";
              insert_stmnt += to_string(r.s_id) + ",";
              insert_stmnt += to_string(r.sf_type) + ",";
              insert_stmnt += to_string(r.start_time) + ",";
              insert_stmnt += to_string(r.end_time) + ",";
              insert_stmnt += r.numberx + ");";
              cout << insert_stmnt << endl;
              char* err_msg = NULL;
              int status = sqlite3_exec(db, sqlite_init, NULL, NULL, &err_msg);
              if(status){printf("load failed:\t%i\t%s\n", status, err_msg);}
            },
        },
        *record);
  }
  std::cout << std::endl;
}

void init(size_t db_size){
  sqlite3 *sqlite;

  char* err_msg = NULL;
  int status = sqlite3_open("../release/benchmark.db", &sqlite);
  if(status){printf("STATUS:\t%i\n", status);}

  /* activate WAL mode*/
  const char* WAL_stmt = "PRAGMA journal_mode = WAL;";
  status = sqlite3_exec(sqlite, sqlite_init, NULL, NULL, &err_msg);
  if(status){printf("Init status:\t%i\n", status);}

  load_db(sqlite ,db_size);

  const char* stmt = "select * from subscriber;";
  status = sqlite3_exec(sqlite, stmt, callback, NULL, &err_msg);
  if(status){printf("Init status:\t%i\n", status);}

  status = sqlite3_close(sqlite);
  if(status){printf("STATUS:\t%i\n", status);}
}


int main (int argc, char** argv){

  if(argc != 2){
    cout << "not enough arguments\n";
    exit(1);
  }
  if(argv[1] == "1")
    init(SIZE_FACTOR_SMALL);
  else if(argv[1] == "10")
    init(SIZE_FACTOR_MEDIUM);
  else
    init(SIZE_FACTOR_LARGE);
  
}