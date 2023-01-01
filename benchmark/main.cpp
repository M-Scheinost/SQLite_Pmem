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
  for(i=0; i<argc-1; i++){
    cout << argv[i] << " | ";
  }
  cout << argv[argc-1] << endl;
  return 0;
}

class Worker {
public:
  Worker(sqlite3 *db, size_t db_size)
      : db(db), procedure_generator_(db_size) {
  }

  bool operator()() {
    return std::visit(
        overloaded{
            [&](const dbbench::tatp::GetSubscriberData &p) {
              sqlite3_stmt *state_1;
              sqlite3_prepare_v2(db, tatp_statement_sql[0], -1, &state_1, NULL);
              sqlite3_bind_int64(state_1, 1, (sqlite3_int64)p.s_id);
              sqlite3_step(state_1);
              sqlite3_finalize(state_1);
              return true;
            },

            [&](const dbbench::tatp::GetNewDestination &p) {
              sqlite3_stmt *stmnt;
              sqlite3_prepare_v2(db, tatp_statement_sql[1], -1, &stmnt, NULL);
              sqlite3_bind_int64(stmnt, 1, (sqlite3_int64)p.s_id);
              sqlite3_bind_int(stmnt, 2, (int)p.sf_type);
              sqlite3_bind_int(stmnt, 3, (int)p.start_time);
              sqlite3_bind_int(stmnt, 4, (int)p.end_time);
              int res = sqlite3_step(stmnt);
              if(res == SQLITE_ROW){
                size_t count = sqlite3_column_int(stmnt,0);
                res = sqlite3_step(stmnt);
                if(res == SQLITE_DONE)
                  sqlite3_finalize(stmnt);
                  return count > 0;
                }
                return false;
            },

            [&](const dbbench::tatp::GetAccessData &p) {
              sqlite3_stmt *stmnt;
              sqlite3_prepare_v2(db, tatp_statement_sql[2], -1, &stmnt, NULL);
              sqlite3_bind_int64(stmnt, 1, (sqlite3_int64)p.s_id);
              sqlite3_bind_int(stmnt, 2, (int)p.ai_type);
              int res = sqlite3_step(stmnt);
              if(res == SQLITE_ROW){
                size_t count = sqlite3_column_int(stmnt,0);
                res = sqlite3_step(stmnt);
                if(res == SQLITE_DONE)
                  sqlite3_finalize(stmnt);
                  return count > 0;
                }
              return false;
            },

            [&](const dbbench::tatp::UpdateSubscriberData &p) {
              sqlite3_stmt *stmnt;
              sqlite3_prepare_v2(db, tatp_statement_sql[3], -1, &stmnt, NULL);
              sqlite3_bind_int(stmnt, 1, (int)p.bit_1);
              sqlite3_bind_int64(stmnt, 2, (sqlite3_int64)p.s_id);
              int res2 = sqlite3_step(stmnt);
              sqlite3_finalize(stmnt);

              sqlite3_prepare_v2(db, tatp_statement_sql[4], -1, &stmnt, NULL);
              sqlite3_bind_int(stmnt, 1, (int)p.data_a);
              sqlite3_bind_int64(stmnt, 2, (sqlite3_int64)p.s_id);
              sqlite3_bind_int(stmnt, 3, (int)p.sf_type);
              int res1 = sqlite3_step(stmnt);
              sqlite3_finalize(stmnt);
 
              return res1 == SQLITE_DONE && res2 == SQLITE_DONE;
            },

            [&](const dbbench::tatp::UpdateLocation &p) {
              sqlite3_stmt *stmnt;
              sqlite3_prepare_v2(db, tatp_statement_sql[5], -1, &stmnt, NULL);
              sqlite3_bind_int64(stmnt, 1, (sqlite3_int64)p.vlr_location);
              sqlite3_bind_text(stmnt, 2, p.sub_nbr.c_str(), -1, SQLITE_TRANSIENT);
              int res2 = sqlite3_step(stmnt);
              sqlite3_finalize(stmnt);
              return true;
            },

            [&](const dbbench::tatp::InsertCallForwarding &p) {

              sqlite3_stmt *stmnt;
              sqlite3_prepare_v2(db, tatp_statement_sql[6], -1, &stmnt, NULL);
              sqlite3_bind_text(stmnt, 1, p.sub_nbr.c_str(), -1, SQLITE_TRANSIENT);
              int res = sqlite3_step(stmnt);
              size_t s_id;
              if(res == SQLITE_ROW){
                s_id = sqlite3_column_int64(stmnt,0);
                res = sqlite3_step(stmnt);
                if(res == SQLITE_DONE)
                  sqlite3_finalize(stmnt);
                }

              sqlite3_prepare_v2(db, tatp_statement_sql[7], -1, &stmnt, NULL);
              sqlite3_bind_int64(stmnt, 1, (sqlite3_int64)s_id);
              res = sqlite3_step(stmnt);
              if(res == SQLITE_ROW){
                size_t count = sqlite3_column_int64(stmnt,0);
                res = sqlite3_step(stmnt);
                if(res == SQLITE_DONE)
                  sqlite3_finalize(stmnt);
                }

              sqlite3_prepare_v2(db, tatp_statement_sql[8], -1, &stmnt, NULL);
              sqlite3_bind_int64(stmnt, 1, (sqlite3_int64)s_id);
              sqlite3_bind_int(stmnt, 2, (int)p.sf_type);
              sqlite3_bind_int(stmnt, 3, (int)p.start_time);
              sqlite3_bind_int(stmnt, 4, (int)p.end_time);
              sqlite3_bind_text(stmnt, 5, p.numberx.c_str(), -1, SQLITE_TRANSIENT);
              int rc = sqlite3_step(stmnt);
              if(res == SQLITE_ROW){
                size_t count = sqlite3_column_int64(stmnt,0);
                res = sqlite3_step(stmnt);
                if(res == SQLITE_DONE)
                  sqlite3_finalize(stmnt);
                }

              if (rc == SQLITE_CONSTRAINT) {
                return false;
              }
              return true;
            },

            [&](const dbbench::tatp::DeleteCallForwarding &p) {
              sqlite3_stmt *stmnt;
              sqlite3_prepare_v2(db, tatp_statement_sql[6], -1, &stmnt, NULL);
              sqlite3_bind_text(stmnt, 1, p.sub_nbr.c_str(), -1, SQLITE_TRANSIENT);
              int res = sqlite3_step(stmnt);
              size_t s_id;
              if(res == SQLITE_ROW){
                s_id = sqlite3_column_int64(stmnt,0);
                res = sqlite3_step(stmnt);
                if(res == SQLITE_DONE)
                  sqlite3_finalize(stmnt);
                }

              sqlite3_prepare_v2(db, tatp_statement_sql[9], -1, &stmnt, NULL);
              
              sqlite3_bind_int64(stmnt, 1, (sqlite3_int64)s_id);
              sqlite3_bind_int(stmnt, 2, (int)p.sf_type);
              sqlite3_bind_int(stmnt, 3, (int)p.start_time);
              int res1 = sqlite3_step(stmnt);
              sqlite3_finalize(stmnt);
 
              return res1 == SQLITE_DONE;
            },
        },
        procedure_generator_.next());
  }

private:
  sqlite3 *db;
  size_t db_size;
  dbbench::tatp::ProcedureGenerator procedure_generator_;
};

void load_db(sqlite3 *db, size_t db_size){
  dbbench::tatp::RecordGenerator record_generator(SIZE_FACTOR_SMALL);
  int i = 0;
  while(auto record = record_generator.next()){
    if(i % 10000 == 0)
      cout << i++ << " " << flush;
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

void load_db_1(sqlite3 *db, size_t db_size){

  
  sqlite3_stmt *call_forwarding;
  sqlite3_prepare_v2(db, "INSERT INTO call_forwarding VALUES (?,?,?,?,?)", -1, &call_forwarding, NULL);

  dbbench::tatp::RecordGenerator record_generator(db_size);
  int i = 0;
  while(auto record = record_generator.next()){
    if(i++ % 100000 == 0)
      cout << i << " " << flush;
    std::visit(
        overloaded{
            [&](const dbbench::tatp::SubscriberRecord &r) {
              sqlite3_stmt *subscriber;
              sqlite3_prepare_v2(db, "INSERT INTO subscriber VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", -1, &subscriber, NULL);
              sqlite3_bind_int64(subscriber, 1, (sqlite3_int64)r.s_id);
              sqlite3_bind_text(subscriber, 2, r.sub_nbr.c_str(), -1, SQLITE_TRANSIENT);
              for (int i = 0; i < 10; ++i) {
                sqlite3_bind_int(subscriber, i+3, (int)r.bit[i]);
              }
              for (int i = 0; i < 10; ++i) {
                sqlite3_bind_int(subscriber, i+13, (int)r.hex[i]);
              }
              for (int i = 0; i < 10; ++i) {
                sqlite3_bind_int(subscriber, i+23, (int)r.byte2[i]);
              }
              sqlite3_bind_int64(subscriber, 33, (sqlite3_int64)r.msc_location);
              sqlite3_bind_int64(subscriber, 34, (sqlite3_int64)r.vlr_location);
              sqlite3_step(subscriber);
              sqlite3_finalize(subscriber);
            },

            [&](const dbbench::tatp::AccessInfoRecord &r) {
              sqlite3_stmt *access_info;
              sqlite3_prepare_v2(db, "INSERT INTO access_info VALUES (?,?,?,?,?,?)", -1, &access_info, NULL);

              sqlite3_bind_int64(access_info, 1, (sqlite3_int64)r.s_id);
              sqlite3_bind_int(access_info, 2, (int)r.ai_type);
              sqlite3_bind_int(access_info, 3, (int)r.data1);
              sqlite3_bind_int(access_info, 4, (int)r.data2);
              sqlite3_bind_text(access_info, 5, r.data3.c_str(), -1, SQLITE_TRANSIENT);
              sqlite3_bind_text(access_info, 6, r.data4.c_str(), -1, SQLITE_TRANSIENT);
      
              sqlite3_step(access_info);
              sqlite3_finalize(access_info);
            },

            [&](const dbbench::tatp::SpecialFacilityRecord &r) {
              sqlite3_stmt *special_facility;
              sqlite3_prepare_v2(db, "INSERT INTO special_facility VALUES (?,?,?,?,?,?)", -1, &special_facility, NULL);

              sqlite3_bind_int64(special_facility, 1, (sqlite3_int64)r.s_id);
              sqlite3_bind_int(special_facility, 2, (int)r.sf_type);
              sqlite3_bind_int(special_facility, 3, (int)r.is_active);
              sqlite3_bind_int(special_facility, 4, (int)r.error_cntrl);
              sqlite3_bind_int(special_facility, 5, (int)r.data_a);
              sqlite3_bind_text(special_facility, 6, r.data_b.c_str(), -1, SQLITE_TRANSIENT);

              sqlite3_step(special_facility);
              sqlite3_finalize(special_facility);
            },

            [&](const dbbench::tatp::CallForwardingRecord &r) {
              sqlite3_stmt *call_forwarding;
              sqlite3_prepare_v2(db, "INSERT INTO call_forwarding VALUES (?,?,?,?,?)", -1, &call_forwarding, NULL);

              sqlite3_bind_int64(call_forwarding, 1, (sqlite3_int64)r.s_id);
              sqlite3_bind_int(call_forwarding, 2, (int)r.sf_type);
              sqlite3_bind_int(call_forwarding, 3, (int)r.start_time);
              sqlite3_bind_int(call_forwarding, 4, (int)r.end_time);
              sqlite3_bind_text(call_forwarding, 5, r.numberx.c_str(), -1, SQLITE_TRANSIENT);

              sqlite3_step(call_forwarding);
              sqlite3_finalize(call_forwarding);
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
  if(status){printf("Open:\t%i\t%s\n", status, err_msg);}

  /* activate WAL mode*/
  const char* WAL_stmt = "PRAGMA journal_mode = WAL;";
  status = sqlite3_exec(sqlite, sqlite_init, NULL, NULL, &err_msg);
  if(status){printf("WAL:\t%i\t%s\n", status, err_msg);}

  sqlite3_exec(sqlite, "BEGIN TRANSACTION;", NULL,NULL,NULL);
  load_db_1(sqlite ,db_size);
  sqlite3_exec(sqlite, "END TRANSACTION;", NULL,NULL,NULL);

  status = sqlite3_exec(sqlite, "select count(*) from subscriber;", callback, NULL, &err_msg);
  if(status){printf("Select:\t%i\t%s\n", status, err_msg);}
  status = sqlite3_exec(sqlite, "select count(*) from access_info;", callback, NULL, &err_msg);
  if(status){printf("Select:\t%i\t%s\n", status, err_msg);}
  status = sqlite3_exec(sqlite, "select count(*) from special_facility;", callback, NULL, &err_msg);
  if(status){printf("Select:\t%i\t%s\n", status, err_msg);}
  status = sqlite3_exec(sqlite, "select count(*) from call_forwarding;", callback, NULL, &err_msg);
  if(status){printf("Select:\t%i\t%s\n", status, err_msg);}

  //###############################
  //  Test
  //###############################
  std::vector<Worker> workers;
  workers.emplace_back(sqlite, db_size);
  size_t try_out {0};
  double throughput = dbbench::run(workers, try_out, try_out);
  cout << "Throughput: " << throughput << endl;


  status = sqlite3_close(sqlite);
  if(status){printf("Close:\t%i\t%s\n", status, err_msg);}
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