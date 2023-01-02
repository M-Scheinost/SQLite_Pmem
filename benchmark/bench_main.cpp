#include "../sqlite/sqlite3.h"
#include "helper.hpp"
#include <iostream>
#include "dbbench/benchmarks/tatp.hpp"
#include "dbbench/runner.hpp"
#include "cxxopts.hpp"

#include "../vfs/pmem_vfs.h"

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

int step(sqlite3_stmt *stmt, size_t &count){
  while(sqlite3_step(stmt) == SQLITE_ROW){
    count++;
  }
  return sqlite3_reset(stmt);
}

int step(sqlite3_stmt *stmt){
  size_t count;
  return step(stmt, count);
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
              int rc = step(state_1);
              return true;
            },

            [&](const dbbench::tatp::GetNewDestination &p) {
              sqlite3_stmt *stmnt;
              sqlite3_prepare_v2(db, tatp_statement_sql[1], -1, &stmnt, NULL);
              sqlite3_bind_int64(stmnt, 1, (sqlite3_int64)p.s_id);
              sqlite3_bind_int(stmnt, 2, (int)p.sf_type);
              sqlite3_bind_int(stmnt, 3, (int)p.start_time);
              sqlite3_bind_int(stmnt, 4, (int)p.end_time);

              size_t count {0};
              int rc = step(stmnt, count);
              return count > 0;
            },

            [&](const dbbench::tatp::GetAccessData &p) {
              sqlite3_stmt *stmnt;
              sqlite3_prepare_v2(db, tatp_statement_sql[2], -1, &stmnt, NULL);
              sqlite3_bind_int64(stmnt, 1, (sqlite3_int64)p.s_id);
              sqlite3_bind_int(stmnt, 2, (int)p.ai_type);

              size_t count {0};
              int rc = step(stmnt, count);
              return count > 0;
            },

            [&](const dbbench::tatp::UpdateSubscriberData &p) {
              sqlite3_stmt *stmnt;
              sqlite3_prepare_v2(db, tatp_statement_sql[3], -1, &stmnt, NULL);
              sqlite3_bind_int(stmnt, 1, (int)p.bit_1);
              sqlite3_bind_int64(stmnt, 2, (sqlite3_int64)p.s_id);
              int rc = step(stmnt);

              sqlite3_prepare_v2(db, tatp_statement_sql[4], -1, &stmnt, NULL);
              sqlite3_bind_int(stmnt, 1, (int)p.data_a);
              sqlite3_bind_int64(stmnt, 2, (sqlite3_int64)p.s_id);
              sqlite3_bind_int(stmnt, 3, (int)p.sf_type);
              rc = step(stmnt);

              sqlite3_exec(db, "COMMIT;", NULL,NULL,NULL);
 
              return sqlite3_changes(db) > 0;
            },

            [&](const dbbench::tatp::UpdateLocation &p) {
              sqlite3_stmt *stmnt;
              sqlite3_prepare_v2(db, tatp_statement_sql[5], -1, &stmnt, NULL);
              sqlite3_bind_int64(stmnt, 1, (sqlite3_int64)p.vlr_location);
              sqlite3_bind_text(stmnt, 2, p.sub_nbr.c_str(), -1, SQLITE_TRANSIENT);
              int rc = step(stmnt);

              return true;
            },

            [&](const dbbench::tatp::InsertCallForwarding &p) {

              sqlite3_stmt *stmnt;
              sqlite3_prepare_v2(db, tatp_statement_sql[6], -1, &stmnt, NULL);
              sqlite3_bind_text(stmnt, 1, p.sub_nbr.c_str(), -1, SQLITE_TRANSIENT);
              int rc = sqlite3_step(stmnt);
              size_t s_id;
              if(rc == SQLITE_ROW){
                s_id = sqlite3_column_int64(stmnt,0);
                rc = step(stmnt);
              }

              sqlite3_prepare_v2(db, tatp_statement_sql[7], -1, &stmnt, NULL);
              sqlite3_bind_int64(stmnt, 1, (sqlite3_int64)s_id);
              rc = step(stmnt);

              sqlite3_prepare_v2(db, tatp_statement_sql[8], -1, &stmnt, NULL);
              sqlite3_bind_int64(stmnt, 1, (sqlite3_int64)s_id);
              sqlite3_bind_int(stmnt, 2, (int)p.sf_type);
              sqlite3_bind_int(stmnt, 3, (int)p.start_time);
              sqlite3_bind_int(stmnt, 4, (int)p.end_time);
              sqlite3_bind_text(stmnt, 5, p.numberx.c_str(), -1, SQLITE_TRANSIENT);
              
              rc = step(stmnt);
              
              bool success = false;
              if (rc == SQLITE_CONSTRAINT) {
                success =  false;
              }
              sqlite3_exec(db, "COMMIT;", NULL,NULL,NULL);
              return success;
            },

            [&](const dbbench::tatp::DeleteCallForwarding &p) {

              sqlite3_stmt *stmnt;
              sqlite3_prepare_v2(db, tatp_statement_sql[6], -1, &stmnt, NULL);
              sqlite3_bind_text(stmnt, 1, p.sub_nbr.c_str(), -1, SQLITE_TRANSIENT);
              int rc = sqlite3_step(stmnt);
              size_t s_id;
              if(rc == SQLITE_ROW){
                s_id = sqlite3_column_int64(stmnt,0);
                rc = step(stmnt);
              }

              sqlite3_prepare_v2(db, tatp_statement_sql[9], -1, &stmnt, NULL);
              
              sqlite3_bind_int64(stmnt, 1, (sqlite3_int64)s_id);
              sqlite3_bind_int(stmnt, 2, (int)p.sf_type);
              sqlite3_bind_int(stmnt, 3, (int)p.start_time);
              rc = step(stmnt);

              sqlite3_exec(db, "COMMIT;", NULL,NULL,NULL);
              return sqlite3_changes(db) > 0;
            },
        },
        procedure_generator_.next());
  }

private:
  sqlite3 *db;
  size_t db_size;
  dbbench::tatp::ProcedureGenerator procedure_generator_;
};


void load_db_1(sqlite3 *db, size_t db_size){

  dbbench::tatp::RecordGenerator record_generator(db_size);
  int i = 0;
  while(auto record = record_generator.next()){
    // if(i++ % 100000 == 0)
    //   cout << i << " " << flush;
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
  int stat = sqlite3_exec(db, "COMMIT;", NULL,NULL,NULL);
  if(stat){cout << stat << endl;}
}


void init(string path){
  sqlite3 *sqlite;

  char* err_msg = NULL;
  int status = sqlite3_open(path.c_str(), &sqlite);
  if(status){printf("Open:\t%i\t%s\n", status, err_msg);}

  /* activate WAL mode*/
  const char* WAL_stmt = "PRAGMA journal_mode = WAL;";
  status = sqlite3_exec(sqlite, sqlite_init, NULL, NULL, &err_msg);
  //if(status){printf("WAL:\t%i\t%s\n", status, err_msg);}

  sqlite3_exec(sqlite, "BEGIN TRANSACTION;", NULL,NULL,NULL);
  load_db_1(sqlite ,1);
  sqlite3_exec(sqlite, "END TRANSACTION;", NULL,NULL,NULL);

  // status = sqlite3_exec(sqlite, "select count(*) from subscriber;", callback, NULL, &err_msg);
  // if(status){printf("Select:\t%i\t%s\n", status, err_msg);}
  // status = sqlite3_exec(sqlite, "select count(*) from access_info;", callback, NULL, &err_msg);
  // if(status){printf("Select:\t%i\t%s\n", status, err_msg);}
  // status = sqlite3_exec(sqlite, "select count(*) from special_facility;", callback, NULL, &err_msg);
  // if(status){printf("Select:\t%i\t%s\n", status, err_msg);}
  // status = sqlite3_exec(sqlite, "select count(*) from call_forwarding;", callback, NULL, &err_msg);
  // if(status){printf("Select:\t%i\t%s\n", status, err_msg);}

  //###############################
  //  Test
  //###############################
  std::vector<Worker> workers;
  //workers.emplace_back(sqlite, db_size);
  size_t try_out {0};
  double throughput = dbbench::run(workers, try_out, try_out);
  cout << "Throughput: " << throughput << endl;


  status = sqlite3_close(sqlite);
  //if(status){printf("Close:\t%i\t%s\n", status, err_msg);}
}




sqlite3* open_db(const char* path, bool pmem){
  sqlite3 *db;
  char* err_msg = NULL;
  int status;
  int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE;
  if(pmem){
    sqlite3_vfs_register(sqlite3_pmem_vfs(), 0);
    status = sqlite3_open_v2(path, &db, flags, "PMem_VFS");
  }
  else{
    status = sqlite3_open_v2(path, &db, flags, "unix");
  }
  if(status){cout <<"Open:\t" << status << "\t" << err_msg << endl;}
  return db;
}

void close_db(sqlite3* db){
  char* err_msg = NULL;
  int status = sqlite3_close(db);
  if(status){cout <<"Close:\t" << status << "\t" << err_msg << endl;}
}


int main (int argc, char** argv){

  cxxopts::Options options = tatp_options("tatp_sqlite3", "TATP on SQLite3");

  cxxopts::OptionAdder adder = options.add_options("SQLite3");
  adder("journal_mode", "Journal mode", cxxopts::value<std::string>()->default_value("DELETE"));
  adder("cache_size", "Cache size", cxxopts::value<std::string>()->default_value("-1000000"));
  adder("path", "Path", cxxopts::value<std::string>()->default_value("tatp_bench.db"));
  adder("pmem", "Pmem", cxxopts::value<bool>()->default_value("false"));

  cxxopts::ParseResult result = options.parse(argc, argv);

  if (result.count("help")) {
    std::cout << options.help();
    return 0;
  }

  uint64_t n_subscriber_records = result["records"].as<uint64_t>();
  string journal_mode = result["journal_mode"].as<std::string>();
  string cache_size = result["cache_size"].as<std::string>();
  string path = result["path"].as<std::string>();
  bool pmem = result["pmem"].as<bool>();

  if (result.count("load")) {
    sqlite3 *db = open_db(path.c_str(), pmem);
    load_db_1(db, n_subscriber_records);
    close_db(db);
  }

  if (result.count("run")) {
    std::vector<Worker> workers;
    sqlite3 *db = open_db(path.c_str(), pmem);
    sqlite3_exec(db,"PRAGMA journal_mode=WAL", NULL,NULL,NULL);
    workers.emplace_back(db, n_subscriber_records);

    double throughput = dbbench::run(workers, result["warmup"].as<size_t>(),result["measure"].as<size_t>());
    std::cout << throughput << std::endl;
    close_db(db);
  }
  return 0;
}