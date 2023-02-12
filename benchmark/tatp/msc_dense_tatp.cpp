#include "helper.hpp"
#include <iostream>
#include "dbbench/benchmarks/tatp.hpp"
#include "dbbench/runner.hpp"
#include "cxxopts.hpp"
#include <fstream>
#include <chrono>
#include <filesystem>

#include "../msc_dense_helper.hpp"

using namespace std;
namespace fs = std::filesystem;
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

int step_single(sqlite3_stmt *stmt){
  size_t count = 0;
  return step(stmt, count);
}

class Worker {
public:
  Worker(sqlite3 *db_, size_t db_size) : db(db_), procedure_generator_(db_size) {

        int rc;
        vector<string> sql = tatp_transactions();
        for(int i = 0; i< 10; i++){
          sqlite3_stmt *stmt;
          rc = sqlite3_prepare_v2(db, sql[i].c_str(), -1, &stmt, NULL);
          if(rc){cout << "Prepare transaction_"<< i << "\t" << rc << endl;}
          stmts_.push_back(stmt);
        }
        for(int i = 0; i < 7000000; i++){
          next_value.push_back(procedure_generator_.next());
        }
        index = 0;
  }

  bool operator()() {
    return std::visit(
        overloaded{
            [&](const dbbench::tatp::GetSubscriberData &p) {
              int rc;
              sqlite3_stmt *stmnt = stmts_[0];
              rc = sqlite3_bind_int64(stmnt, 1, (sqlite3_int64)p.s_id);
              if(rc){cout << "Transition_1 bind "<< rc << endl;}
              rc = step_single(stmnt);
              if(rc){cout << "Transition_1 step "<< rc << endl;}
              return true;
            },

            [&](const dbbench::tatp::GetNewDestination &p) {
              int rc; 
              sqlite3_stmt *stmnt = stmts_[1];
              rc = sqlite3_bind_int64(stmnt, 1, (sqlite3_int64)p.s_id);
              if(rc){cout << "Transition_2 bind "<< rc << endl;}
              rc = sqlite3_bind_int(stmnt, 2, (int)p.sf_type);
              if(rc){cout << "Transition_2 bind "<< rc << endl;}
              rc = sqlite3_bind_int(stmnt, 3, (int)p.start_time);
              if(rc){cout << "Transition_2 bind "<< rc << endl;}
              rc = sqlite3_bind_int(stmnt, 4, (int)p.end_time);
              if(rc){cout << "Transition_2 bind "<< rc << endl;}

              size_t count {0};
              rc = step(stmnt, count);
              if(rc){cout << "Transition_1 step "<< rc << endl;}
              return count > 0;
            },

            [&](const dbbench::tatp::GetAccessData &p) {
              int rc;
              sqlite3_stmt *stmnt = stmts_[2];
              rc = sqlite3_bind_int64(stmnt, 1, (sqlite3_int64)p.s_id);
              if(rc){cout << "Transition_3 bind "<< rc << endl;}
              rc = sqlite3_bind_int(stmnt, 2, (int)p.ai_type);
              if(rc){cout << "Transition_3 bind "<< rc << endl;}

              size_t count {0};
              rc = step(stmnt, count);
              if(rc){cout << "Transition_3 bind "<< rc << endl;}
              return count > 0;
            },

            [&](const dbbench::tatp::UpdateSubscriberData &p) {
              int rc;
              rc = sqlite3_exec(db, "BEGIN DEFERRED;", NULL,NULL,NULL);
              if(rc){cout << "Transition_4 init "<< rc << endl;}

              sqlite3_stmt *stmnt = stmts_[3];
              rc = sqlite3_bind_int(stmnt, 1, (int)p.bit_1);
              if(rc){cout << "Transition_4 bind "<< rc << endl;}
              rc = sqlite3_bind_int64(stmnt, 2, (sqlite3_int64)p.s_id);
              if(rc){cout << "Transition_4 bind "<< rc << endl;}
              rc = step_single(stmnt);
              if(rc){cout << "Transition_4 step "<< rc << endl;}

              stmnt = stmts_[4];
              rc = sqlite3_bind_int(stmnt, 1, (int)p.data_a);
              if(rc){cout << "Transition_4 bind 2 "<< rc << endl;}
              rc = sqlite3_bind_int64(stmnt, 2, (sqlite3_int64)p.s_id);
              if(rc){cout << "Transition_4 bind 2 "<< rc << endl;}
              rc = sqlite3_bind_int(stmnt, 3, (int)p.sf_type);
              if(rc){cout << "Transition_4 bind 2 "<< rc << endl;}
              rc = step_single(stmnt);
              if(rc){cout << "Transition_4 step2 "<< rc << endl;}

              rc = sqlite3_exec(db, "COMMIT;", NULL,NULL,NULL);
              if(rc){cout << "Transition_4 commit "<< rc << endl;}
 
              return sqlite3_changes(db) > 0;
            },

            [&](const dbbench::tatp::UpdateLocation &p) {
              
              int rc;
              rc = sqlite3_exec(db, "BEGIN DEFERRED;", NULL,NULL,NULL);
              if(rc){cout << "Transition_6 init "<< rc << endl;}

              sqlite3_stmt *stmnt = stmts_[5];
              rc = sqlite3_bind_int64(stmnt, 1, (sqlite3_int64)p.vlr_location);
              if(rc){cout << "Transition_5 bind "<< rc << endl;}
              rc = sqlite3_bind_text(stmnt, 2, p.sub_nbr.c_str(), -1, SQLITE_TRANSIENT);
              if(rc){cout << "Transition_5 bind "<< rc << endl;}
              rc = step_single(stmnt);
              if(rc){cout << "Transition_5 step "<< rc << endl;}

              rc = sqlite3_exec(db, "COMMIT;", NULL,NULL,NULL);
              if(rc){cout << "Transition_6 commit "<< rc << endl;}

              return true;
            },

            [&](const dbbench::tatp::InsertCallForwarding &p) {
              int rc;
              rc = sqlite3_exec(db, "BEGIN DEFERRED;", NULL,NULL,NULL);
              if(rc){cout << "Transition_6 init "<< rc << endl;}

              sqlite3_stmt *stmnt = stmts_[6];;
              rc = sqlite3_bind_text(stmnt, 1, p.sub_nbr.c_str(), -1, SQLITE_TRANSIENT);
              if(rc){cout << "Transition_6 bind "<< rc << endl;}
              rc = sqlite3_step(stmnt);
              size_t s_id;
              if(rc == SQLITE_ROW){
                s_id = sqlite3_column_int64(stmnt,0);
                rc = step_single(stmnt);
              }
              else if(rc == SQLITE_DONE){
                return false;
              }
              if(rc){cout << "Transition_6 step "<< rc << endl;}

              stmnt = stmts_[7];
              rc = sqlite3_bind_int64(stmnt, 1, (sqlite3_int64)s_id);
              if(rc){cout << "Transition_6 bind2 "<< rc << endl;}
              rc = step_single(stmnt);
              if(rc){cout << "Transition_6 step 2 "<< rc << endl;}

              stmnt = stmts_[8];
              rc = sqlite3_bind_int64(stmnt, 1, (sqlite3_int64)s_id);
              if(rc){cout << "Transition_6 bind3 "<< rc << endl;}
              rc = sqlite3_bind_int(stmnt, 2, (int)p.sf_type);
              if(rc){cout << "Transition_6 bind3 "<< rc << endl;}
              rc = sqlite3_bind_int(stmnt, 3, (int)p.start_time);
              if(rc){cout << "Transition_6 bind3 "<< rc << endl;}
              rc = sqlite3_bind_int(stmnt, 4, (int)p.end_time);
              if(rc){cout << "Transition_6 bind3 "<< rc << endl;}
              rc = sqlite3_bind_text(stmnt, 5, p.numberx.c_str(), -1, SQLITE_TRANSIENT);
              if(rc){cout << "Transition_6 bind3 "<< rc << endl;}
              
              rc = step_single(stmnt);
              bool success = false;
              if (rc) {
                if(rc != SQLITE_CONSTRAINT){cout << "Transition_6 step3 "<< rc << endl;}
                success =  false;
              }
              rc = sqlite3_exec(db, "COMMIT;", NULL,NULL,NULL);
              if(rc){cout << "Transition_6 commit "<< rc << endl;}
              return success;
            },

            [&](const dbbench::tatp::DeleteCallForwarding &p) {
              int rc;
              rc = sqlite3_exec(db, "BEGIN DEFERRED;", NULL,NULL,NULL);
              if(rc){cout << "Transition_7 init "<< rc << endl;}

              sqlite3_stmt *stmnt = stmts_[6];
              rc = sqlite3_bind_text(stmnt, 1, p.sub_nbr.c_str(), -1, SQLITE_TRANSIENT);
              if(rc){cout << "Transition_7 bind "<< rc << endl;}
              rc = sqlite3_step(stmnt);
              size_t s_id;
              if(rc == SQLITE_ROW){
                s_id = sqlite3_column_int64(stmnt,0);
                rc = step_single(stmnt);
              }
              else if(rc == SQLITE_DONE){
                return false;
              }
              if(rc){cout << "Transition_7 step "<< rc << endl;}

              stmnt = stmts_[9];
              rc = sqlite3_bind_int64(stmnt, 1, (sqlite3_int64)s_id);
              if(rc){cout << "Transition_7 bind2 "<< rc << endl;}
              rc = sqlite3_bind_int(stmnt, 2, (int)p.sf_type);
              if(rc){cout << "Transition_7 bind2 "<< rc << endl;}
              rc = sqlite3_bind_int(stmnt, 3, (int)p.start_time);
              if(rc){cout << "Transition_7 bind2 "<< rc << endl;}
              rc = step_single(stmnt);
              if(rc){cout << "Transition_7 step2 "<< rc << endl;}

              rc = sqlite3_exec(db, "COMMIT;", NULL,NULL,NULL);
              if(rc){cout << "Transition_7 commit "<< rc << endl;}
              return sqlite3_changes(db) > 0;
            },
        },
        next_value[index++]);
        //index++;
        //cout << index << endl;
        //procedure_generator_.next());
  }

private:
  sqlite3 *db;
  size_t db_size;
  std::vector<sqlite3_stmt*> stmts_;
  dbbench::tatp::ProcedureGenerator procedure_generator_;
  vector<dbbench::tatp::Procedure> next_value;
  size_t index;
};

void init_load(sqlite3 *db){
  int rc;

  rc = sqlite3_exec(db, sqlite_init, NULL,NULL,NULL);
  if(rc){cout << "Create tables: " << rc << endl;}
}

void load_db(sqlite3 *db, size_t db_size){

  int rc;

  sqlite3_stmt *subscriber;
  sqlite3_stmt *access_info;
  sqlite3_stmt *special_facility;
  sqlite3_stmt *call_forwarding;

  rc = sqlite3_prepare_v2(db, prep_sub.c_str(), prep_sub.size(), &subscriber, NULL);
  if(rc){cout <<"SR_prepare:\t" << rc << endl;}
  rc = sqlite3_prepare_v2(db, prep_ai.c_str(), prep_ai.size(), &access_info, NULL);
  if(rc){cout <<"AIR_prepare:\t" << rc << endl;}
  rc = sqlite3_prepare_v2(db, prep_sf.c_str(), prep_sf.size(), &special_facility, NULL);
  if(rc){cout <<"SF_prepare:\t" << rc << endl;}
  rc = sqlite3_prepare_v2(db, prep_cf.c_str(), prep_cf.size(), &call_forwarding, NULL);
  if(rc){cout <<"CFR_prepare:\t" << rc << endl;}

  dbbench::tatp::RecordGenerator record_generator(db_size);
  
  sqlite3_exec(db, "BEGIN EXCLUSIVE;", NULL,NULL,NULL);
  while(auto record = record_generator.next()){
    std::visit(
        overloaded{
            [&](const dbbench::tatp::SubscriberRecord &r) {
              int rc = sqlite3_bind_int64(subscriber, 1, (sqlite3_int64)r.s_id);
              if(rc){cout <<"SR_bind:\t" << rc << endl;}
              rc = sqlite3_bind_text(subscriber, 2, r.sub_nbr.c_str(), -1, SQLITE_TRANSIENT);
              if(rc){cout <<"SR_bind:\t" << rc << endl;}
              for (int i = 0; i < 10; ++i) {
                rc = sqlite3_bind_int(subscriber, i+3, (int)r.bit[i]);
                if(rc){cout <<"SR_bind:\t" << rc << endl;}
              }
              for (int i = 0; i < 10; ++i) {
                rc = sqlite3_bind_int(subscriber, i+13, (int)r.hex[i]);
                if(rc){cout <<"SR_bind:\t" << rc << endl;}
              }
              for (int i = 0; i < 10; ++i) {
                rc = sqlite3_bind_int(subscriber, i+23, (int)r.byte2[i]);
                if(rc){cout <<"SR_bind:\t" << rc << endl;}
              }
              rc = sqlite3_bind_int64(subscriber, 33, (sqlite3_int64)r.msc_location);
              if(rc){cout <<"SR_bind:\t" << rc << endl;}
              rc = sqlite3_bind_int64(subscriber, 34, (sqlite3_int64)r.vlr_location);
              if(rc){cout <<"SR_bind:\t" << rc << endl;}

              rc = step_single(subscriber);
              if(rc){cout <<"SR_step:\t" << rc << endl;}
            },

            [&](const dbbench::tatp::AccessInfoRecord &r) {

              int rc = sqlite3_bind_int64(access_info, 1, (sqlite3_int64)r.s_id);
              if(rc){cout <<"AIR_bind:\t" << rc << endl;}
              rc = sqlite3_bind_int(access_info, 2, (int)r.ai_type);
              if(rc){cout <<"AIR_bind:\t" << rc << endl;}
              rc = sqlite3_bind_int(access_info, 3, (int)r.data1);
              if(rc){cout <<"AIR_bind:\t" << rc << endl;}
              rc = sqlite3_bind_int(access_info, 4, (int)r.data2);
              if(rc){cout <<"AIR_bind:\t" << rc << endl;}
              rc = sqlite3_bind_text(access_info, 5, r.data3.c_str(), -1, SQLITE_TRANSIENT);
              if(rc){cout <<"AIR_bind:\t" << rc << endl;}
              rc = sqlite3_bind_text(access_info, 6, r.data4.c_str(), -1, SQLITE_TRANSIENT);
              if(rc){cout <<"AIR_bind:\t" << rc << endl;}
      
              rc = step_single(access_info);
              if(rc){cout <<"AIR_step:\t" << rc << endl;}
            },

            [&](const dbbench::tatp::SpecialFacilityRecord &r) {

              int rc = sqlite3_bind_int64(special_facility, 1, (sqlite3_int64)r.s_id);
              if(rc){cout <<"SF_bind:\t" << rc << endl;}
              rc = sqlite3_bind_int(special_facility, 2, (int)r.sf_type);
              if(rc){cout << "SF_bind:\t" << rc << endl;}
              rc = sqlite3_bind_int(special_facility, 3, (int)r.is_active);
              if(rc){cout << "SF_bind:\t" << rc << endl;}
              rc = sqlite3_bind_int(special_facility, 4, (int)r.error_cntrl);
              if(rc){cout << "SF_bind:\t" << rc << endl;}
              rc = sqlite3_bind_int(special_facility, 5, (int)r.data_a);
              if(rc){cout << "SF_bind:\t" << rc << endl;}
              rc = sqlite3_bind_text(special_facility, 6, r.data_b.c_str(), -1, SQLITE_TRANSIENT);
              if(rc){cout << "SF_bind:\t" << rc << endl;}

              rc = step_single(special_facility);
              if(rc){cout << "SF_step:\t" << rc << endl;}
            },

            [&](const dbbench::tatp::CallForwardingRecord &r) {

              int rc = sqlite3_bind_int64(call_forwarding, 1, (sqlite3_int64)r.s_id);
              if(rc){cout <<"CFR_bind:\t" << rc << endl;}
              rc = sqlite3_bind_int(call_forwarding, 2, (int)r.sf_type);
              if(rc){cout <<"CFR_bind:\t" << rc << endl;}
              rc = sqlite3_bind_int(call_forwarding, 3, (int)r.start_time);
              if(rc){cout <<"CFR_bind:\t" << rc << endl;}
              rc = sqlite3_bind_int(call_forwarding, 4, (int)r.end_time);
              if(rc){cout <<"CFR_bind:\t" << rc << endl;}
              rc = sqlite3_bind_text(call_forwarding, 5, r.numberx.c_str(), -1, SQLITE_TRANSIENT);
              if(rc){cout <<"CFR_bind:\t" << rc << endl;}

              rc = step_single(call_forwarding);
              if(rc){cout <<"CFR_step:\t" << rc << endl;}
            },
        },
        *record);
  }

  rc = sqlite3_exec(db, "COMMIT;", NULL,NULL,NULL);
  if(rc){cout <<"load commit: " << stat << endl;}
  
  
  rc = sqlite3_finalize(call_forwarding);
  if(rc){cout <<"CFR_step:\t" << rc << endl;}
  rc = sqlite3_finalize(special_facility);
  if(rc){cout <<"SF_step:\t" << rc << endl;}
  rc = sqlite3_finalize(access_info);
  if(rc){cout <<"ACR_step:\t" << rc << endl;}
  rc = sqlite3_finalize(subscriber);
  if(rc){cout <<"SR_step:\t" << rc << endl;}
}


int main (int argc, char** argv){

  cxxopts::Options options = tatp_options("tatp_sqlite3", "TATP on SQLite3");

  cxxopts::ParseResult result = options.parse(argc, argv);

  if (result.count("help")) {
    std::cout << options.help();
    return 0;
  }

  uint64_t n_subscriber_records = result["records"].as<uint64_t>();
  string journal_mode = result["journal_mode"].as<std::string>();
  string cache_size = result["cache_size"].as<std::string>();
  string path = result["path"].as<std::string>();
  string pmem = result["pmem"].as<string>();
  string sync = result["sync"].as<string>();

  if (result.count("load")) {
    sqlite3 *db = open_db(path.c_str(), pmem);
    auto start = chrono::steady_clock::now();
    init_load(db);
    load_db(db, n_subscriber_records);

    auto end = chrono::steady_clock::now();
    auto time = chrono::duration_cast<chrono::milliseconds>(end - start).count();
    close_db(db);
    
    ofstream result_file {"../../results/master_results.csv", ios::app};
    result_file <<"\"TATP\",\"SQLite\",\"msc-dense\",\""
            << pmem
            << "\",\"loading\",\""
            << n_subscriber_records
            << "\",\""
            << time
            << "\",\"ms\",\"\",\"1\",\"\""
            << endl;
  }

  if (result.count("run")) {
    int rc;
    std::vector<Worker> workers;
    sqlite3 *db = open_db(path.c_str(), pmem, sync, cache_size);

    workers.emplace_back(db, n_subscriber_records);

    double throughput = dbbench::run(workers, result["warmup"].as<size_t>(),result["measure"].as<size_t>());
    close_db(db);

    ofstream result_file {"../../results/master_results.csv", ios::app};

    result_file <<"\"TATP\",\"SQLite\",\"msc-dense\",\""
                << pmem
                << "\",\"evaluation\",\""
                << n_subscriber_records
                << "\",\""
                << throughput
                << "\",\"tps\",\"\",\"1\",\"\""
                << endl;
  }
  return 0;
}