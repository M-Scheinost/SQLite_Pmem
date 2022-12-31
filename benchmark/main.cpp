#include "../sqlite/sqlite3.h"
#include "helper.h"
#include "stdio.h"
#include "dbbench/benchmarks/tatp.hpp"
#include "dbbench/runner.hpp"

static int callback(void *NotUsed, int argc, char **argv, char **azColName){
  int i;
  for(i=0; i<argc; i++){
    printf("%s | ", argv[i] ? argv[i] : "NULL");
  }
  printf("\n");
  return 0;
}

void init(){
  sqlite3 *sqlite;

  char* err_msg = NULL;
  int status = sqlite3_open("../release/benchmark.db", &sqlite);
  if(status){printf("STATUS:\t%i\n", status);}

  /* activate WAL mode*/
  const char* WAL_stmt = "PRAGMA journal_mode = WAL;";
  status = sqlite3_exec(sqlite, sqlite_init, NULL, NULL, &err_msg);
  if(status){printf("Init status:\t%i\n", status);}

  status = sqlite3_close(sqlite);
  if(status){printf("STATUS:\t%i\n", status);}
}


int main (int argc, char** argv){
  printf("init sqlite\n");
  init();
  printf("init done\n");
}