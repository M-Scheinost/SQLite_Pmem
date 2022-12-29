#include "../sqlite/sqlite3.h"
#include "../vfs/pmem_vfs.h"
#include "stdio.h"

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
  char* WAL_stmt = "PRAGMA journal_mode = WAL;";
  status = sqlite3_exec(sqlite, WAL_stmt, NULL, NULL, &err_msg);
  if(status){printf("WAL - STATUS:\t%i\n", status);}

  /* load data*/
  char* stmnt = "select * from Address;";
  status = sqlite3_exec(sqlite, stmnt, callback, NULL, &err_msg);
  if(status){printf("Select - STATUS:\t%i\n", status);}

  status = sqlite3_close(sqlite);
  if(status){printf("STATUS:\t%i\n", status);}
}


int main (int argc, char** argv){

  init();
}