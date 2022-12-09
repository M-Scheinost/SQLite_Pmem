#include "sqlite/sqlite3.h"
#include "vfs/pmem_vfs.h"
#include "stdio.h"
static int callback(void *NotUsed, int argc, char **argv, char **azColName){
  int i;
  for(i=0; i<argc; i++){
    printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
  }
  printf("\n");
  return 0;
}

int main (int argc, char** argv){

    sqlite3 *sqlite;
    sqlite3_vfs_register(sqlite3_pmem_vfs(), 1);

    char* err_msg = NULL;

    int status = sqlite3_open("test.db", &sqlite);
    /* Default cache size is a combined 4 MB */
    char* WAL_stmt = "PRAGMA journal_mode = WAL";
    status = sqlite3_exec(sqlite, WAL_stmt, NULL, NULL, &err_msg);
    

    char* create_stmt = "create table test (key integer, value integer, primary key (key));";
    status = sqlite3_exec(sqlite, create_stmt, callback, NULL, &err_msg);

    char* insert_stmt = "INSERT INTO test VALUES (10, 20);";
    status = sqlite3_exec(sqlite, insert_stmt, callback, NULL, &err_msg);

    //char* select_stmt = "select * from test;";
    //status = sqlite3_exec(sqlite, select_stmt, callback, NULL, &err_msg);

    status = sqlite3_close(sqlite);
}