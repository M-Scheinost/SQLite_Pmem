#include "sqlite/sqlite3.h"
#include "vfs/pmem_vfs.h"
#include "vfs/test_demovfs.h"
#include "stdio.h"
static int callback(void *NotUsed, int argc, char **argv, char **azColName){
  int i;
  for(i=0; i<argc; i++){
    printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
  }
  printf("\n");
  return 0;
}

void test_normal(){
  sqlite3 *sqlite;

  char* err_msg = NULL;
  int status = sqlite3_open("sqlite.db", &sqlite);
  printf("STATUS:\t%i\n", status);
  /* Default cache size is a combined 4 MB */
  char* WAL_stmt = "PRAGMA journal_mode = WAL";
  status = sqlite3_exec(sqlite, WAL_stmt, NULL, NULL, &err_msg);
  printf("STATUS:\t%i\n", status);

  char* create_stmt = "create table test (key integer, value integer);";
  status = sqlite3_exec(sqlite, create_stmt, callback, NULL, &err_msg);
  printf("STATUS:\t%i\n", status);

  char* insert_stmt = "INSERT INTO test VALUES (10, 22330);";
  status = sqlite3_exec(sqlite, insert_stmt, callback, NULL, &err_msg);
  printf("STATUS:\t%i\n", status);

  //char* select_stmt = "select * from test;";
  //status = sqlite3_exec(sqlite, select_stmt, callback, NULL, &err_msg);

  status = sqlite3_close(sqlite);
  printf("STATUS:\t%i\nNormal ende\n", status);
}

void test_pmem(){
  sqlite3 *sqlite;
    sqlite3_vfs_register(sqlite3_demovfs(), 1);

    char* err_msg = NULL;

    int status = sqlite3_open("pmem.db", &sqlite);
    printf("STATUS:\t%i\n", status);
    /* Default cache size is a combined 4 MB */
    char* WAL_stmt = "PRAGMA journal_mode = WAL";
    status = sqlite3_exec(sqlite, WAL_stmt, NULL, NULL, &err_msg);
    printf("STATUS:\t%i\n", status);

    char* create_stmt = "create table test (key integer, value integer);";
    status = sqlite3_exec(sqlite, create_stmt, callback, NULL, &err_msg);
    printf("STATUS:\t%i\n", status);

    char* insert_stmt = "INSERT INTO test VALUES (10, 22330);";
    status = sqlite3_exec(sqlite, insert_stmt, callback, NULL, &err_msg);
    printf("STATUS:\t%i\n", status);

    //char* select_stmt = "select * from test;";
    //status = sqlite3_exec(sqlite, select_stmt, callback, NULL, &err_msg);

    status = sqlite3_close(sqlite);
    printf("STATUS:\t%i\n", status);
}

int main (int argc, char** argv){

    test_normal();
    test_pmem();
}