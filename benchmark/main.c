#include "../sqlite/sqlite3.h"
#include "../vfs/pmem_vfs.h"
#include "stdio.h"



void init(){
  sqlite3 *sqlite;

  char* err_msg = NULL;
  int status = sqlite3_open("benchmark.db", &sqlite);
  if(status){printf("STATUS:\t%i\n", status);}

  /* activate WAL mode*/
  char* WAL_stmt = "PRAGMA journal_mode = WAL;";
  status = sqlite3_exec(sqlite, WAL_stmt, NULL, NULL, &err_msg);
  if(status){printf("WAL - STATUS:\t%i\n", status);}

  /* activate csv mode*/
  char* stmnt = "mode csv";
  status = sqlite3_exec(sqlite, stmnt, NULL, NULL, &err_msg);
  if(status){printf("CSV - STATUS:\t%i\terror: %s\n", status, err_msg);}

  /* create tables */
  stmnt = ".read ../sql/create_tables.sql;";
  status = sqlite3_exec(sqlite, stmnt, NULL, NULL, &err_msg);
  if(status){printf("Create table - STATUS:\t%i\n", status);}

  /* load data*/
  stmnt = ".read ../sql/load_data.sql;";
  status = sqlite3_exec(sqlite, stmnt, NULL, NULL, &err_msg);
  if(status){printf("Data load - STATUS:\t%i\n", status);}


  status = sqlite3_close(sqlite);
  if(status){printf("STATUS:\t%i\n", status);}
  
}


int main (int argc, char** argv){

  init();
}